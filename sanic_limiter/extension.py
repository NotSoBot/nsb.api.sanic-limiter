"""
the sanic extension
"""

import inspect
import logging
import sys

from sanic import Sanic, Request
from typing import Any, Awaitable, Callable, Optional, Type, Union, cast

import six

from limits import RateLimitItem, WindowStats
from limits.aio.storage import Storage as StorageAIO
from limits.aio.strategies import STRATEGIES as STRATEGIES_AIO
from limits.errors import ConfigurationError
from limits.storage import Storage, storage_from_string
from limits.strategies import STRATEGIES
from limits.util import parse_many
from sanic.blueprints import Blueprint

from .errors import RateLimitExceeded
from .util import execute_callback, execute_callback_with_request, get_remote_address


class ConfigVariables:
    ENABLED = "RATELIMIT_ENABLED"
    STORAGE_URL = "RATELIMIT_STORAGE_URL"
    STORAGE_OPTIONS = "RATELIMIT_STORAGE_OPTIONS"
    STRATEGY = "RATELIMIT_STRATEGY"
    GLOBAL_LIMITS = "RATELIMIT_GLOBAL"
    SWALLOW_ERRORS = "RATELIMIT_SWALLOW_ERRORS"


class ContextVariables:
    RATELIMITS_HIT = "RATELIMITS_HIT"


executable_key_function = Union[
    Callable[[], Union[Awaitable[Union[str, None]], Union[str, None]]],
    Callable[[Any], Union[Awaitable[Union[str, None]], Union[str, None]]],
]

executable_error_message = Union[
    Callable[[], Union[Awaitable[Union[str, None]], Union[str, None]]],
    Callable[[Any], Union[Awaitable[Union[str, None]], Union[str, None]]],
]

executable_exempt_when_function = Union[
    Callable[[], Union[Awaitable[bool], bool]],
    Callable[[Any], Union[Awaitable[bool], bool]],
]

executable_request_filter_function = Union[
    Callable[[], Union[Awaitable[bool], bool]],
    Callable[[Any], Union[Awaitable[bool], bool]],
]

class ExtLimit(object):
    """
    simple wrapper to encapsulate limits and their context
    """

    def __init__(
        self,
        item: Union[RateLimitItem, Callable[[], str]],
        key_func: executable_key_function,
        scope: Optional[str] = None,
        per_method: bool = False,
        methods: Optional[list[str]] = None,
        error_message: Optional[Union[str, executable_error_message]] = None,
        exempt_when: Optional[executable_exempt_when_function] = None,
    ):
        self.limit: Union[RateLimitItem, None] = None
        self._dynamic_limit: Union[Callable[[], str], None] = None
        self._scope = scope

        if callable(item):
            self._dynamic_limit = item
        else:
            self.limit = item

        self.key_func = key_func
        self.per_method = per_method
        self.methods = [x.lower() for x in methods] if methods else methods
        self.error_message = error_message
        self.exempt_when = exempt_when

    @property
    def scope(self):
        return self._scope or ''


class Limiter(object):
    """
    :param app: :class:`sanic.Sanic` instance to initialize the extension with.
    :param list global_limits: a variable list of strings denoting global
     limits to apply to all routes. :ref:`ratelimit-string` for  more details.
    :param function key_func: a callable that returns the key to rate limit by.
    :param str strategy: the strategy to use. refer to :ref:`ratelimit-strategy`
    :param str storage_uri: the storage location. refer to :ref:`ratelimit-conf`
    :param dict storage_options: kwargs to pass to the storage implementation upon instantiation.
    :param bool swallow_errors: whether to swallow errors when hitting a rate limit.
     An exception will still be logged. default ``False``
    """

    def __init__(
        self,
        app: Optional[Sanic] = None,
        key_func: Optional[executable_key_function] = None,
        global_limits: list[str] = [],
        strategy: Optional[str] = None,
        storage_uri: Optional[str] = None,
        storage_options: dict = {},
        swallow_errors: bool = False,
        strategy_async: Optional[bool] = None,
    ):
        self.logger = logging.getLogger('sanic-limiter')

        self.enabled = True
        self._global_limits: list[ExtLimit] = []
        self._exempt_routes: set[str] = set()
        self._request_filters: list[executable_request_filter_function]= []
        self._strategy = strategy
        self._strategy_should_be_async = strategy_async
        self._storage_uri = storage_uri
        self._storage_options = storage_options
        self._swallow_errors = swallow_errors

        self._key_func = key_func or get_remote_address
        for string in global_limits:
            limits = [ExtLimit(x, self._key_func) for x in parse_many(string)]
            self._global_limits.extend(limits)

        self._dynamic_route_limits: dict[str, list[ExtLimit]] = {}
        self._blueprint_dynamic_limits: dict[str, list[ExtLimit]] = {}
        self._blueprint_limits: dict[str, list[ExtLimit]] = {}
        self._route_limits: dict[str, list[ExtLimit]] = {}

        self._storage: Union[Storage, StorageAIO, None] = None
        self._limiter = None
        self._storage_dead = False

        class BlackHoleHandler(logging.StreamHandler):
            def emit(*_):
                return

        self.logger.addHandler(BlackHoleHandler())
        if app:
            self.init_app(app)

    def init_app(self, app: Sanic):
        """
        :param app: :class:`sanic.Sanic` instance to rate limit.
        """
        self.app = app
        self.enabled = app.config.setdefault(ConfigVariables.ENABLED, True)
        self._swallow_errors = app.config.setdefault(ConfigVariables.SWALLOW_ERRORS, self._swallow_errors)
        self._storage_options.update(app.config.get(ConfigVariables.STORAGE_OPTIONS, {}))

        storage_string = self._storage_uri or app.config.setdefault(ConfigVariables.STORAGE_URL, 'memory://')
        if self._strategy_should_be_async is None:
            self._strategy_should_be_async = storage_string.startswith('async+')

        if self._strategy_should_be_async:
            if not storage_string.startswith('async+'):
                storage_string = f'async+{storage_string}'

        self._storage = storage_from_string(storage_string, **self._storage_options)

        strategy = self._strategy or app.config.setdefault(ConfigVariables.STRATEGY, 'fixed-window')
        strategies = cast(dict, STRATEGIES_AIO if self._strategy_should_be_async else STRATEGIES)
        if strategy not in strategies:
            raise ConfigurationError(f'Invalid rate limiting strategy {strategy}')

        self._limiter = strategies[strategy](self._storage)

        config_limits = app.config.get(ConfigVariables.GLOBAL_LIMITS, None)
        if not self._global_limits and config_limits:
            limits = [ExtLimit(x, self._key_func) for x in parse_many(config_limits)]
            self._global_limits.extend(limits)

        app.request_middleware.append(self.__check_request_limit)

    @property
    def limiter(self):
        return self._limiter

    @property
    def limiter_is_async(self):
        return self._strategy_should_be_async

    async def __check_request_limit(self, request: Request) -> None:
        ratelimits_hit: list[tuple[str, str, RateLimitItem, WindowStats]] = []
        request.ctx.RATELIMITS_HIT = ratelimits_hit

        endpoint = (request.route and request.route.path) or request.path or ''
        view_handler = request.app.router.get(request.path, request.method, request.host)
        if view_handler is None:
            return

        view_func = view_handler[1]
        view_bpname = view_func.__dict__.get('__blueprintname__', None)
        if hasattr(view_func, 'func'):
            view_func = view_func.func

        name = ''
        if cast(Any, view_func):
            name = f'{view_func.__module__}.{view_func.__name__}'

        if not endpoint or not self.enabled or name in self._exempt_routes:
            return

        filters = [await execute_callback_with_request(fn, request) for fn in self._request_filters]
        if any(filters):
            return

        limits = self._route_limits.get(name, [])
        dynamic_limits = []
        if name in self._dynamic_route_limits:
            for limit in self._dynamic_route_limits[name]:
                if not limit._dynamic_limit:
                    continue

                limit_value = limit._dynamic_limit()
                try:
                    for x in parse_many(limit_value):
                        dynamic_limit = ExtLimit(
                            x,
                            limit.key_func,
                            scope=limit.scope,
                            per_method=limit.per_method,
                            methods=limit.methods,
                            error_message=limit.error_message,
                            exempt_when=limit.exempt_when,
                        )
                        dynamic_limits.append(dynamic_limit)
                except ValueError as error:
                    self.logger.error(f'failed to load ratelimit for view function {name} {error}')

        if view_bpname:
            if view_bpname in self._blueprint_dynamic_limits and not dynamic_limits:
                for limit in self._blueprint_dynamic_limits[view_bpname]:
                    if not limit._dynamic_limit:
                        continue
                    
                    limit_value = limit._dynamic_limit()
                    try:
                        for x in parse_many(limit_value):
                            dynamic_limit = ExtLimit(
                                x,
                                limit.key_func,
                                scope=limit.scope,
                                per_method=limit.per_method,
                                methods=limit.methods,
                                error_message=limit.error_message,
                                exempt_when=limit.exempt_when,
                            )
                            dynamic_limits.append(dynamic_limit)

                    except ValueError as error:
                        self.logger.error(f'failed to load ratelimit for view blueprint {view_bpname} {error}')

            if view_bpname in self._blueprint_limits and not limits:
                limits.extend(self._blueprint_limits[view_bpname])

        failed_limits: list[tuple[str, str, ExtLimit]] = []
        try:
            for limit in (limits + dynamic_limits or self._global_limits):
                scope = limit.scope or endpoint
                if callable(limit.exempt_when) and await execute_callback_with_request(limit.exempt_when, request):
                    return

                if limit.methods is not None and request.method.lower() not in limit.methods:
                    return

                if limit.per_method:
                    scope += ":%s" % request.method

                key: Union[str, None] = None
                if limit.key_func:
                    key = await execute_callback_with_request(limit.key_func, request)

                if key is None:
                    # Ignore empty result of the key function.
                    continue

                if limit.limit:
                    succeeded = await execute_callback(self.limiter.hit, limit.limit, key, scope)

                    stats = await execute_callback(self.limiter.get_window_stats, limit.limit, key, scope)
                    ratelimits_hit.append((scope, key, limit.limit, stats))
                    if not succeeded:
                        self.logger.warning(f'ratelimit {limit.limit} ({key}) exceeded at endpoint: {scope}')
                        failed_limits.append((scope, key, limit))

            if failed_limits:
                description = 'Ratelimit Exceeded'
                if len(failed_limits) == 1:
                    scope, key, item = failed_limits[0]
                    if item.error_message:
                        description = ''
                        if callable(item.error_message):
                            description = await execute_callback_with_request(item.error_message, request)
                        else:
                            description = item.error_message
                    elif item.limit:
                        description = six.text_type(item.limit)
                else:
                    description = 'Multiple Ratelimits Exceeded'
                raise RateLimitExceeded(
                    description,
                    failed_limits=[(scope, key, cast(RateLimitItem, item.limit)) for scope, key, item in failed_limits],
                )
        except Exception as error:
            if isinstance(error, RateLimitExceeded):
                six.reraise(*sys.exc_info())

            if self._swallow_errors:
                self.logger.exception('Failed to ratelimit. Swallowing error')
            else:
                six.reraise(*sys.exc_info())

    def __limit_decorator(
        self,
        limit_value: str,
        key_func=None,
        shared: bool = False,
        scope: Optional[str] = None,
        per_method: bool = False,
        methods: Optional[list[str]] = None,
        error_message: Optional[str] = None,
        exempt_when: Optional[Callable] = None,
    ):
        _scope = scope if shared else None

        def _inner(obj: Any):
            func = key_func or self._key_func
            is_bp = True if isinstance(obj, Blueprint) else False

            name = ''
            if is_bp:
                name = obj.name
            else:
                name = f'{obj.__module__}.{obj.__name__}'

            dynamic_limit: Optional[ExtLimit] = None
            static_limits: list[ExtLimit] = []
 
            if callable(limit_value):
                dynamic_limit = ExtLimit(
                    limit_value,
                    func,
                    scope=_scope,
                    per_method=per_method,
                    methods=methods,
                    error_message=error_message,
                    exempt_when=exempt_when,
                )
            else:
                try:
                    for limit in parse_many(limit_value):
                        static_limit = ExtLimit(
                            limit,
                            func,
                            scope=_scope,
                            per_method=per_method,
                            methods=methods,
                            error_message=error_message,
                            exempt_when=exempt_when,
                        )
                        static_limits.append(static_limit)
                except ValueError as error:
                    self.logger.error(f'failed to configure view function {name} ({error})')

            if is_bp:
                if dynamic_limit:
                    limits = self._blueprint_dynamic_limits.setdefault(name, [])
                    limits.append(dynamic_limit)
                else:
                    limits = self._blueprint_limits.setdefault(name, [])
                    limits.extend(static_limits)
            else:
                if dynamic_limit:
                    limits = self._dynamic_route_limits.setdefault(name, [])
                    limits.append(dynamic_limit)
                else:
                    limits = self._route_limits.setdefault(name, [])
                    limits.extend(static_limits)
                return obj

        return _inner

    def limit(
        self,
        limit_value: str,
        key_func=None,
        per_method=False,
        methods=None,
        error_message=None,
        exempt_when=None,
    ):
        """
        decorator to be used for rate limiting individual routes.

        :param limit_value: rate limit string or a callable that returns a string.
         :ref:`ratelimit-string` for more details.
        :param function key_func: function/lambda to extract the unique identifier for
         the rate limit. defaults to remote address of the request.
        :param bool per_method: whether the limit is sub categorized into the http
         method of the request.
        :param list methods: if specified, only the methods in this list will be rate
         limited (default: None).
        :param error_message: string (or callable that returns one) to override the
         error message used in the response.
        :return:
        """
        return self.__limit_decorator(limit_value, key_func, per_method=per_method,
                                      methods=methods, error_message=error_message,
                                      exempt_when=exempt_when)

    def shared_limit(self, limit_value, scope, key_func=None,
                     error_message=None, exempt_when=None):
        """
        decorator to be applied to multiple routes sharing the same rate limit.

        :param limit_value: rate limit string or a callable that returns a string.
         :ref:`ratelimit-string` for more details.
        :param scope: a string or callable that returns a string
         for defining the rate limiting scope.
        :param function key_func: function/lambda to extract the unique identifier for
         the rate limit. defaults to remote address of the request.
        :param error_message: string (or callable that returns one) to override the
         error message used in the response.
        """
        return self.__limit_decorator(
            limit_value, key_func, True, scope, error_message=error_message,
            exempt_when=exempt_when
        )

    def exempt(self, obj):
        """
        decorator to mark a view as exempt from global rate limits.
        """
        name = "{}.{}".format(obj.__module__, obj.__name__)

        self._exempt_routes.add(name)
        return obj

    def request_filter(self, fn: Callable):
        """
        decorator to mark a function as a filter to be executed
        to check if the request is exempt from rate limiting.
        """
        self._request_filters.append(fn)
        return fn

    def reset(self):
        """
        resets the storage if it supports being reset
        """
        try:
            self._storage.reset()
            self.logger.info("Storage has been reset and all limits cleared")
        except NotImplementedError:
            self.logger.warning("This storage type does not support being reset")
