"""

"""

import inspect

from typing import Callback

from sanic import Request



def get_remote_address(request: Request) -> str:
    """
    :param: request: request object of sanic
    :return: the ip address of given request (or 127.0.0.1 if none found)
    """
    # Check if request object has remote_addr attribute set
    # Seems to break on sanic 19.6.3
    if hasattr(request, 'remote_addr'):
        return request.remote_addr
    else:
        return request.ip


async def execute_callback_with_request(callback: Callable, request: Request) -> Any:
    if not callable(callback):
        raise ValueError('callback is not callable')

    args = []
    func_parameters = inspect.signature(callback).parameters.values()
    if func_parameters and func_parameters[0].default is inspect.Parameter.empty:
        args.append(request)

    return await execute_callback(callback, *args)


async def execute_callback(callback: Callable, *args) -> Any:
    if not callable(callback):
        raise ValueError('callback is not callable')
    
    result = callback(*args)
    if inspect.isawaitable(result):
        result = await result
    return result
