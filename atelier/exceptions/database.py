#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.19                                                                              #
# Python     : 3.10.10                                                                             #
# Filename   : /atelier/exceptions/database.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday April 17th 2023 02:27:40 am                                                  #
# Modified   : Monday April 17th 2023 02:46:05 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Database Exception Handling Module"""
import functools
import logging
from typing import Union

from sqlalchemy.exc import SQLAlchemyError

from atelier.log.log import DefaultLogger, get_default_logger


# ------------------------------------------------------------------------------------------------ #
class ObjectNotFound(Exception):
    """Object not found in database or persistence mechanism exception

    Args:
        id (int): Object id. Optional.
        name (str): Object name. Optional.
    """

    def __init__(self, id: int = None, name: str = None) -> None:
        id = "" if id is None else f" id = {id}"
        name = "" if name is None else f" name = {name}"
        message = f"Object {id} {name} not found."
        super().__init__(message)


# ------------------------------------------------------------------------------------------------ #


# ------------------------------------------------------------------------------------------------ #
def database(_func=None, *, default_logger: Union[DefaultLogger, logging.Logger] = None):
    def decorator_log(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            module = func.__module__
            qualname = func.__qualname__
            logger = get_default_logger(module, qualname)
            try:
                if default_logger is None:
                    first_args = next(iter(args), None)  # capture first arg to check for `self`
                    logger_params = [  # does kwargs have any logger
                        x
                        for x in kwargs.values()
                        if isinstance(x, logging.Logger) or isinstance(x, DefaultLogger)
                    ] + [  # # does args have any logger
                        x
                        for x in args
                        if isinstance(x, logging.Logger) or isinstance(x, DefaultLogger)
                    ]
                    if hasattr(first_args, "__dict__"):  # is first argument `self`
                        logger_params = logger_params + [
                            x
                            for x in first_args.__dict__.values()  # does class (dict) members have any logger
                            if isinstance(x, logging.Logger) or isinstance(x, DefaultLogger)
                        ]
                    h_logger = next(
                        iter(logger_params), DefaultLogger(module=module, qualname=qualname)
                    )  # get the next/first/default logger
                else:
                    h_logger = default_logger  # logger is passed explicitly to the decorator

                if isinstance(h_logger, DefaultLogger):
                    logger = h_logger.get_logger(qualname)
                else:
                    logger = h_logger

                args_repr = [repr(a) for a in args]
                kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
                signature = ", ".join(args_repr + kwargs_repr)
                logger.debug(f"function {func.__qualname__} called with args {signature}")
            except Exception:
                pass

            try:
                result = func(*args, **kwargs)
                return result
            except SQLAlchemyError as e:
                logger.exception(
                    f"Exception {type[e]} was raised in {func.__qualname__}. Exception: {str(e)}"
                )
                raise e

        return wrapper

    if _func is None:
        return decorator_log
    else:
        return decorator_log(_func)
