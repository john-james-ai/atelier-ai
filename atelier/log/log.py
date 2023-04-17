#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.19                                                                              #
# Python     : 3.10.4                                                                              #
# Filename   : /atelier/logging/log.py                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday April 17th 2023 12:02:41 am                                                  #
# Modified   : Monday April 17th 2023 02:33:39 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Logging Decorator Module: Provides basic and standardized logging facility. """
import functools
import logging
from typing import Union

from dependency_injector import containers, providers


# ------------------------------------------------------------------------------------------------ #
class LoggingContainer(containers.DeclarativeContainer):
    """Logging and Cross-Cutting Concerns as"""

    config = providers.Configuration(yaml_files=["atelier/logging/config.yml"])

    logging = providers.Resource(
        logging.config.dictConfig,
        config=config.logging,
    )


# ------------------------------------------------------------------------------------------------ #
class DefaultLogger:
    def __init__(self, module: str, qualname: str):
        self._module = module
        self._qualname = qualname

    def get_logger(self) -> logging:
        return logging.getLogger(f"{self._module}.{self._qualname}")


# ------------------------------------------------------------------------------------------------ #
def get_default_logger(module: str, qualname: str):
    return DefaultLogger(module=module, qualname=qualname).get_logger()


# ------------------------------------------------------------------------------------------------ #
def log(_func=None, *, default_logger: Union[DefaultLogger, logging.Logger] = None):
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
            except Exception as e:
                logger.exception(f"Exception raised in {func.__qualname__}. exception: {str(e)}")
                raise e

        return wrapper

    if _func is None:
        return decorator_log
    else:
        return decorator_log(_func)
