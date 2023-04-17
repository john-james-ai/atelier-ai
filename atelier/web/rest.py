#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.19                                                                              #
# Python     : 3.10.10                                                                             #
# Filename   : /atelier/web/rest.py                                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday April 17th 2023 03:00:02 am                                                  #
# Modified   : Monday April 17th 2023 05:23:24 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Rest API Module"""
import functools
import time
import math

from atelier.log.log import get_default_logger


# ------------------------------------------------------------------------------------------------ #
def retry(tries, delay=3, backoff=2):
    """Retries a function or method using exponential backoff.

    delay sets the initial delay in seconds, and backoff sets the factor by which
    the delay should lengthen after each failure. backoff must be greater than 1,
    or else it isn't really a backoff. tries must be at least 0, and delay
    greater than 0."""
    if backoff <= 1:
        raise ValueError("backoff must be greater than 1")
    tries = math.floor(tries)
    if tries < 0:
        raise ValueError("tries must be 0 or greater")
    if delay <= 0:
        raise ValueError("delay must be greater than 0")

    def retry_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            module = func.__module__
            qualname = func.__qualname__
            logger = get_default_logger(module, qualname)
            mtries, mdelay = tries, delay  # make mutable
            while mtries > 0:
                try:
                    response = func(*args, **kwargs)  # attempt
                except Exception as e:
                    mtries -= 1  # consume an attempt
                    mdelay *= backoff  # make future wait longer
                    time.sleep(mdelay)  # wait...then retry
                    msg = f"Encountered a {type[e]} exception. Retrying."
                    logger.info(msg)
                else:
                    if response.status_code == 200:
                        return response
            msg = "Ran out of retries. Aborting the request."
            logger.info(msg)
            return response  # Ran out of tries :-(

        return wrapper  # true decorator -> decorated function

    return retry_decorator  # @retry(arg[, ...]) -> true decorator
