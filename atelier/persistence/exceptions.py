#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /atelier/workspace/exceptions.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday March 2nd 2023 05:50:42 am                                                 #
# Modified   : Thursday March 2nd 2023 07:32:05 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
class RecsysException(Exception):  # pragma: no cover
    """Base class for all project exceptions."""


class ObjectNotFoundError(RecsysException):  # pragma: no cover
    def __init__(self, msg) -> None:
        super().__init__(msg)


class ObjectExistsError(RecsysException):  # pragma: no cover
    def __init__(self, msg) -> None:
        super().__init__(msg)


class ObjectDBEmpty(RecsysException):  # pragma: no cover
    def __init__(self, msg) -> None:
        super().__init__(msg)


class ObjectDatabaseConnectionError(RecsysException):  # pragma: no cover
    def __init__(self, msg) -> None:
        super().__init__(msg)
