#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /tests/test_persistence/test_database.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday March 2nd 2023 05:57:22 am                                                 #
# Modified   : Thursday March 2nd 2023 12:41:20 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
import os
import inspect
from datetime import datetime
import pytest
import logging
import shutil

from atelier.persistence.odb import ObjectDB
from atelier.persistence.exceptions import (
    ObjectExistsError,
    ObjectNotFoundError,
    ObjectDatabaseConnectionError,
)

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"

DB_FILEPATH = "tests/testdata/test_workspace/test.db"


@pytest.mark.odb
class TestDatabase:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        shutil.rmtree(os.path.dirname(DB_FILEPATH), ignore_errors=True)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\nCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_connect_context(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        db = ObjectDB(name="test_db", filepath=DB_FILEPATH)
        assert db.is_connected is False
        db.connect()
        assert db.is_connected is True
        db.close()
        assert db.is_connected is False
        with db as odb:
            assert odb.is_connected is True
        assert odb.is_connected is False
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_insert_exists(self, dataframe, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        key = inspect.stack()[0][3] + "_1"
        odb = ObjectDB(name="test_db", filepath=DB_FILEPATH)
        with odb as db:
            db.insert(key=key, value=dataframe)
            assert db.exists(key=key)
            assert db.is_connected is True
        assert odb.is_connected is False

        # Exceptions
        odb.connect()
        with pytest.raises(ObjectExistsError):
            odb.insert(key=key, value=dataframe)
        odb.close()

        with pytest.raises(ObjectDatabaseConnectionError):
            odb.insert(key=key, value=dataframe)

        self.test_setup(caplog)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_select(self, dataframe, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        key = inspect.stack()[0][3] + "_1"
        odb = ObjectDB(name="test_db", filepath=DB_FILEPATH)

        with pytest.raises(ObjectDatabaseConnectionError):
            odb.select(key=key)

        with odb as db:
            db.insert(key=key, value=dataframe)
            df = db.select(key=key)
            assert dataframe.equals(df)
        assert odb.is_connected is False

        odb.connect()
        with pytest.raises(ObjectNotFoundError):
            df = odb.select(key="test_df_xes")
        odb.close()

        with pytest.raises(ObjectDatabaseConnectionError):
            odb.select(key=key)

        self.test_setup(caplog)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_selectall(self, dataframe, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        database = ObjectDB(name="test_db", filepath=DB_FILEPATH)

        with pytest.raises(ObjectDatabaseConnectionError):
            database.selectall()

        with database as db:
            for i in range(1, 6):
                key = inspect.stack()[0][3] + "_" + str(i)
                db.insert(key=key, value=dataframe)

        # Autoconnect True  autoclose is True
        db = ObjectDB(name="test_db", filepath=DB_FILEPATH)
        db.connect()
        result = db.selectall()
        db.close()
        assert len(result) == 5
        assert isinstance(result, dict)
        assert db.is_connected is False

        key = inspect.stack()[0][3] + "_1"
        with pytest.raises(ObjectDatabaseConnectionError):
            db.selectall()

        self.test_setup(caplog)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_update(self, dataframe, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        key = inspect.stack()[0][3] + "_1"
        d = {"some": "dictionary", "for": "update"}
        database = ObjectDB(name="test_db", filepath=DB_FILEPATH)

        with pytest.raises(ObjectDatabaseConnectionError):
            database.update(key="doesn't matter", value=d)

        with database as db:
            db.insert(key=key, value=dataframe)
            db.update(key=key, value=d)
            d2 = db.select(key=key)
            assert d == d2

            with pytest.raises(ObjectNotFoundError):
                with database as db:
                    db.update(key="xsdsdls", value=d)
        self.test_setup(caplog)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_delete(self, dataframe, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        key = inspect.stack()[0][3] + "_1"
        database = ObjectDB(name="test_db", filepath=DB_FILEPATH)

        with pytest.raises(ObjectDatabaseConnectionError):
            database.delete(key="doesn't matter")

        with database as db:
            db.insert(key=key, value=dataframe)
            db.delete(key=key)
            assert db.exists(key=key) is False

            with pytest.raises(ObjectNotFoundError):
                with database as db:
                    db.delete(key="xsdsdls")

            with pytest.raises(ObjectDatabaseConnectionError):
                db.delete(key=key)

        self.test_setup(caplog)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_clear(self, dataframe, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        database = ObjectDB(name="test_db", filepath=DB_FILEPATH)

        with pytest.raises(ObjectDatabaseConnectionError):
            database.clear()

        with database as db:
            for i in range(1, 6):
                key = inspect.stack()[0][3] + "_" + str(i)
                db.insert(key=key, value=dataframe)
            db.clear()
            result = db.selectall()
            assert len(result) == 0

        with pytest.raises(ObjectDatabaseConnectionError):
            database.clear()

        self.test_setup(caplog)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_properties(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        database = ObjectDB(name="test_db", filepath=DB_FILEPATH)
        assert database.name == "test_db"
        assert database.filepath == DB_FILEPATH

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_exists(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        database = ObjectDB(name="test_db", filepath=DB_FILEPATH)

        with pytest.raises(ObjectDatabaseConnectionError):
            database.exists(key="doesn't matter")

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
        logger.info(single_line)
