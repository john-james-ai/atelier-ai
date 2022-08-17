#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.1                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /test_io.py                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday August 15th 2022 06:11:16 pm                                                 #
# Modified   : Wednesday August 17th 2022 12:09:41 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
import pytest
import logging
import pandas as pd
import shutil

# Enter imports for modules and classes being tested here
from atelier.data.io import IOFactory, CsvIO, YamlIO, PickleIO, ParquetIO

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.io
class TestCSVIO:
    def test_setup(self, test_data_folder):
        output_filepath = os.path.join(test_data_folder, "test_io")
        shutil.rmtree(output_filepath, ignore_errors=True)

    def test_io(self, caplog, dataframe, test_data_folder):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fileformat = "csv"
        output_filepath = os.path.join(test_data_folder, "test_io", "test_csv", "test.csv")

        io = IOFactory.io(fileformat=fileformat)
        assert isinstance(io, CsvIO)

        io.write(data=dataframe, filepath=output_filepath)
        df = io.read(filepath=output_filepath)
        assert isinstance(df, pd.DataFrame)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_csv_params(self, caplog, test_data_folder):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fileformat = "csv"
        output_filepath = os.path.join(test_data_folder, "test_io", "test_csv", "test.csv")

        sep = ","
        encoding_errors = "ignore"
        header = 0
        usecols = ["discourse_id", "discourse_type"]
        nrows = 100
        thousands = ","

        io = IOFactory.io(fileformat=fileformat)
        df = io.read(
            filepath=output_filepath,
            sep=sep,
            encoding_errors=encoding_errors,
            header=header,
            usecols=usecols,
            nrows=nrows,
            thousands=thousands,
        )
        assert isinstance(df, pd.DataFrame)
        print(df.head)
        assert df.shape == (100, 2)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


@pytest.mark.io
class TestYamlIO:
    def test_yml_io(self, caplog, dictionary, test_data_folder):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fileformat = "yml"
        output_filepath = os.path.join(test_data_folder, "test_io", "test_yml", "test.yml")
        io = IOFactory.io(fileformat=fileformat)
        assert isinstance(io, YamlIO)

        io.write(data=dictionary, filepath=output_filepath)
        assert os.path.exists(output_filepath)

        yml = io.read(filepath=output_filepath)
        assert isinstance(yml, dict)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


@pytest.mark.io
class TestPickleIO:
    def test_pickle_io(self, caplog, dictionary, test_data_folder):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fileformat = "pickle"
        output_filepath = os.path.join(test_data_folder, "test_io", "test_pickle", "test.pickle")
        io = IOFactory.io(fileformat=fileformat)
        assert isinstance(io, PickleIO)

        io.write(data=dictionary, filepath=output_filepath)
        assert os.path.exists(output_filepath)

        yml = io.read(filepath=output_filepath)
        assert isinstance(yml, dict)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


@pytest.mark.io
class TestParquetIO:
    def test_parquet(self, caplog, dataframe, test_data_folder):

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fileformat = "parquet"
        output_filepath = os.path.join(test_data_folder, "test_io", "test_parquet", "test.parquet")
        io = IOFactory.io(fileformat=fileformat)
        assert isinstance(io, ParquetIO)

        io.write(data=dataframe, filepath=output_filepath)
        assert os.path.exists(output_filepath)

        df = io.read(filepath=output_filepath)
        assert isinstance(df, pd.DataFrame)

        df = io.read(filepath=output_filepath, columns=["discourse_id", "discourse_type"])
        assert isinstance(df, pd.DataFrame)
        assert df.shape[1] == 2

        filename = "ksdsdsd"
        with pytest.raises(FileNotFoundError):
            io.read(filepath=filename)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


@pytest.mark.io
class TestIOFactory:
    def test_factory(self, caplog, dictionary):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fileformat = "ylkj"
        with pytest.raises(KeyError):
            IOFactory.io(fileformat=fileformat)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
