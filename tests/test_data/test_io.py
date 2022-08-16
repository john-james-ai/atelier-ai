#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier - Workspace for Sculpting and Curating Data Science                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /test_io.py                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday August 15th 2022 06:11:16 pm                                                 #
# Modified   : Monday August 15th 2022 07:21:14 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
import pytest
import logging
import pandas as pd

# Enter imports for modules and classes being tested here
from atelier.data.io import IOFactory, CsvIO, YamlIO, PickleIO

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.io
class TestCSVIO:
    def test_io(self, caplog, dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fileformat = "csv"
        output_filepath = "tests/output/io/test.csv"
        io = IOFactory.io(fileformat=fileformat)
        assert isinstance(io, CsvIO)

        io.write(data=dataframe, filepath=output_filepath)
        df = io.read(filepath=output_filepath)
        assert isinstance(df, pd.DataFrame)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_csv_params(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fileformat = "csv"
        output_filepath = "tests/output/io/test.csv"

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
    def test_yml_io(self, caplog, dictionary):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fileformat = "yml"
        output_filepath = "tests/output/io/test.yml"
        io = IOFactory.io(fileformat=fileformat)
        assert isinstance(io, YamlIO)

        io.write(data=dictionary, filepath=output_filepath)
        assert os.path.exists(output_filepath)

        yml = io.read(filepath=output_filepath)
        assert isinstance(yml, dict)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


@pytest.mark.io
class TestPickleIO:
    def test_pickle_io(self, caplog, dictionary):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fileformat = "pickle"
        output_filepath = "tests/output/io/test.pickle"
        io = IOFactory.io(fileformat=fileformat)
        assert isinstance(io, PickleIO)

        io.write(data=dictionary, filepath=output_filepath)
        assert os.path.exists(output_filepath)

        yml = io.read(filepath=output_filepath)
        assert isinstance(yml, dict)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


@pytest.mark.io
class TestIOFactory:
    def test_factory(self, caplog, dictionary):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fileformat = "ylkj"
        with pytest.raises(KeyError):
            IOFactory.io(fileformat=fileformat)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
