#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : \operators.py                                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday August 11th 2022 09:43:52 pm                                               #
# Modified   : Tuesday August 16th 2022 05:44:10 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Operator Module"""
import os
import shlex
import subprocess
import zipfile
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
import logging

from atelier.data.io import IOFactory

# ------------------------------------------------------------------------------------------------ #
logging.getLogger(__name__).addHandler(logging.NullHandler())

# ------------------------------------------------------------------------------------------------ #
#                                OPERATOR BASE CLASS                                               #
# ------------------------------------------------------------------------------------------------ #


class Operator(ABC):
    """Abstract class for operator classes

    Args:
        seq (int): Sequence number of operation in a pipeline.
        params (Any): Parameters for the operation.

    Class Variables:
        __name (str): The human-reedable name for the operator
        __desc (str): String describing what the operator does.

    """

    def __init__(self, name: str, params: dict = {}) -> None:
        self._name = name
        self._params = params

        self._created = datetime.now()
        self._started = None
        self._stopped = None
        self._duration = None

    def __str__(self) -> str:
        return str(
            "Sequence #: {}\tOperator: {}\t{}\tParams: {}".format(
                self._seq, Operator.__name, Operator.__desc, self._params
            )
        )

    def run(self, data: Any = None, context: dict = {}) -> Any:
        self._setup()
        data = self.execute(data=data, context=context)
        self._teardown()
        return data

    @abstractmethod
    def execute(self, data: Any = None, context: dict = {}) -> Any:
        pass

    @property
    def name(self) -> str:
        return self._name

    @property
    def params(self) -> Any:
        return self._params

    @property
    def created(self) -> datetime:
        return self._created

    @property
    def started(self) -> datetime:
        return self._started

    @property
    def stopped(self) -> datetime:
        return self._stopped

    @property
    def duration(self) -> datetime:
        return self._duration

    def _setup(self) -> None:
        self._started = datetime.now()

    def _teardown(self) -> None:
        self._stopped = datetime.now()
        self._duration = round((self._stopped - self._started).total_seconds(), 4)


# ------------------------------------------------------------------------------------------------ #
#                                   FILE OPERATORS                                                 #
# ------------------------------------------------------------------------------------------------ #


class KaggleDownloader(Operator):
    """Downloads Dataset from Kaggle using the Kaggle API

    Args:
        name (str): name for the step in the pipeline
        competition (str): The name of the competition. Refer
            to the api syntax on the competition data page
        filename (str): The name of the zip file. This may be obtained by clicking on the
            'Download All' button on the data page.
        destination (str): The folder to which the data will be downloaded.
        force (bool): Indicates whether to force execution if current data exists.
    """

    def __init__(self, name: str = None, params: dict = {}) -> None:
        super(KaggleDownloader, self).__init__(name=name, params=params)
        competition = self._params.get("competition", None)
        self._filename = self._params.get("filename", None)
        self._destination = self._params.get("destination", None)
        self._command = (
            "kaggle competitions download" + " -p " + self._destination + " -c " + competition
        )
        self._force = self._params.get("force", None)

    def execute(self, data: Any = None, context: dict = {}) -> Any:
        """Downloads compressed data via an API using bash

        Args:
            data: Not used
            context: not used.
        """
        if self._force or not os.path.exists(os.path.join(self._destination, self._filename)):
            try:
                os.makedirs(self._destination, exist_ok=True)
                subprocess.run(shlex.split(self._command), check=True, text=True, shell=False)
            except subprocess.CalledProcessError as e:
                logging.error(e.output)


# ------------------------------------------------------------------------------------------------ #


class ExtractZip(Operator):
    """Extracts files from a Zip archive

    Args:
        name (str): The name for the step in the pipeline.
        source (str): The filepath for the source zip file.
        destination (str): The folder into which the zip will be extracted
        force (bool): Whether to overwrite existing data. If data already exists
            then force must be True, otherwise the step is not executed.

    """

    def __init__(self, name: str = None, params: dict = {}) -> None:
        super(ExtractZip, self).__init__(name=name, params=params)
        self._source = self._params.get("source", None)
        self._destination = self._params.get("destination", None)
        self._force = self._params.get("force", None)

    def execute(self, data: Any = None, context: dict = {}) -> None:
        """Decompresses a zipfile from source and stores contents at destination.

        Args:
            data: Not used
            context: not used.
        """
        print(
            "Force is {}\t Directory {} contains {} files".format(
                self._force, self._destination, str(len(os.listdir(self._destination)))
            )
        )
        if self._force or not len(os.listdir(self._destination)) > 0:
            os.makedirs(self._destination, exist_ok=True)

            with zipfile.ZipFile(self._source, "r") as zf:
                zf.extractall(self._destination)


# ------------------------------------------------------------------------------------------------ #


class LoadCSV(Operator):
    """Loads a CSV File into a pandas DataFrame

    Args:
        name (str): The name for the step in the pipeline.
        filepath (str): The location of the csv file.
        encoding_errors (str): Pandas treatment of encoding errors. See pandas documentation.
    """

    def __init__(self, name: str = None, params: dict = {}) -> None:
        super(LoadCSV, self).__init__(name=name, params=params)

        self._filepath = self._params.get("filepath", None)
        self._encoding_errors = self._params.get("encoding_errors", "strict")
        fileformat = os.path.splitext(self._filepath)[1].replace(".", "")
        self._io = IOFactory.io(fileformat=fileformat)

    def execute(self, data: Any = None, context: dict = {}) -> pd.DataFrame:
        """Loads data from a csv file into a DataFrame

        Args:
            data: Not used
            context: not used.
        Returns:
            DataFrame
        """

        try:
            return self._io.read(filepath=self._filepath, encoding_errors=self._encoding_errors)

        except UnicodeError as e:
            logging.error("Encoding error with {} error handling".format(self._encoding_errors))
            raise (e)

        except FileNotFoundError as e:
            logging.error("File {} not found.".format(self._filepath))
            raise (e)


# ------------------------------------------------------------------------------------------------ #


class SaveCSV(Operator):
    """Saves a pandas DataFrame to CSV File

    Args:
        name (str): The name for the step in the pipeline.
        filepath (str): The location of the csv file.
        force (bool): Whether to overwrite existing data. If data already exists
            then force must be True, otherwise the step is not executed.
    """

    def __init__(self, name: str = None, params: dict = {}) -> None:
        super(SaveCSV, self).__init__(name=name, params=params)

        self._filepath = self._params.get("filepath", None)
        self._force = self._params.get("force", False)
        fileformat = os.path.splitext(self._filepath)[1].replace(".", "")
        self._io = IOFactory.io(fileformat=fileformat)

    def execute(self, data: Any = None, context: dict = {}) -> None:
        """Loads data from a csv file into a DataFrame

        Args:
            data (DataFrame): Contains data to be saved.
            context: not used.

        """
        if not os.path.exists(self._filepath) or self._force:
            os.makedirs(os.path.dirname(self._filepath), exist_ok=True)
            self._io.write(data=data, filepath=self._filepath)


# ------------------------------------------------------------------------------------------------ #


class LoadParquet(Operator):
    """Loads a pandas DataFrame from a Parquet File

    Args:
        name (str): The name for the step in the pipeline.
        filepath (str): The location of the parquet file.
    """

    def __init__(self, name: str = None, params: dict = {}) -> None:
        super(LoadParquet, self).__init__(name=name, params=params)
        self._filepath = self._params.get("filepath", None)
        fileformat = os.path.splitext(self._filepath)[1].replace(".", "")
        self._io = IOFactory.io(fileformat=fileformat)

    def execute(self, data: Any = None, context: dict = {}) -> pd.DataFrame:
        """Loads data from a csv file into a DataFrame

        Args:
            data: Not used
            context: not used.
        Returns:
            DataFrame
        """
        return self._io.read(filepath=self._filepath)


# ------------------------------------------------------------------------------------------------ #


class SaveParquet(Operator):
    """Saves a pandas DataFrame to a parquet file

    Args:
        name (str): The name for the step in the pipeline.
        filepath (str): The location of the parquet file.
        force (bool): Whether to overwrite existing data. If data already exists
            then force must be True, otherwise the step is not executed.
    """

    def __init__(self, name: str = None, params: dict = {}) -> None:
        super(SaveParquet, self).__init__(name=name, params=params)
        self._filepath = self._params.get("filepath", None)
        self._force = self._params.get("force", False)
        fileformat = os.path.splitext(self._filepath)[1].replace(".", "")
        self._io = IOFactory.io(fileformat=fileformat)

    def execute(self, data: Any = None, context: dict = {}) -> None:
        """Loads data from a csv file into a DataFrame

        Args:
            data (DataFrame): Contains data to be saved.
            context: not used.

        """
        if self._force or not os.path.exists(self._filepath):
            os.makedirs(os.path.dirname(self._filepath), exist_ok=True)
            self._io.write(data=data, filepath=self._filepath)


# ------------------------------------------------------------------------------------------------ #
#                                  DATA OPERATORS                                                  #
# ------------------------------------------------------------------------------------------------ #


class Encoder(Operator):
    """Encodes a DataFrame

    Args:
        name (str): The name for the step in the pipeline.
        encoding (str): The encoding system, e.g. 'utf-9'
    """

    def __init__(self, name: str = None, params: dict = {}) -> None:
        super(Encoder, self).__init__(name=name, params=params)
        self._encoding = self._params.get("encoding", "utf-8")
        self._column = self._params.get("column", None)

    def execute(self, data: Any = None, context: dict = {}) -> pd.DataFrame:
        """Loads data from a csv file into a DataFrame

        Args:
            data (DataFrame): Contains data to be encoded.
            context: not used.

        """
        try:
            data[self._column] = data[self._column].str.encode("utf-8", "strict")
        except UnicodeEncodeError as e:
            logging.error("Error encoding data")
            raise (e)


# ------------------------------------------------------------------------------------------------ #


class SampleDataFrame(Operator):
    """Samples a pandas DataFrame

    Args:
        n (int): The number of rows to sample
        frac (float): The rows return as a proportion of size of the full DataFrame
        replace (bool): Indicates whether to sample with replacement. Defaults to false
        random_state (int): Random seed for reproducibility
        ignore_index (bool): If True, the original indexes will be reset.
    """

    def __init__(self, name: str = None, params: dict = {}) -> None:
        super(SampleDataFrame, self).__init__(name=name, params=params)

        self._n = self._params.get("n", None)
        self._frac = self._params.get("frac", None)
        self._replace = self._params.get("replace", False)
        self._random_state = self._params.get("random_state", None)
        self._ignore_index = self._params.get("ignore_index", False)

    def execute(self, data: Any = None, context: dict = {}) -> pd.DataFrame:
        """Loads data from a csv file into a DataFrame

        Args:
            data (DataFrame): Contains data to be encoded.
            context: not used.

        """
        if self._frac > 1:
            self._replace = True

        return data.sample(
            n=self._n,
            frac=self._frac,
            replace=self._replace,
            random_state=self._random_state,
            ignore_index=self._ignore_index,
        )
