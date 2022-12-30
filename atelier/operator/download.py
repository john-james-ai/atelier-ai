#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /atelier/operator/download.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 29th 2022 09:20:10 pm                                             #
# Modified   : Thursday December 29th 2022 09:31:26 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Download Module"""
import urllib
import tarfile
from zipfile import ZipFile

from .base import Operator


# ------------------------------------------------------------------------------------------------ #
#                                        DOWNLOADER                                                #
# ------------------------------------------------------------------------------------------------ #
class Downloader(Operator):
    """Downloads a file from a website. Also base class for DownloadExtractors.
    Args:
        url (str): The URL to the web resource
        destination (str): A path of the downloaded file.
    """

    def __init__(self, url: str, destination: str) -> None:
        super().__init__()
        self._url = url
        self._destination = destination

    def execute(self) -> None:
        """Downloads a file from a remote source."""
        try:
            _ = urllib.request.urlretrieve(url=self._url, filename=self._destination)
        except IsADirectoryError:
            msg = "The destination parameter is a directory. For download, this must be a path to a file."
            self._logger.error(msg)
            raise IsADirectoryError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                  DOWNLOAD EXTRACTOR ZIP                                          #
# ------------------------------------------------------------------------------------------------ #
class DownloadExtractorZip(Downloader):
    """Downloads a ZipFile from a website and extracts the contents a destination directory.
    Args:
        url (str): The URL to the web resource
        destination (str): A directory into which the ZipFile contents will be extracted.
    """

    def __init__(self, url: str, destination: str) -> None:
        super().__init__(url=url, destination=destination)

    def execute(self) -> None:
        """Downloads and extracts the data from a zip file."""
        # Open the url
        zipresp = urllib.request.urlopen(self._url)
        # Create a new file on the hard drive
        tempzip = open("/tmp/tempfile.zip", "wb")
        # Write the contents of the downloaded file into the new file
        tempzip.write(zipresp.read())
        # Close the newly-created file
        tempzip.close()
        # Re-open the newly-created file with ZipFile()
        zf = ZipFile("/tmp/tempfile.zip")
        # Extract its contents into <extraction_path>
        # note that extractall will automatically create the path
        zf.extractall(path=self._destination)
        # close the ZipFile instance
        zf.close()


# ------------------------------------------------------------------------------------------------ #
#                                DOWNLOAD EXTRACTOR TAR GZ                                         #
# ------------------------------------------------------------------------------------------------ #
class DownloadExtractorTarGZ(Downloader):
    """Downloads a Tar GZ from a website and extracts the contents a destination directory.
    Args:
        url (str): The URL to the web resource
        destination (str): A directory into which the ZipFile contents will be extracted.
    """

    def __init__(self, url: str, destination: str) -> None:
        super().__init__(url=url, destination=destination)

    def execute(self) -> None:
        """Downloads and extracts the data from a .tar.gz file."""
        # Open the url
        ftpstream = urllib.request.urlopen(self._url)
        # Create a new file on the hard drive
        targz_file = tarfile.open(fileobj=ftpstream, mode="r|gz")
        # Extract its contents into <extraction_path>
        targz_file.extractall(path=self._destination)
