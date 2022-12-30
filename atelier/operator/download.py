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
# Modified   : Friday December 30th 2022 09:06:57 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Download Module"""
from abc import abstractmethod
import os
import urllib
import tarfile
from zipfile import ZipFile

from .base import Operator


# ------------------------------------------------------------------------------------------------ #
#                                      DOWNLOADER ABC                                              #
# ------------------------------------------------------------------------------------------------ #
class DownloaderABC(Operator):

    def __init__(self, url: str, destination: str) -> None:
        super().__init__()
        self._url = url
        self._destination = destination

    @abstractmethod
    def execute(self) -> None:
        """Downloads the data to the destination directory."""


# ------------------------------------------------------------------------------------------------ #
#                                      DOWNLOADERFILE                                              #
# ------------------------------------------------------------------------------------------------ #
class DownloaderFile(DownloaderABC):
    """Downloads an uncompressed file from a website.
    Args:
        url (str): The URL to the web resource
        destination (str): A directory into which the source file will be stored. The destination
            file will have the same name as the source file.
    """

    def __init__(self, url: str, destination: str) -> None:
        super().__init__(url=url, destination=destination)

    def execute(self) -> None:
        """Downloads a file from a remote source."""
        try:
            filename = os.path.basename(self._url)
            destination = os.path.join(self._destination, filename)
            _ = urllib.request.urlretrieve(url=self._url, filename=destination)
        except IsADirectoryError:
            msg = "The destination parameter is a directory. For download, this must be a path to a file."
            self._logger.error(msg)
            raise IsADirectoryError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                  DOWNLOAD EXTRACTOR ZIP                                          #
# ------------------------------------------------------------------------------------------------ #
class DownloadExtractorZip(DownloaderABC):
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
class DownloadExtractorTarGZ(DownloaderABC):
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


# ------------------------------------------------------------------------------------------------ #
#                                    DOWNLOADER                                                    #
# ------------------------------------------------------------------------------------------------ #
class Downloader(DownloaderABC):
    """Downloads data from a web source, given a url and a destination directory.
    Args:
        url (str): The URL to the web resource
        destination (str): A directory into which the ZipFile contents will be extracted.
    """
    def __init__(self, url: str, destination: str) -> None:
        super().__init__(url=url, destination=destination)

    def execute(self) -> None:
        """Downloads and decompresses the data if necessary."""
        downloader = self._get_downloader()
        d = downloader(url=self._url, destination=self._destination)
        d.execute()

    def _get_downloader(self) -> DownloaderABC:
        downloaders = {'.zip': DownloadExtractorZip, '.gz': DownloadExtractorTarGZ}
        format = os.path.splitext(self._url)[1]
        try:
            return downloaders[format]
        except KeyError:
            return DownloaderFile
