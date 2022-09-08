#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /split.py                                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 7th 2022 08:18:20 am                                            #
# Modified   : Thursday September 8th 2022 01:04:55 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pandas as pd
import numpy as np
from scipy.stats import f_oneway
from sklearn.model_selection import train_test_split

# ------------------------------------------------------------------------------------------------ #


class DistributionPreservingSplitter:
    """Distribution Preserving Train/Test Splits

    Splits a file into training and test sets such that the distribution of the target variable
    in the original dataset is preserved in the splits.

    Args:
        n_iter (int): Optional. The number of iterations from which random splits will be
            generated. Defaults to 100.

    """

    def __init__(self, n_iter: int = 100) -> None:
        self._n_iter = n_iter  # The number of random split generation iterations to try
        self._target = None  # Target variable from original dataframe.

    def split(self, data: pd.DataFrame, target_vars: list, p_train: float, p_test: float) -> tuple:
        """Splits the data according to the proportions in p_train and p_test

        Args:
            data (pd.DataFrame): Data in pandas DataFrame format.
            target_vars (list): List of continuous target variable(s) for which the distribution(s)
                should be preserved in the training and test sets.
            p_train (float): Number in (0,1), representing the proportion of the original
                data designated for the training set.
            p_test (float): Number in (0,1), representing the proportion of the original
                data designated for the test set.

        Note: p_train + p_test != 1, the proportions will be normalized by the sum
            of the proportions.
        """
        splits = []
        f_scores = []

        # Storing the data to avoid references being passed back and forth between methods
        # for each iteration.
        self._data = data
        self._target_vars = target_vars
        indices = data.index

        # Normalize the train/test proportions
        p_train, p_test = self._normalize_proportions(p_train, p_test)

        # Perform n_iter split trials
        for i in range(self._n_iter):
            train, test = train_test_split(
                indices, test_size=p_test, train_size=p_train, shuffle=True
            )
            splits.append([train, test])
            f_score = self._compare_distributions(train, test)
            f_scores.append(f_score)

        # Obtain the index for the minimum total f_score returned from the above function
        idx = np.argmin(f_scores)

        # Get the indices from the training and test split having the lowest total f_score
        train, test = splits[idx]

        # Filter the data by the train, test indices and return
        train = self._data.loc[train]
        test = self._data.loc[test]

        return train, test

    def _normalize_proportions(self, p_train: float, p_test: float) -> [float, float]:
        """Normalizes the proportions so that they add to one."""
        total_prop = p_train + p_test
        p_train = p_train / total_prop
        p_test = p_test / total_prop

        return p_train, p_test

    def _compare_distributions(self, train: np.array, test: np.array) -> float:
        """Compares original and train, test and returns total Anova f_score

        Args:
            train (np.array): indexes into the original dataframe containing train data.
            test (np.array): indexes into the original dataframe containing test data.
        """
        total_f_score = 0

        for target_var in self._target_vars:
            train_targets = self._data.loc[train, target_var]
            test_targets = self._data.loc[test, target_var]

            target_data = self._data[target_var]

            f_train, p_value = f_oneway(target_data, train_targets)
            f_test, p_value = f_oneway(target_data, test_targets)

            total_f_score += f_train + f_test

        return total_f_score
