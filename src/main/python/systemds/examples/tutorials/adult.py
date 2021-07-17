# -------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------

import array
import functools
import gzip
import operator
import os
import struct

import numpy as np
import pandas as pd
import requests

class DataManager:

    _train_data_url: str
    _train_labels_url: str
    _test_data_url: str
    _test_labels_url: str

    _train_data_loc: str
    _train_labels_loc: str
    _test_data_loc: str
    _test_labels_loc: str

    _data_columns: []
    _data_string_labels: []

    def __init__(self):
        self._train_data_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data"
        self._test_data_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.test"

        self._train_data_loc = "systemds/examples/tutorials/adult/train_data.csv"
        self._test_data_loc = "systemds/examples/tutorials/adult/test_data.csv"

        self._data_columns = ["age", "workclass", "fnlwgt", "education", "education-num", "marital-status", "occupation",
                   "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", "native-country",
                   "income"]

        self._classification_features_labels = [{'workclass': ['Private', 'Self-emp-not-inc', 'Self-emp-inc', 'Federal-gov', 'Local-gov', 'State-gov', 'Without-pay', 'Never-worked']},
                                    {'education': ['Bachelors', 'Some-college', '11th', 'HS-grad', 'Prof-school', 'Assoc-acdm', 'Assoc-voc', '9th', '7th-8th', '12th', 'Masters', '1st-4th', '10th', 'Doctorate', '5th-6th', 'Preschool']},
                                    {'marital-status': ['Married-civ-spouse', 'Divorced', 'Never-married', 'Separated', 'Widowed', 'Married-spouse-absent', 'Married-AF-spouse']},
                                    {'occupation': ['Tech-support', 'Craft-repair', 'Other-service', 'Sales', 'Exec-managerial', 'Prof-specialty', 'Handlers-cleaners', 'Machine-op-inspct', 'Adm-clerical', 'Farming-fishing', 'Transport-moving', 'Priv-house-serv', 'Protective-serv', 'Armed-Forces']},
                                    {'relationship': ['Wife', 'Own-child', 'Husband', 'Not-in-family', 'Other-relative', 'Unmarried']},
                                    {'race': ['White', 'Asian-Pac-Islander', 'Amer-Indian-Eskimo', 'Other', 'Black']},
                                    {'sex': ['Female', 'Male']},
                                    {'native-country': ['United-States', 'Cambodia', 'England', 'Puerto-Rico', 'Canada', 'Germany', 'Outlying-US(Guam-USVI-etc)', 'India', 'Japan', 'Greece', 'South', 'China', 'Cuba', 'Iran', 'Honduras', 'Philippines', 'Italy', 'Poland', 'Jamaica', 'Vietnam', 'Mexico', 'Portugal', 'Ireland', 'France', 'Dominican-Republic', 'Laos', 'Ecuador', 'Taiwan', 'Haiti', 'Columbia', 'Hungary', 'Guatemala', 'Nicaragua', 'Scotland', 'Thailand', 'Yugoslavia', 'El-Salvador', 'Trinadad&Tobago', 'Peru', 'Hong', 'Holand-Netherlands']},
                                    {'income': ['>50K', '<=50K']}]






    def get_train_data(self) -> np.array:
        self._get_data(self._train_data_url, self._train_data_loc)
        return self._parse_data(self._train_data_loc)\
            .drop(labels=self._data_columns[len(self._data_columns)-1], axis=1).to_numpy()

    def get_train_labels(self) -> np.array:
        self._get_data(self._train_data_url, self._train_data_loc)
        data_list = self._data_columns.copy()
        data_list.pop(len(self._data_columns)-1)
        data = self._parse_data(self._train_data_loc).drop(labels=data_list, axis=1)
        return data.to_numpy().flatten()

    def get_test_data(self) -> np.array:
        self._get_data(self._test_data_url, self._test_data_loc)
        return self._parse_data(self._test_data_loc)\
            .drop(labels=self._data_columns[len(self._data_columns)-1], axis=1).iloc[1:].to_numpy()

    def get_test_labels(self) -> np.array:
        self._get_data(self._test_data_url, self._test_data_loc)
        data_list = self._data_columns.copy()
        data_list.pop(len(self._data_columns)-1)
        data = self._parse_data(self._test_data_loc).drop(labels=data_list, axis=1).iloc[1:]
        data["income"] = data["income"].str.replace('>50K.','>50K', regex=False)
        data["income"] = data["income"].str.replace('<=50K.','<=50K', regex=False)
        return data.to_numpy().flatten()

    def _parse_data(self, loc) -> pd.DataFrame:
        return pd.read_csv(loc, header=None, names=self._data_columns)


    def _get_data(self, url, loc):
        if not os.path.isfile(loc):
            myfile = requests.get(url)
            folder = os.path.dirname(loc)
            if not os.path.isdir(folder):
                os.makedirs(folder)
            with open(loc, 'wb') as f:
                f.write(myfile.content)


    def get_preprocessed_dataset(self, interpolate=False, standardize=False, dimred=0):
        train_array = np.concatenate([self.get_train_data(), self.get_train_labels()[...,np.newaxis]], axis=1)
        train_dataset = pd.DataFrame(train_array, columns=self._data_columns)
        test_array = np.concatenate([self.get_test_data(), self.get_test_labels()[...,np.newaxis]], axis=1)
        test_dataset = pd.DataFrame(test_array, columns=self._data_columns)

        if not interpolate:
            train_dataset = train_dataset[~(train_dataset.astype(str) == ' ?').any(1)]
            test_dataset = test_dataset[~(test_dataset.astype(str) == ' ?').any(1)]

        train_len = len(train_dataset)

        combined_dataset = train_dataset.append(test_dataset, ignore_index=True, sort=False)
        conditional_labels = [list(dic.keys())[0]for dic in self._classification_features_labels]
        combined_dataset_frame = combined_dataset.copy().drop(labels=conditional_labels, axis=1)
        combined_dataset_frame = combined_dataset_frame.apply(pd.to_numeric)

        if standardize:
            train_data = combined_dataset_frame.iloc[0:train_len,:]
            test_data = combined_dataset_frame.iloc[train_len:,:]
            train_mean = train_data.mean(axis=0)
            train_std = train_data.std(axis=0)
            train_data = (train_data - train_mean)/train_std
            test_data = (test_data - train_mean)/train_std
            combined_dataset_frame = train_data.append(test_data, ignore_index=True, sort=False)

        for x in self._classification_features_labels:
            #insert most common string
            current_frame = combined_dataset[list(x.keys())[0]]
            if interpolate:
                most_common_category = current_frame.iloc[:train_len].mode()
                current_frame = current_frame.str.replace(' ?', most_common_category.iloc[0], regex=False)
            if dimred > 0 and dimred <= 1:
                labels_percent = (current_frame.iloc[:train_len].value_counts() / train_len)
                labels_to_combine = labels_percent.index[labels_percent < dimred]
                current_frame = current_frame.str.replace("|".join(labels_to_combine), " other", regex=True)

            converted_one_hot_column = pd.get_dummies(current_frame, prefix=x.keys())
            combined_dataset_frame = pd.concat([combined_dataset_frame, converted_one_hot_column],  axis=1, join="outer", sort=False)

        processed_labels = combined_dataset_frame.iloc[: , -1:]
        combined_dataset_frame = combined_dataset_frame.iloc[:, :-2]

        train_data =  combined_dataset_frame.iloc[0:train_len,:].to_numpy()
        train_labels = processed_labels.iloc[0:train_len,:].to_numpy().flatten()
        test_data =  combined_dataset_frame.iloc[train_len:,:].to_numpy()
        test_labels = processed_labels.iloc[train_len:,:].to_numpy().flatten()

        return train_data, train_labels, test_data, test_labels
