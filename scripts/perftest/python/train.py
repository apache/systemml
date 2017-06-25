#!/usr/bin/env python3
#-------------------------------------------------------------
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
#-------------------------------------------------------------

import sys
import glob
import os
from os.path import join
from utils import split_rowcol, config_writer, create_dir
from functools import reduce

# Contains configuration setting for training
DATA_FORMAT = 'csv'


def kmeans_train(save_file_name, load_datagen, datagen_dir, train_dir):

    full_path_datagen = join(datagen_dir, load_datagen)
    X = join(full_path_datagen, 'X.data')

    full_path_train = join(train_dir, save_file_name)
    C = join(full_path_train, 'C.data')

    config = dict(X=X, k='50', maxi='50', tol='0.0001', C=C)

    config_writer(full_path_train + '.json', config)

    return full_path_train


def config_packets_train(algo_payload, datagen_dir, train_dir):

    config_bundle = {}

    for current_algo, current_family in algo_payload.items():
        data_gen_path = join(datagen_dir, current_family)
        data_gen_subdir = glob.glob(data_gen_path + "*")
        data_gen_folders = filter(lambda x: os.path.isdir(x), data_gen_subdir)
        config_bundle[current_algo] = []

        for current_folder in data_gen_folders:
            algo_func = current_algo.lower().replace('-', '_') + '_train'
            load_file_name = current_folder.split('/')[-1]
            file_name_split = load_file_name.split('.')
            save_file_name = '.'.join([current_algo] + file_name_split[1:])
            conf_path = globals()[algo_func](save_file_name, load_file_name, datagen_dir, train_dir)
            config_bundle[current_algo].append(conf_path)

    return config_bundle
