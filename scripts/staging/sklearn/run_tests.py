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

import sys
import os
import subprocess
import argparse
import logging

from sklearn.preprocessing import StandardScaler, Normalizer
from sklearn.impute import SimpleImputer
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans, DBSCAN
from sklearn.pipeline import make_pipeline
from sklearn.svm import LinearSVC
from sklearn.linear_model import TweedieRegressor, LogisticRegression
from sklearn.mixture import GaussianMixture

from SklearnToDMLMapper import SklearnToDMLMapper
from tests.util import test_script, compare_script, get_systemds_root

def test_valid(name, pipeline):
    mapper = SklearnToDMLMapper(pipeline)
    mapper.transform()
    path = f'{name}_gen.dml'
    mapper.save(path)
    return test_script(path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', action='store', default='ERROR', 
                        help='Set logging level (ERROR, INFO, DEBUG).')

    options = parser.parse_args()
    numeric_level = getattr(logging, options.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {options.log}')
    logging.basicConfig(level=numeric_level)

    try:
        get_systemds_root()
    except Exception as e:
        logging.error(e)
        exit(-1)

    
    valid_pipelines = [
        make_pipeline(StandardScaler(), KMeans()),
        make_pipeline(Normalizer(), KMeans()),
        make_pipeline(SimpleImputer(strategy='mean'), KMeans()),
        make_pipeline(SimpleImputer(strategy='median'), KMeans()),
        make_pipeline(Normalizer(), LinearSVC()),
        make_pipeline(Normalizer(), TweedieRegressor()),
        make_pipeline(StandardScaler(), LogisticRegression()),
        make_pipeline(Normalizer(), LogisticRegression()),
        #TODO: Tests which use PCA or DBSCAN, trigger a NullPointerException during parsing for some reason
        make_pipeline(StandardScaler(), DBSCAN()),
        make_pipeline(Normalizer(), DBSCAN()),
        make_pipeline(SimpleImputer(strategy='mean'), DBSCAN()),
        make_pipeline(SimpleImputer(strategy='median'), DBSCAN()),
        make_pipeline(PCA(), KMeans()),
        make_pipeline(PCA(), DBSCAN()),
        # TODO: GaussianMixtureModel results in LanguageException -- ERROR: [line 0:0] -- Function get_sample_maps() is undefined.
        make_pipeline(StandardScaler(), GaussianMixture()),
        make_pipeline(Normalizer(), GaussianMixture())
    ]

    valid_results = []
    valid_tests_names = []
    for i, pipeline in enumerate(valid_pipelines):
        name = f'test_{i}_' + '_'.join([s[0] for s in pipeline.steps])
        logging.info('*' * 50)
        logging.info((18*'*' + name + (50-20-len(name)) * '*'))
        result = test_valid(name, pipeline)
        valid_results.append(result)
        valid_tests_names.append(name)
    
    print('*' * 50)
    print('Finished all tests.')
    for (name, r) in zip(valid_tests_names, valid_results):
        print('{}: {}'.format(name, 'Failed' if not r else 'Success'))
