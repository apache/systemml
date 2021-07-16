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

from .mapper import Mapper

class StandardScalerMapper(Mapper):
    name = 'scale'
    sklearn_name = 'standardscaler'
    is_intermediate = True
    mapped_output = [
        'Y'
    ]

    def map_params(self):
        self.mapped_params = [
            'TRUE' if self.params.get('with_mean', True) else 'FALSE',
            'TRUE' if self.params.get('with_std', True) else 'FALSE'
        ]

class NormalizeMapper(Mapper):
    name = 'normalize'
    sklearn_name = 'normalizer'
    is_intermediate = True
    mapped_output = [
        'Y'
    ]

    def map_params(self):
        self.mapped_params = []


class SimpleImputerMapper(Mapper):
    name = 'impute'
    sklearn_name = 'simpleimputer'
    is_intermediate = True
    mapped_output = [
        'X'
    ]

    def map_params(self):  # might update naming ?
        if self.params['strategy'] == 'median':
            self.name = 'imputeByMedian'
        else:
            self.name = 'imputeByMean'

        self.mapped_params = []


class PCAMapper(Mapper):
    name = 'pca'
    sklearn_name = 'pca'
    is_intermediate = True
    mapped_output = [
        'Xout',
        'Mout'
    ]

    def map_params(self):
        self.mapped_params = [
            2 if self.params['n_components'] is None else self.params['random_state'],
            'TRUE',  # non existant in SKlearn
            'TRUE'  # non existant in SKlearn
        ]
