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

class LinearSVMMapper(Mapper):
    name = 'l2svm'
    sklearn_name = 'linearsvc'
    is_supervised = True
    mapped_output = [
        'model'
    ]

    def map_params(self):
        self.mapped_params = [
            'TRUE' if self.params.get('fit_intercept', False) else 'FALSE',
            self.params.get('tol', 0.001),
            self.params.get('C', 1.0),
            self.params.get('max_iter', 100),
            20, # maxii parameter is unkown in sklearn and not documented in dml
            'TRUE' if self.params.get('verbose', False) else 'FALSE',
            -1  # column_id is unkown in sklearn
        ]

class TweedieRegressorMapper(Mapper):
    name = 'glm'
    sklearn_name = 'tweedieregressor'
    is_supervised = True
    mapped_output = [
        'beta'
    ]

    def map_params(self):
        # TODO: many parameters cannot be mapped directly:
        # how to handle defaults for dml?
        self.mapped_params = [
            1,  # sklearn impl supports power only, dfam
            self.params.get('power', 0.0),  # vpow
            0,  # link
            1.0,  # lpow
            0.0,  # yneg
            # sklearn does not know last case
            0 if self.params.get('fit_intercept', 1) else 1, # icpt
            0.0,  # disp
            0.0,  # reg
            self.params.get('tol', 0.000001), # tol
            200,  # moi
            0,  # mii,
            'TRUE' if self.params.get('verbose', False) else 'FALSE'
        ]


class LogisticRegressionMapper(Mapper):
    name = 'multiLogReg'
    sklearn_name = 'logisticregression'
    is_supervised = True
    mapped_output = [
        'beta'
    ]

    def map_params(self):
        self.mapped_params = [
            # sklearn does not know last case
            0 if self.params.get('fit_intercept', 1) else 1,
            self.params.get('tol', 0.000001), # tol
            self.params.get('C', 0.0), # reg
            100,  # maxi
            0,  # maxii
            'TRUE' if self.params.get('verbose', False) else 'FALSE'
        ]
