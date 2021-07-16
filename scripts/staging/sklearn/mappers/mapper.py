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

import os

builtin_path = "scripts/builtin"

def scripts_home():
    systemds_home = os.getenv('SYSTEMDS_HOME')
    if systemds_home is None:
        return builtin_path
    else:
        return f'{systemds_home}/{builtin_path}'

class Mapper:
    name = None
    sklearn_name = None

    mapped_params = []
    mapped_output = []

    is_intermediate = False
    is_supervised = False

    def __init__(self, params=None):
        self.params = params
        if params is not None:
            self.map_params()

    def get_source(self):
        return 'source("{}/{}.dml") as ns_{}'.format(scripts_home(),
                                                 self.name,
                                                 self.name)

    def get_call(self):
        input_ = ['X', 'Y'] if self.is_supervised else ['X']
        input_ += self.mapped_params
        output_ = ', '.join(self.mapped_output) if not self.is_intermediate else 'X'
        param_ = ', '.join(map(str, input_))
        call = "[{}] = ns_{}::m_{}({})".format(
            output_, self.name, self.name, param_)
        return call

    def map_params(self):
        pass
