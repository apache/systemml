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

# Autogenerated By   : src/main/python/generator/generator.py
# Autogenerated From : scripts/builtin/als.dml

from typing import Dict, Iterable

from systemds.operator import OperationNode, Matrix
from systemds.script_building.dag import OutputType
from systemds.utils.consts import VALID_INPUT_TYPES

def als(X: OperationNode, **kwargs: Dict[str, VALID_INPUT_TYPES]):
    """
    :param X: Location to read the input matrix X to be factorized
    :param rank: Rank of the factorization
    :param reg: Regularization: 
    :param lambda: Regularization parameter, no regularization if 0.0
    :param maxi: Maximum number of iterations
    :param check: Check for convergence after every iteration, i.e., updating U and V once
    :param thr: Assuming check is set to TRUE, the algorithm stops and convergence is declared 
    :param if: in loss in any two consecutive iterations falls below this threshold; 
    :param if: FALSE thr is ignored
    :return: 'OperationNode' containing x n matrix v 
    """
    params_dict = {'X':X}
    params_dict.update(kwargs)
    return OperationNode(X.sds_context, 'als', named_input_nodes=params_dict, output_type=OutputType.LIST, number_of_outputs=2, output_types=[OutputType.MATRIX, OutputType.MATRIX])


    