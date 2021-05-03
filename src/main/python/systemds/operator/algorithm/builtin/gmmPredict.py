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
# Autogenerated From : scripts/builtin/gmmPredict.dml

from typing import Dict, Iterable

from systemds.operator import OperationNode, Matrix
from systemds.script_building.dag import OutputType
from systemds.utils.consts import VALID_INPUT_TYPES

def gmmPredict(X: OperationNode, weight: OperationNode, mu: OperationNode, precisions_cholesky: OperationNode, model: str) -> Matrix:
    """
    :param X: Matrix X (instances to be clustered)
    :param weight: Weight of learned model
    :param mu: fitted clusters mean
    :param precisions_cholesky: fitted precision matrix for each mixture
    :param model: fitted model
    :return: 'OperationNode' containing predicted cluster labels & probabilities of belongingness & for new instances given the variance and mean of fitted data 
    """
    
    X._check_matrix_op()
    weight._check_matrix_op()
    mu._check_matrix_op()
    precisions_cholesky._check_matrix_op()
    params_dict = {'X':X, 'weight':weight, 'mu':mu, 'precisions_cholesky':precisions_cholesky, 'model':model}
    return OperationNode(X.sds_context, 'gmmPredict', named_input_nodes=params_dict, output_type=OutputType.LIST, number_of_outputs=2, output_types=[OutputType.MATRIX, OutputType.MATRIX])


    