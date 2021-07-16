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

Scikit-learn - Importer
=======================
Scikit-learn_ is a very popular and well established open-source python library for data science applications. A large number of common algorithms and many useful tools are implemented and maintained. 

Idea
----
Allowing the import of sklearn models, allows for an easy extension of already established implementations with systemds.

Current State
^^^^^^^^^^^^^
Currently a ONNX_ importer is in staging, which is somehow broken or rather breaks something else in the stable branch.

This tool allows the conversion of ONNX graphs to dml.

The Importer
------------
We have following idea. Since the ONNX importer is (to some degree) working, we suggest the modification/extension of the importer to work with sklearn models.

When reading from a saved model we expect for either approach a pickle_ serialized python object, since scikit-learn uses Python's built-in `persistence model`_. joblib_ is a pickle replacement, which works more efficient on large/complex objects, which is the case with some scikit-learn models. In both cases there are some security and maintainability concerns_ to be kept in mind.

Proposal 1
^^^^^^^^^^
One possible approach to this problem is a direct mapping from scikit-learn to DML. But the effort for this approach may be out of scope for this pull request (for now). 

Proposal 2
^^^^^^^^^^
An easier approach would involve a indirect mapping to ONNX and then to DML:

sklearn --> onnx && onnx --> dml ==> sklearn --> dml

Sklearn models may be converted to ONNX using the sklearn-onnx_ converter, part of the official ONNX project. The conversion from ONNX to DML can be accomplished using the existing ONNX-Importer of systemds.

This approach requires fixing the onnx importer and the inclusion of a further dependency. sklearn-onnx_ is published under a MIT license and requires a few other dependencies_.


.. _Scikit-learn: https://scikit-learn.org/stable/index.html
.. _sklearn-onnx: https://github.com/onnx/sklearn-onnx
.. _dependencies: https://github.com/onnx/sklearn-onnx/blob/master/requirements.txt
.. _ONNX: http://onnx.ai/sklearn-onnx/
.. _sklearn_related: https://scikit-learn.org/stable/related_projects.html#related-projects
.. _`persistence model`: https://scikit-learn.org/stable/modules/model_persistence.html
.. _pickle: https://docs.python.org/3/library/pickle.html
.. _joblib: https://joblib.readthedocs.io/en/latest/persistence.html
.. _concerns: https://scikit-learn.org/stable/modules/model_persistence.html#security-maintainability-limitations
