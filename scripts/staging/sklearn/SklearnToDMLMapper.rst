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

SklearnToDMLMapper
==================

SklearnToDMLMapper is a simple tool for transforming scikit-learn pipelines into DML scripts.
This tool may be used over a simple command line interface, where a scikit-learn pipeline provided over a `pickle <https://docs.python.org/3/library/pickle.html>`_ file. Alternatively, SklearnToDMLMapper can be used in a script as a Python module.


Prerequisites
-------------

If a pickle file is provided, no dependecies are necessary except for python 3.6+.
Otherwise, scikit-learn needs to be `installed <https://scikit-learn.org/stable/install.html>`_.

Usage
-----

For usage over the CLI, as example call may look as follows:

    python SklearnToDMLMapper.py -i input -o output_path pipe.pkl

* input: name (prefix) of the input file(s) (see below)
* output_path: transformed pipeline as .dml script
* pipe.pkl: binary file (pickle) of a sklear pipeline

Used as a Python module a script may look as follows::

    from sklearn.pipeline import make_pipeline
    # Other imports from sklearn
    from SklearnToDMLMapper import SklearnToDMLMapper

    pipeline = make_pipeline(...)

    mapper = SklearnToDMLMapper(pipeline, 'input')
    mapper.transform()
    mapper.save('mapped_pipeline')

or, alternatively using a pickle file::

    from SklearnToDMLMapper import SklearnToDMLMapper

    with open('pipeline.pkl', 'rb') as f:
        pipeline = pickle.load(f)

    mapper = SklearnToDMLMapper(pipeline, 'input')
    mapper.transform()
    mapper.save('mapped_pipeline')

API description
---------------

.. autoclass:: SklearnToDMLMapper