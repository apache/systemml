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

import pickle

def dump():
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans

    from sklearn.pipeline import make_pipeline

    pipeline = make_pipeline(StandardScaler(), KMeans())

    print('Sklearn pipeline:')
    print(pipeline)

    with open('pipe.pkl', 'wb') as f:
        pickle.dump(pipeline, f)

    print()
dump()

# source scripts from /scripts/builtin/
# call functions: https://apache.github.io/systemds/site/dml-language-reference.html#user-defined-function-udf

def map_lm(sklearn_func):
    # TODO
    call = 'm_lm()'.format()

def map_kmeans(sklearn_func):
    '''
    m_kmeans = function(Matrix[Double] X, Integer k = 10, Integer runs = 10, Integer max_iter = 1000,
    Double eps = 0.000001, Boolean is_verbose = FALSE, Integer avg_sample_size_per_centroid = 50,
    Integer seed = -1)
    return (Matrix[Double] C, Matrix[Double] Y)
    '''
    params = sklearn_func.get_params()
    return 'm_kmeans(X, {}, {}, {})'.format(params['n_clusters'], params['n_init'], params['max_iter'], params['tol'])

def map_scale(sklearn_func):
    params = sklearn_func.get_params()
    # handle default params as in dml definiton
    # handle type mappings
    return 'm_scale(X, {}, {})'.format(params['with_mean'], params['with_std'])
    
algorithms = {
    "linearregression": ("lm", map_lm),
    "standardscaler": ("scale", map_scale),
    "kmeans": ("kmeans", map_kmeans)
}

# use setwd for this?
builtin_path = "scripts/builtin"

sources = []

dml_pipeline = []

# use jinja templating for this?
# source directory?
# create sperate source file which contains supported algorithms
# and combine into common namespace?

# validate contents of pipeline:
# intermediate steps need to be transformative
# and the last step fits an estimator
# see https://scikit-learn.org/stable/modules/generated/sklearn.pipeline.Pipeline.html

with open('pipe.pkl', 'rb') as f:
    loaded = pickle.load(f)

for i, (sklearn_name, algorithm) in enumerate(loaded.steps):
    name, mapping = algorithms[sklearn_name]
    call = mapping(algorithm)
    sources.append('source("{}/{}") as ns_{}'.format(builtin_path, name, name))
    # step_i will be needed in following steps
    dml_pipeline.append('step_{} = ns_{}::{}'.format(i, name, call))

dml_script = '\n'.join(sources)
dml_script += '\n\n'
dml_script += '\n'.join(dml_pipeline)

print('DML Script')
print(dml_script)
