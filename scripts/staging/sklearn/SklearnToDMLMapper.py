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
import sys
import inspect
import mappers
import argparse

class SklearnToDMLMapper:
    """ SklearnToDMLMapper is a simple tool for transforming scikit-learn pipelines into DML scripts.
        This tool may be used over a simple command line interface, where a scikit-learn pipeline provided over 
        a pickle file. Alternatively, SklearnToDMLMapper can be used in a script as a Python module.

        Args:
            pipeline (sklearn.pipeline.Pipeline): sklearn pipeline
            input_name (str, optional): Name for the input variable (prefix). Defaults to 'input'. 
                                        Depending on the pipeline two files are necessary.
                                        Example: input_name="input". Maps to files input_X.csv and input_Y.csv 
                                        for a pipeline ending in a supervised algorithm.
    """
    def __init__(self, pipeline, input_name='input'):
        """Create an SklearnToDMLMapper."""
        self.steps = pipeline.steps
        self.functions = self.__get_functions()
        self.dml_script = None
        self.input_name = input_name

    def __get_functions(self):
        clsmembers = inspect.getmembers(sys.modules['mappers'], inspect.isclass)
        functions = {}
        for cls in clsmembers:
            instance = cls[1]()
            if instance.sklearn_name is not None:
                functions[instance.sklearn_name] = cls[1]

        return functions

    def __get_input(self):
        # Get last function (an algorithm)
        func = self.functions[self.steps[-1][0]]()
        if func is None:
            raise RuntimeError(f'{self.steps[-1][0]} is not supported.')

        if func.is_supervised:
            return f'X = read(${self.input_name}_X)\nY = read(${self.input_name}_Y)'
        else:
            return f'X = read(${self.input_name}_X)'

    def __get_output(self):
        func = self.functions[self.steps[-1][0]]()
        if func is None:
            raise RuntimeError(f'{self.steps[-1][0]} is not supported.')
        return '\n'.join([f'write({output}, "{output}.csv")' for output in func.mapped_output])

    def transform(self):
        """Transforms a sklearn pipeline in a .dml script. 

        Returns:
            str: The transformed .dml script.
        """
        sources = []
        calls = []

        for name, step in self.steps:
            if name not in self.functions:
                continue

            mapper = self.functions[name](step.get_params())
            calls.append(mapper.get_call())
            sources.append(mapper.get_source())

        self.dml_script = "{}\n\n{}\n\n{}\n\n{}".format('\n'.join(sources), 
                                                        self.__get_input(), 
                                                        '\n'.join(calls), 
                                                        self.__get_output())
        return self.dml_script

    def save(self, path):
        """Saves the transformed .dml script.

        Args:
            path (str): Location where the DML script is to be saved.

        Raises:
            RuntimeError: Save can only be called if a transformation was executed beforehand.
        """
        if self.dml_script is None:
            raise RuntimeError('Transformation was not applied yet.')

        with open(path, 'w') as f:
            f.write(self.dml_script)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Tool that parses a sklearn pipeline and produces a dml script')
    parser.add_argument('Path',
            metavar='path',
            type=str,
            help='Location of the sklearn pipeline saved as pickle file')
    parser.add_argument('-i',
            metavar='input_name',
            type=str,
            default='X',
            help='Name for the input variable (prefix). Depending on the pipeline two files are necessary. Example: input_name="input". Maps to files input_X.csv and input_Y.csv for a pipeline ending in a supervised algorithm.')
    parser.add_argument('-o',
            metavar='output',
            type=str,
            default='./pipeline.dml',
            help='Path for the dml output script')

    args = parser.parse_args()

    try:
        with open(args['path'], 'rb') as f:
            pipeline = pickle.load(f)

        mapper = SklearnToDMLMapper(pipeline, args['input_name'])
        mapper.transform()
        mapper.save(args['output'])
    except Exception as e:
        print(f'Failed to transform pipeline.\nError:\n{e}')