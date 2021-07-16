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
import difflib
import logging

def get_systemds_root():
    try:
        return os.environ['SYSTEMDS_ROOT']
    except KeyError as error:
        raise KeyError(f"SYSTEMDS_ROOT is not set.\nError\n{error}")
        
def get_sklearn_root():
    return f'{get_systemds_root()}/scripts/staging/sklearn'

def invoke_systemds(path):
    root = get_systemds_root()

    try:
        script_path = os.path.relpath(path, os.getcwd())
        result = subprocess.run([root + "/bin/systemds", script_path, '-nvargs input_X=tests/input_X.csv input_Y=tests/input_Y.csv'],
                             check=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             timeout=10000)
        
        logging.debug('*' * 100)
        logging.debug('\n' + result.stdout.decode('utf-8'))
        logging.debug('\n' + result.stderr.decode('utf-8'))
        logging.debug('*' * 100)
        
        # It looks like python does not notice systemds errors
        # Is 0 returned in error cases?
        # Check if there is any error and raise manually.
        if len(result.stderr) != 0 or 'error' in str(result.stdout).lower():
            raise subprocess.CalledProcessError(returncode=result.returncode, cmd=result.args, 
                                                stderr=result.stderr, output=result.stdout)
        
    except subprocess.CalledProcessError as systemds_error:
        logging.error("Failed to run systemds!")
        logging.error("Error code: " + str(systemds_error.returncode))
        logging.error("Stdout:")
        logging.error(systemds_error.output.decode("utf-8"))
        logging.error("Stderr:")
        logging.error(systemds_error.stderr.decode("utf-8"))
        return False
    logging.info("Successfully executed script.")
    return True

def test_script(path):
    logging.info('#' * 30)
    logging.info('Running generated script on systemds.')
    result = invoke_systemds(path)
    logging.info('Finished test.')
    return result

# Compares two script using diff
def compare_script(actual, expected):
    try:
        f_expected = open(f'{get_sklearn_root()}/tests/expected/{expected}')
        f_actual = open(f'{get_sklearn_root()}/{actual}')
        diff = difflib.ndiff(f_actual.readlines(), f_expected.readlines())
        changes = [l.strip() for l in diff if not l.startswith('  ')]
        logging.info('#' * 30)
        if len(changes) == 0:
            logging.info('Actual script matches expected script.')
            return True
        else:
            logging.info('Actual script does not match expected script.')
            logging.info('Legend:')
            logging.info('    "+ " ... line unique to actual script')
            logging.info('    "- " ... line unique to expected script')
            logging.info('    "? " ... linue not present in either script')
            logging.info('#' * 30)
            logging.info('\n' + '\n'.join(changes))
            logging.info('#' * 30)
            return False
    except Exception as e:
        logging.error('Failed to compare script.')
        logging.error(e)
        return False