/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.api.linalg

object Vector {
  /**
    * Create a Matrix with one row
    * @param values
    * @return
    */
  def apply(values: Array[Double]): Matrix = {
    Matrix(values, values.length, 1)
  }

  def rand(length: Int): Matrix = {
    Matrix.rand(length, 1)
  }

  def zeros(length: Int): Matrix = {
    Matrix.zeros(length, 1)
  }

  def ones(length: Int): Matrix = {
    Matrix.ones(length, 1)
  }
}
