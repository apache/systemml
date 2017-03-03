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

package org.apache.sysml.api

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FreeSpec, Matchers}

/**
  * Specification for the SystemML API
  *
  * This specification follows the DML Language reference from https://apache.github.io/incubator-systemml/dml-language-reference
  * The goal is to support all Types, Expressions, and Statements that SystemML offers.
  *
  * This is not the specification for the supported Scala source language!
  */
@RunWith(classOf[JUnitRunner])
trait BaseAPISpec extends FreeSpec with Matchers {

}
