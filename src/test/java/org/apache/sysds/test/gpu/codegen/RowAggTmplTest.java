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

package org.apache.sysds.test.gpu.codegen;

import org.apache.sysds.test.AutomatedTestBase;
import org.junit.Test;

public class RowAggTmplTest extends AutomatedTestBase {
	org.apache.sysds.test.functions.codegen.RowAggTmplTest dmlTestCase;

	@Override public void setUp() {
		TEST_GPU = true;
		dmlTestCase = new org.apache.sysds.test.functions.codegen.RowAggTmplTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
	}

	@Test public void testCodegenRowAgg1CP() { dmlTestCase.testCodegenRowAgg1CP(); }
	@Test public void testCodegenRowAgg2CP() { dmlTestCase.testCodegenRowAgg2CP(); }
	@Test public void testCodegenRowAgg3CP() { dmlTestCase.testCodegenRowAgg3CP(); }
	@Test public void testCodegenRowAgg4CP() { dmlTestCase.testCodegenRowAgg4CP(); }
	@Test public void testCodegenRowAgg5CP() { dmlTestCase.testCodegenRowAgg5CP(); }
	@Test public void testCodegenRowAgg6CP() { dmlTestCase.testCodegenRowAgg6CP(); }
	@Test public void testCodegenRowAgg7CP() { dmlTestCase.testCodegenRowAgg7CP(); }
	@Test public void testCodegenRowAgg8CP() { dmlTestCase.testCodegenRowAgg8CP(); }
	@Test public void testCodegenRowAgg9CP() { dmlTestCase.testCodegenRowAgg9CP(); }
	@Test public void testCodegenRowAgg10CP() { dmlTestCase.testCodegenRowAgg10CP(); }
	@Test public void testCodegenRowAgg11CP() { dmlTestCase.testCodegenRowAgg11CP(); }
	@Test public void testCodegenRowAgg12CP() { dmlTestCase.testCodegenRowAgg12CP(); }
	@Test public void testCodegenRowAgg13CP() { dmlTestCase.testCodegenRowAgg13CP(); }
	@Test public void testCodegenRowAgg14CP() { dmlTestCase.testCodegenRowAgg14CP(); }
	@Test public void testCodegenRowAgg15CP() { dmlTestCase.testCodegenRowAgg15CP(); }
	@Test public void testCodegenRowAgg16CP() { dmlTestCase.testCodegenRowAgg16CP(); }
	@Test public void testCodegenRowAgg17CP() { dmlTestCase.testCodegenRowAgg17CP(); }
	@Test public void testCodegenRowAgg18CP() { dmlTestCase.testCodegenRowAgg18CP(); }
	@Test public void testCodegenRowAgg19CP() { dmlTestCase.testCodegenRowAgg19CP(); }
	@Test public void testCodegenRowAgg20CP() { dmlTestCase.testCodegenRowAgg20CP(); }
	@Test public void testCodegenRowAgg21CP() { dmlTestCase.testCodegenRowAgg21CP(); }
	@Test public void testCodegenRowAgg22CP() { dmlTestCase.testCodegenRowAgg22CP(); }
	@Test public void testCodegenRowAgg23CP() { dmlTestCase.testCodegenRowAgg23CP(); }
	@Test public void testCodegenRowAgg24CP() { dmlTestCase.testCodegenRowAgg24CP(); }
	@Test public void testCodegenRowAgg25CP() { dmlTestCase.testCodegenRowAgg25CP(); }
	@Test public void testCodegenRowAgg26CP() { dmlTestCase.testCodegenRowAgg26CP(); }
	@Test public void testCodegenRowAgg27CP() { dmlTestCase.testCodegenRowAgg27CP(); }
	@Test public void testCodegenRowAgg28CP() { dmlTestCase.testCodegenRowAgg28CP(); }
	@Test public void testCodegenRowAgg29CP() { dmlTestCase.testCodegenRowAgg29CP(); }
	@Test public void testCodegenRowAgg30CP() { dmlTestCase.testCodegenRowAgg30CP(); }
	@Test public void testCodegenRowAgg31CP() { dmlTestCase.testCodegenRowAgg31CP(); }
	@Test public void testCodegenRowAgg32CP() { dmlTestCase.testCodegenRowAgg32CP(); }
	@Test public void testCodegenRowAgg33CP() { dmlTestCase.testCodegenRowAgg33CP(); }
	@Test public void testCodegenRowAgg34CP() { dmlTestCase.testCodegenRowAgg34CP(); }
	@Test public void testCodegenRowAgg35CP() { dmlTestCase.testCodegenRowAgg35CP(); }
	@Test public void testCodegenRowAgg36CP() { dmlTestCase.testCodegenRowAgg36CP(); }
	@Test public void testCodegenRowAgg37CP() { dmlTestCase.testCodegenRowAgg37CP(); }
	@Test public void testCodegenRowAgg38CP() { dmlTestCase.testCodegenRowAgg38CP(); }
	@Test public void testCodegenRowAgg39CP() { dmlTestCase.testCodegenRowAgg39CP(); }
	@Test public void testCodegenRowAgg40CP() { dmlTestCase.testCodegenRowAgg40CP(); }
	@Test public void testCodegenRowAgg41CP() { dmlTestCase.testCodegenRowAgg41CP(); }
	@Test public void testCodegenRowAgg42CP() { dmlTestCase.testCodegenRowAgg42CP(); }
	@Test public void testCodegenRowAgg43CP() { dmlTestCase.testCodegenRowAgg43CP(); }
	@Test public void testCodegenRowAgg44CP() { dmlTestCase.testCodegenRowAgg44CP(); }
	@Test public void testCodegenRowAgg45CP() { dmlTestCase.testCodegenRowAgg45CP(); }
	@Test public void testCodegenRowAgg46CP() { dmlTestCase.testCodegenRowAgg46CP(); }

	@Test public void testCodegenRowAggRewrite1CP() { dmlTestCase.testCodegenRowAggRewrite1CP(); }
	@Test public void testCodegenRowAggRewrite2CP() { dmlTestCase.testCodegenRowAggRewrite2CP(); }
	@Test public void testCodegenRowAggRewrite3CP() { dmlTestCase.testCodegenRowAggRewrite3CP(); }
	@Test public void testCodegenRowAggRewrite4CP() { dmlTestCase.testCodegenRowAggRewrite4CP(); }
	@Test public void testCodegenRowAggRewrite5CP() { dmlTestCase.testCodegenRowAggRewrite5CP(); }
	@Test public void testCodegenRowAggRewrite6CP() { dmlTestCase.testCodegenRowAggRewrite6CP(); }
	@Test public void testCodegenRowAggRewrite7CP() { dmlTestCase.testCodegenRowAggRewrite7CP(); }
	@Test public void testCodegenRowAggRewrite8CP() { dmlTestCase.testCodegenRowAggRewrite8CP(); }
	@Test public void testCodegenRowAggRewrite9CP() { dmlTestCase.testCodegenRowAggRewrite9CP(); }
	@Test public void testCodegenRowAggRewrite10CP() { dmlTestCase.testCodegenRowAggRewrite10CP(); }
	@Test public void testCodegenRowAggRewrite11CP() { dmlTestCase.testCodegenRowAggRewrite11CP(); }
	@Test public void testCodegenRowAggRewrite12CP() { dmlTestCase.testCodegenRowAggRewrite12CP(); }
	@Test public void testCodegenRowAggRewrite13CP() { dmlTestCase.testCodegenRowAggRewrite13CP(); }
	@Test public void testCodegenRowAggRewrite14CP() { dmlTestCase.testCodegenRowAggRewrite14CP(); }
	@Test public void testCodegenRowAggRewrite15CP() { dmlTestCase.testCodegenRowAggRewrite15CP(); }
	@Test public void testCodegenRowAggRewrite16CP() { dmlTestCase.testCodegenRowAggRewrite16CP(); }
	@Test public void testCodegenRowAggRewrite17CP() { dmlTestCase.testCodegenRowAggRewrite17CP(); }
	@Test public void testCodegenRowAggRewrite18CP() { dmlTestCase.testCodegenRowAggRewrite18CP(); }
	@Test public void testCodegenRowAggRewrite19CP() { dmlTestCase.testCodegenRowAggRewrite19CP(); }
	@Test public void testCodegenRowAggRewrite20CP() { dmlTestCase.testCodegenRowAggRewrite20CP(); }
	@Test public void testCodegenRowAggRewrite21CP() { dmlTestCase.testCodegenRowAggRewrite21CP(); }
	@Test public void testCodegenRowAggRewrite22CP() { dmlTestCase.testCodegenRowAggRewrite22CP(); }
	@Test public void testCodegenRowAggRewrite23CP() { dmlTestCase.testCodegenRowAggRewrite23CP(); }
	@Test public void testCodegenRowAggRewrite24CP() { dmlTestCase.testCodegenRowAggRewrite24CP(); }
	@Test public void testCodegenRowAggRewrite25CP() { dmlTestCase.testCodegenRowAggRewrite25CP(); }
	@Test public void testCodegenRowAggRewrite26CP() { dmlTestCase.testCodegenRowAggRewrite26CP(); }
	@Test public void testCodegenRowAggRewrite27CP() { dmlTestCase.testCodegenRowAggRewrite27CP(); }
	@Test public void testCodegenRowAggRewrite28CP() { dmlTestCase.testCodegenRowAggRewrite28CP(); }
	@Test public void testCodegenRowAggRewrite29CP() { dmlTestCase.testCodegenRowAggRewrite29CP(); }
	@Test public void testCodegenRowAggRewrite30CP() { dmlTestCase.testCodegenRowAggRewrite30CP(); }
	@Test public void testCodegenRowAggRewrite31CP() { dmlTestCase.testCodegenRowAggRewrite31CP(); }
	@Test public void testCodegenRowAggRewrite32CP() { dmlTestCase.testCodegenRowAggRewrite32CP(); }
	@Test public void testCodegenRowAggRewrite33CP() { dmlTestCase.testCodegenRowAggRewrite33CP(); }
	@Test public void testCodegenRowAggRewrite34CP() { dmlTestCase.testCodegenRowAggRewrite34CP(); }
	@Test public void testCodegenRowAggRewrite35CP() { dmlTestCase.testCodegenRowAggRewrite35CP(); }
	@Test public void testCodegenRowAggRewrite36CP() { dmlTestCase.testCodegenRowAggRewrite36CP(); }
	@Test public void testCodegenRowAggRewrite37CP() { dmlTestCase.testCodegenRowAggRewrite37CP(); }
	@Test public void testCodegenRowAggRewrite38CP() { dmlTestCase.testCodegenRowAggRewrite38CP(); }
	@Test public void testCodegenRowAggRewrite39CP() { dmlTestCase.testCodegenRowAggRewrite39CP(); }
	@Test public void testCodegenRowAggRewrite40CP() { dmlTestCase.testCodegenRowAggRewrite40CP(); }
	@Test public void testCodegenRowAggRewrite41CP() { dmlTestCase.testCodegenRowAggRewrite41CP(); }
	@Test public void testCodegenRowAggRewrite42CP() { dmlTestCase.testCodegenRowAggRewrite42CP(); }
	@Test public void testCodegenRowAggRewrite43CP() { dmlTestCase.testCodegenRowAggRewrite43CP(); }
	@Test public void testCodegenRowAggRewrite44CP() { dmlTestCase.testCodegenRowAggRewrite44CP(); }
	@Test public void testCodegenRowAggRewrite45CP() { dmlTestCase.testCodegenRowAggRewrite45CP(); }
	@Test public void testCodegenRowAggRewrite46CP() { dmlTestCase.testCodegenRowAggRewrite46CP(); }

}
