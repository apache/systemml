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

public class CellwiseTmplTest extends AutomatedTestBase {
	org.apache.sysds.test.functions.codegen.CellwiseTmplTest dmlTestCase;

	@Override public void setUp() {
		TEST_GPU = true;
		dmlTestCase = new org.apache.sysds.test.functions.codegen.CellwiseTmplTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
	}

	@Test public void testCodegenCellwise1() { dmlTestCase.testCodegenCellwise1(); }
	@Test public void testCodegenCellwise2() { dmlTestCase.testCodegenCellwise2(); }
	@Test public void testCodegenCellwise3() { dmlTestCase.testCodegenCellwise3(); }
	@Test public void testCodegenCellwise4() { dmlTestCase.testCodegenCellwise4(); }
	@Test public void testCodegenCellwise5() { dmlTestCase.testCodegenCellwise5(); }
	@Test public void testCodegenCellwise6() { dmlTestCase.testCodegenCellwise6(); }
	@Test public void testCodegenCellwise7() { dmlTestCase.testCodegenCellwise7(); }
	@Test public void testCodegenCellwise8() { dmlTestCase.testCodegenCellwise8(); }
	@Test public void testCodegenCellwise9() { dmlTestCase.testCodegenCellwise9(); }
	@Test public void testCodegenCellwise10() { dmlTestCase.testCodegenCellwise10(); }
	@Test public void testCodegenCellwise11() { dmlTestCase.testCodegenCellwise11(); }
	@Test public void testCodegenCellwise12() { dmlTestCase.testCodegenCellwise12(); }
	@Test public void testCodegenCellwise13() { dmlTestCase.testCodegenCellwise13(); }
	@Test public void testCodegenCellwise14() { dmlTestCase.testCodegenCellwise14(); }
	@Test public void testCodegenCellwise15() { dmlTestCase.testCodegenCellwise15(); }
	@Test public void testCodegenCellwise16() { dmlTestCase.testCodegenCellwise16(); }
	@Test public void testCodegenCellwise17() { dmlTestCase.testCodegenCellwise17(); }
	@Test public void testCodegenCellwise18() { dmlTestCase.testCodegenCellwise18(); }
	@Test public void testCodegenCellwise19() { dmlTestCase.testCodegenCellwise19(); }
	@Test public void testCodegenCellwise20() { dmlTestCase.testCodegenCellwise20(); }
	@Test public void testCodegenCellwise21() { dmlTestCase.testCodegenCellwise21(); }
	@Test public void testCodegenCellwise22() { dmlTestCase.testCodegenCellwise22(); }
	@Test public void testCodegenCellwise23() { dmlTestCase.testCodegenCellwise23(); }
	@Test public void testCodegenCellwise24() { dmlTestCase.testCodegenCellwise24(); }
	@Test public void testCodegenCellwise25() { dmlTestCase.testCodegenCellwise25(); }
	@Test public void testCodegenCellwise26() { dmlTestCase.testCodegenCellwise26(); }
	@Test public void testCodegenCellwise27() { dmlTestCase.testCodegenCellwise27(); }

	@Test public void testCodegenCellwiseRewrite1() { dmlTestCase.testCodegenCellwiseRewrite1(); }
	@Test public void testCodegenCellwiseRewrite2() { dmlTestCase.testCodegenCellwiseRewrite2(); }
	@Test public void testCodegenCellwiseRewrite3() { dmlTestCase.testCodegenCellwiseRewrite3(); }
	@Test public void testCodegenCellwiseRewrite4() { dmlTestCase.testCodegenCellwiseRewrite4(); }
	@Test public void testCodegenCellwiseRewrite5() { dmlTestCase.testCodegenCellwiseRewrite5(); }
	@Test public void testCodegenCellwiseRewrite6() { dmlTestCase.testCodegenCellwiseRewrite6(); }
	@Test public void testCodegenCellwiseRewrite7() { dmlTestCase.testCodegenCellwiseRewrite7(); }
	@Test public void testCodegenCellwiseRewrite8() { dmlTestCase.testCodegenCellwiseRewrite8(); }
	@Test public void testCodegenCellwiseRewrite9() { dmlTestCase.testCodegenCellwiseRewrite9(); }
	@Test public void testCodegenCellwiseRewrite10() { dmlTestCase.testCodegenCellwiseRewrite10(); }
	@Test public void testCodegenCellwiseRewrite11() { dmlTestCase.testCodegenCellwiseRewrite11(); }
	@Test public void testCodegenCellwiseRewrite12() { dmlTestCase.testCodegenCellwiseRewrite12(); }
	@Test public void testCodegenCellwiseRewrite13() { dmlTestCase.testCodegenCellwiseRewrite13(); }
	@Test public void testCodegenCellwiseRewrite14() { dmlTestCase.testCodegenCellwiseRewrite14(); }
	@Test public void testCodegenCellwiseRewrite15() { dmlTestCase.testCodegenCellwiseRewrite15(); }
	@Test public void testCodegenCellwiseRewrite16() { dmlTestCase.testCodegenCellwiseRewrite16(); }
	@Test public void testCodegenCellwiseRewrite17() { dmlTestCase.testCodegenCellwiseRewrite17(); }
	@Test public void testCodegenCellwiseRewrite18() { dmlTestCase.testCodegenCellwiseRewrite18(); }
	@Test public void testCodegenCellwiseRewrite19() { dmlTestCase.testCodegenCellwiseRewrite19(); }
	@Test public void testCodegenCellwiseRewrite20() { dmlTestCase.testCodegenCellwiseRewrite20(); }
	@Test public void testCodegenCellwiseRewrite21() { dmlTestCase.testCodegenCellwiseRewrite21(); }
	@Test public void testCodegenCellwiseRewrite22() { dmlTestCase.testCodegenCellwiseRewrite22(); }
	@Test public void testCodegenCellwiseRewrite23() { dmlTestCase.testCodegenCellwiseRewrite23(); }
	@Test public void testCodegenCellwiseRewrite24() { dmlTestCase.testCodegenCellwiseRewrite24(); }
	@Test public void testCodegenCellwiseRewrite25() { dmlTestCase.testCodegenCellwiseRewrite25(); }
	@Test public void testCodegenCellwiseRewrite26() { dmlTestCase.testCodegenCellwiseRewrite26(); }
	@Test public void testCodegenCellwiseRewrite27() { dmlTestCase.testCodegenCellwiseRewrite27(); }
}
