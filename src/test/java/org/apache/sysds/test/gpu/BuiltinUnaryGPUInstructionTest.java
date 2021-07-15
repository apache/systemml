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

package org.apache.sysds.test.gpu;

import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.functions.builtin.BuiltinSigmoidTest;
import org.apache.sysds.test.functions.unary.matrix.ACosTest;
import org.apache.sysds.test.functions.unary.matrix.ASinTest;
import org.apache.sysds.test.functions.unary.matrix.ATanTest;
import org.apache.sysds.test.functions.unary.matrix.AbsTest;
import org.apache.sysds.test.functions.unary.matrix.CosTest;
import org.apache.sysds.test.functions.unary.matrix.FullCummaxTest;
import org.apache.sysds.test.functions.unary.matrix.FullCumminTest;
import org.apache.sysds.test.functions.unary.matrix.FullCumprodTest;
import org.apache.sysds.test.functions.unary.matrix.FullCumsumTest;
import org.apache.sysds.test.functions.unary.matrix.FullCumsumprodTest;
import org.apache.sysds.test.functions.unary.matrix.FullSignTest;
import org.apache.sysds.test.functions.unary.matrix.RoundTest;
import org.apache.sysds.test.functions.unary.matrix.SinTest;
import org.apache.sysds.test.functions.unary.matrix.SqrtTest;
import org.apache.sysds.test.functions.unary.matrix.TanTest;
import org.junit.Assert;
import org.junit.Test;

public class BuiltinUnaryGPUInstructionTest extends AutomatedTestBase {
	@Override public void setUp() {
		TEST_GPU = true;
		VERBOSE_STATS = true;
	}

	// ToDo:
	//	@Test public void ExponentTest() {}
	//	@Test public void LogarithmTest() {}
	//  @Test public void SoftmaxTest() {}
	//  @Test public void CoshTest() {}
	//  @Test public void SinhTest() {}
	//  @Test public void TanhTest() {}

	@Test public void AbsTest() {
		AbsTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.AbsTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testPositive();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_abs"));
		dmlTestCase.testNegative();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_abs"));
		dmlTestCase.testRandom();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_abs"));
	}

	@Test public void ACosTest() {
		ACosTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.ACosTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testPositive();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_acos"));
		dmlTestCase.testNegative();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_acos"));
		dmlTestCase.testRandom();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_acos"));
	}

	@Test public void ASinTest() {
		ASinTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.ASinTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testPositive();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_asin"));
		dmlTestCase.testNegative();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_asin"));
		dmlTestCase.testRandom();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_asin"));
	}

	@Test public void ATanTest() {
		ATanTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.ATanTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testPositive();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_atan"));
		dmlTestCase.testNegative();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_atan"));
		dmlTestCase.testRandom();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_atan"));
	}

	@Test public void CeilTest() {
		RoundTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.RoundTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testCeil1();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ceil"));
		dmlTestCase.testCeil2();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ceil"));
		dmlTestCase.testCeil3();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ceil"));
		dmlTestCase.testCeil4();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ceil"));
		dmlTestCase.testCeil5();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ceil"));
		dmlTestCase.testCeil6();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ceil"));
	}

	@Test public void CosTest() {
		CosTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.CosTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testPositive();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_cos"));
		dmlTestCase.testNegative();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_cos"));
		dmlTestCase.testRandom();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_cos"));
	}

	@Test public void CummaxTest() {
		FullCummaxTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.FullCummaxTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testCummaxColVectorDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucummax"));
		dmlTestCase.testCummaxColVectorSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucummax"));
		dmlTestCase.testCummaxMatrixDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucummax"));
		dmlTestCase.testCummaxMatrixSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucummax"));
		dmlTestCase.testCummaxRowVectorDenseCP();
		Assert.assertFalse(heavyHittersContainsSubString("gpu_ucummax"));
		dmlTestCase.testCummaxRowVectorDenseNoRewritesCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucummax"));
		dmlTestCase.testCummaxRowVectorSparseCP();
		Assert.assertFalse(heavyHittersContainsSubString("gpu_ucummax"));
		dmlTestCase.testCummaxRowVectorSparseNoRewritesCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucummax"));
	}

	@Test public void CumminTest() {
		FullCumminTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.FullCumminTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testCumminColVectorDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucummin"));
		dmlTestCase.testCumminColVectorSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucummin"));
		dmlTestCase.testCumminMatrixDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucummin"));
		dmlTestCase.testCumminMatrixSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucummin"));
		dmlTestCase.testCumminRowVectorDenseCP();
		Assert.assertFalse(heavyHittersContainsSubString("gpu_ucummin"));
		dmlTestCase.testCumminRowVectorDenseNoRewritesCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucummin"));
		dmlTestCase.testCumminRowVectorSparseCP();
		Assert.assertFalse(heavyHittersContainsSubString("gpu_ucummin"));
		dmlTestCase.testCumminRowVectorSparseNoRewritesCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucummin"));
	}

	@Test public void CumprodTest() {
		FullCumprodTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.FullCumprodTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testCumprodColVectorDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucum*"));
		dmlTestCase.testCumprodColVectorSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucum*"));
		dmlTestCase.testCumprodMatrixDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucum*"));
		dmlTestCase.testCumprodMatrixSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucum*"));
		dmlTestCase.testCumprodRowVectorDenseCP();
		Assert.assertFalse(heavyHittersContainsSubString("gpu_ucum*"));
		dmlTestCase.testCumprodRowVectorDenseNoRewritesCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucum*"));
		dmlTestCase.testCumprodRowVectorSparseCP();
		Assert.assertFalse(heavyHittersContainsSubString("gpu_ucum*"));
		dmlTestCase.testCumprodRowVectorSparseNoRewritesCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucum*"));
	}

	@Test public void CumsumTest() {
		FullCumsumTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.FullCumsumTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testCumsumColVectorDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucumk+"));
		dmlTestCase.testCumsumColVectorSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucumk+"));
		dmlTestCase.testCumsumMatrixDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucumk+"));
		dmlTestCase.testCumsumMatrixSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucumk+"));
		dmlTestCase.testCumsumRowVectorDenseCP();
		Assert.assertFalse(heavyHittersContainsSubString("gpu_ucumk+"));
		dmlTestCase.testCumsumRowVectorDenseNoRewritesCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucumk+"));
		dmlTestCase.testCumsumRowVectorSparseCP();
		Assert.assertFalse(heavyHittersContainsSubString("gpu_ucumk+"));
		dmlTestCase.testCumsumRowVectorSparseNoRewritesCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucumk+"));
	}

	@Test public void CumsumprodTest() {
		FullCumsumprodTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.FullCumsumprodTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testCumsumprodBackwardDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucumk+*"));
		dmlTestCase.testCumsumprodBackwardSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucumk+*"));
		dmlTestCase.testCumsumprodForwardDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucumk+*"));
		dmlTestCase.testCumsumprodForwardSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_ucumk+*"));
	}

	@Test public void FloorTest() {
		RoundTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.RoundTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testFloor1();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_floor"));
		dmlTestCase.testFloor2();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_floor"));
		dmlTestCase.testFloor3();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_floor"));
		dmlTestCase.testFloor4();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_floor"));
		dmlTestCase.testFloor5();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_floor"));
		dmlTestCase.testFloor6();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_floor"));
	}

	@Test public void RoundTest() {
		RoundTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.RoundTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testRound1();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_round"));
		dmlTestCase.testRound2();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_round"));
		dmlTestCase.testRound3();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_round"));
		dmlTestCase.testRound4();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_round"));
		dmlTestCase.testRound5();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_round"));
		dmlTestCase.testRound6();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_round"));
	}

	@Test public void SinTest() {
		SinTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.SinTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testPositive();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sin"));
		dmlTestCase.testNegative();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sin"));
		dmlTestCase.testRandom();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sin"));
	}

	@Test public void SqrtTest() {
		SqrtTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.SqrtTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testPositive();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sqrt"));
		dmlTestCase.testNegativeMatrix();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sqrt"));
		dmlTestCase.testNegativeVector();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sqrt"));
	}

	@Test public void SignTest() {
		FullSignTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.FullSignTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testRewriteSignDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sign"));
		dmlTestCase.testRewriteSignSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sign"));
		dmlTestCase.testSignDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sign"));
		dmlTestCase.testSignSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sign"));
	}

	@Test public void SigmoidTest() {
		BuiltinSigmoidTest dmlTestCase = new org.apache.sysds.test.functions.builtin.BuiltinSigmoidTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testSigmoidMatrixDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sigmoid"));
		dmlTestCase.testSigmoidMatrixSparseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sigmoid"));
		dmlTestCase.testSigmoidScalarDenseCP();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_sigmoid"));
	}

	@Test public void TanTest() {
		TanTest dmlTestCase = new org.apache.sysds.test.functions.unary.matrix.TanTest();
		dmlTestCase.setUpBase();
		dmlTestCase.setUp();
		dmlTestCase.testPositive();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_tan"));
		dmlTestCase.testNegative();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_tan"));
		dmlTestCase.testRandom();
		Assert.assertTrue(heavyHittersContainsSubString("gpu_tan"));
	}
}
