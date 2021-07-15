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

package org.apache.sysds.test.component.compress;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.sysds.runtime.compress.CompressedMatrixBlock;
import org.apache.sysds.runtime.compress.CompressionSettingsBuilder;
import org.apache.sysds.runtime.compress.colgroup.AColGroup.CompressionType;
import org.apache.sysds.runtime.functionobjects.CM;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.CMOperator;
import org.apache.sysds.runtime.matrix.operators.CMOperator.AggregateOperationTypes;
import org.apache.sysds.test.TestUtils;
import org.apache.sysds.test.component.compress.TestConstants.MatrixTypology;
import org.apache.sysds.test.component.compress.TestConstants.OverLapping;
import org.apache.sysds.test.component.compress.TestConstants.SparsityType;
import org.apache.sysds.test.component.compress.TestConstants.ValueRange;
import org.apache.sysds.test.component.compress.TestConstants.ValueType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class CompressedVectorTest extends CompressedTestBase {

	protected static MatrixTypology[] usedMatrixTypologyLocal = new MatrixTypology[] {// types
		MatrixTypology.SINGLE_COL,
		// MatrixTypology.SINGLE_COL_L
	};

	@Parameters
	public static Collection<Object[]> data() {
		ArrayList<Object[]> tests = new ArrayList<>();
		for(SparsityType st : usedSparsityTypes)
			for(ValueType vt : usedValueTypes)
				for(ValueRange vr : usedValueRanges)
					for(CompressionSettingsBuilder cs : usedCompressionSettings)
						for(MatrixTypology mt : usedMatrixTypologyLocal)
							for(OverLapping ov : overLapping)
								tests.add(new Object[] {st, vt, vr, cs, mt, ov, null});

		return tests;
	}

	public CompressedVectorTest(SparsityType sparType, ValueType valType, ValueRange valRange,
		CompressionSettingsBuilder compSettings, MatrixTypology matrixTypology, OverLapping ov, Collection<CompressionType> ct) {
		super(sparType, valType, valRange, compSettings, matrixTypology, ov, 1, ct);
	}

	@Test
	public void testCentralMoment() throws Exception {
		try {
			if(!(cmb instanceof CompressedMatrixBlock) || cols != 1)
				return; // Input was not compressed then just pass test

			AggregateOperationTypes opType = CMOperator.getCMAggOpType(2);
			CMOperator cm = new CMOperator(CM.getCMFnObject(opType), opType);
			double ret1 = mb.cmOperations(cm).getRequiredResult(opType);
			double ret2 = cmb.cmOperations(cm).getRequiredResult(opType);

			if(_cs.lossy) {
				double tol = lossyTolerance * 10;
				assertTrue(
					this.toString() + ": values uncomprssed: " + ret1 + "vs compressed: " + ret2 + " tolerance " + tol,
					TestUtils.compareCellValue(ret1, ret2, tol, false));
			}
			else {
				assertTrue(this.toString() + "\n expected: " + ret1 + " was:" + ret2,
					TestUtils.compareScalarBits(ret1, ret2, 1024));
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new Exception(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testQuartile() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock) || cols != 1)
				return; // Input was not compressed then just pass test

			double ret1 = mb.sortOperations().pickValue(0.95);
			double ret2 = cmb.sortOperations().pickValue(0.95);

			if(_cs.lossy)
				TestUtils.compareCellValue(ret1, ret2, lossyTolerance, false);
			else
				assertTrue(this.toString(), TestUtils.compareScalarBits(ret1, ret2, 0));

		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testSortOperations() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock) || cols != 1)
				return; // Input was not compressed then just pass test

			MatrixBlock ret1 = mb.sortOperations();
			MatrixBlock ret2 = cmb.sortOperations();

			compareResultMatrices(ret1, ret2, 1);

		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}
}
