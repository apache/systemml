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

package org.apache.sysml.test.integration.functions.estim;

import org.junit.Test;
import org.apache.commons.lang.NotImplementedException;
import org.apache.sysml.hops.estim.EstimatorBasicAvg;
import org.apache.sysml.hops.estim.EstimatorBasicWorst;
import org.apache.sysml.hops.estim.EstimatorMatrixHistogram;
import org.apache.sysml.hops.estim.SparsityEstimator;
import org.apache.sysml.hops.estim.SparsityEstimator.OpCode;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.test.integration.AutomatedTestBase;
import org.apache.sysml.test.utils.TestUtils;

/**
 * this is the basic operation check for all estimators with single operations
 */
public class OpBindTest extends AutomatedTestBase 
{
	private final static int m = 600;
	private final static int k = 300;
	private final static int n = 100;
	private final static double[] sparsity = new double[]{0.2, 0.4};
//	private final static OpCode mult = OpCode.MULT;
//	private final static OpCode plus = OpCode.PLUS;
	private final static OpCode rbind = OpCode.RBIND;
	private final static OpCode cbind = OpCode.CBIND;
//	private final static OpCode eqzero = OpCode.EQZERO;
//	private final static OpCode diag = OpCode.DIAG;
//	private final static OpCode neqzero = OpCode.NEQZERO;
//	private final static OpCode trans = OpCode.TRANS;
//	private final static OpCode reshape = OpCode.RESHAPE;

	@Override
	public void setUp() {
		//do  nothing
	}
	
	//Average Case
	@Test
	public void testAvgRbind() {
		runSparsityEstimateTest(new EstimatorBasicAvg(), m, k, n, sparsity, rbind);
	}
	
	@Test
	public void testAvgCbind() {
		runSparsityEstimateTest(new EstimatorBasicAvg(), m, k, n, sparsity, cbind);
	}
	
	//Worst Case
	@Test
	public void testWorstRbind() {
		runSparsityEstimateTest(new EstimatorBasicWorst(), m, k, n, sparsity, rbind);
	}
	
	@Test
	public void testWorstCbind() {
		runSparsityEstimateTest(new EstimatorBasicWorst(), m, k, n, sparsity, cbind);
	}
	
	//DensityMap
	/*@Test
	public void testDMCaserbind() {
		runSparsityEstimateTest(new EstimatorDensityMap(), m, k, n, sparsity, rbind);
	}
	
	@Test
	public void testDMCasecbind() {
		runSparsityEstimateTest(new EstimatorDensityMap(), m, k, n, sparsity, cbind);
	}*/
	
	//MNC
	@Test
	public void testMNCRbind() {
		runSparsityEstimateTest(new EstimatorMatrixHistogram(), m, k, n, sparsity, rbind);
	}
		
	@Test
	public void testMNCCbind() {
		runSparsityEstimateTest(new EstimatorMatrixHistogram(), m, k, n, sparsity, cbind);
	}

	//Bitset
	/*@Test
	public void testBitsetCaserbind() {
		runSparsityEstimateTest(new EstimatorBitsetMM(), m, k, n, sparsity, rbind);
	}
		
	@Test
	public void testBitsetCasecbind() {
		runSparsityEstimateTest(new EstimatorBitsetMM(), m, k, n, sparsity, cbind);
	}
		
	//Layered Graph
	@Test
	public void testLGCaserbind() {
		runSparsityEstimateTest(new EstimatorLayeredGraph(), m, k, n, sparsity, rbind);
	}
			
	@Test
	public void testLGCasecbind() {
		runSparsityEstimateTest(new EstimatorLayeredGraph(), m, k, n, sparsity, cbind);
	}
		
	//Sample
	@Test
	public void testSampleCaserbind() {
		runSparsityEstimateTest(new EstimatorSample(), m, k, n, sparsity, rbind);
	}
			
	@Test
	public void testSampleCasecbind() {
		runSparsityEstimateTest(new EstimatorSample(), m, k, n, sparsity, cbind);
	}*/
	
	
	private void runSparsityEstimateTest(SparsityEstimator estim, int m, int k, int n, double[] sp, OpCode op) {
		MatrixBlock m1;
		MatrixBlock m2;
		MatrixBlock m3 = new MatrixBlock();
		double est = 0;
		switch(op) {
			case RBIND:
				m1 = MatrixBlock.randOperations(m, k, sp[0], 1, 1, "uniform", 3);
				m2 = MatrixBlock.randOperations(n, k, sp[1], 1, 1, "uniform", 3);
				m1.append(m2, m3, false);
				est = estim.estim(m1, m2, op);
				break;
			case CBIND:
				m1 = MatrixBlock.randOperations(m, k, sp[0], 1, 1, "uniform", 3);
				m2 = MatrixBlock.randOperations(m, n, sp[1], 1, 1, "uniform", 3);
				m1.append(m2, m3);
				est = estim.estim(m1, m2, op);
				break;
			default:
				throw new NotImplementedException();
		}
		//compare estimated and real sparsity
		TestUtils.compareScalars(est, m3.getSparsity(), (estim instanceof EstimatorBasicWorst) ? 5e-1 : 1e-2);
	}
}
