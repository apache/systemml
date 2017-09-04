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

package org.apache.sysml.test.integration.mlcontext.algorithms;

import static org.apache.sysml.api.mlcontext.ScriptFactory.dmlFromFile;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.log4j.Logger;
import org.apache.sysml.api.mlcontext.Script;
import org.apache.sysml.test.integration.mlcontext.MLContextTestBase;
import org.apache.sysml.test.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MLContextGLMTest extends MLContextTestBase {
        protected static Logger log = Logger.getLogger(MLContextGLMTest.class);
        
        protected final static String TEST_SCRIPT_MAIN = "scripts/algorithms/GLM.dml";
	protected final static String TEST_SCRIPT_PRED = "scripts/algotithms/GLM-predict.dml";
	
	private final static double sparsity1 = 0.7; //dense
	private final static double sparsity2 = 0.1; //sparse
	
	public enum GLMType {
		POISSON_LOG,
		GAMMA_LOG,
		BINOMIAL_PROBIT,
	}
        
        @Test 
        public void testGLMPoissonSparse() {
                runGLMTestMLC(GLMType.POISSON_LOG, true, true);
        }
        
        @Test
        public void testGLMPoissonDense() {
                runGLMTestMLC(GLMType.POISSON_LOG, true, false);
        }
        
        @Test 
        public void testGLMGammaSparse() {
                runGLMTestMLC(GLMType.GAMMA_LOG, true, true);
        }
        
        @Test 
        public void testGLMGammaDense() {
                runGLMTestMLC(GLMType.GAMMA_LOG, true, false);
        }
               
        @Test
        public void testGLMBinomialSparse() {
                runGLMTestMLC(GLMType.BINOMIAL_PROBIT, true, true);
        }
        
        @Test
        public void testGLMBinomialDense() {
                runGLMTestMLC(GLMType.BINOMIAL_PROBIT, true, false);
        }
	
	@Test 
        public void testGLMPredPoissonSparse() {
                runGLMTestMLC(GLMType.POISSON_LOG, false, true);
        }
        
        @Test
        public void testGLMPredPoissonDense() {
                runGLMTestMLC(GLMType.POISSON_LOG,  false, false);
        }
        
        @Test 
        public void testGLMPredGammaSparse() {
                runGLMTestMLC(GLMType.GAMMA_LOG, false, true);
        }
        
        @Test 
        public void testGLMPredGammaDense() {
                runGLMTestMLC(GLMType.GAMMA_LOG, false, false);
        }
               
        @Test
        public void testGLMPredBinomialSparse() {
                runGLMTestMLC(GLMType.BINOMIAL_PROBIT, false, true);
        }
        
        @Test
        public void testGLMPredBinomialDense() {
                runGLMTestMLC(GLMType.BINOMIAL_PROBIT, false, false);
        }
        
        private void runGLMTestMLC( GLMType type, boolean main, boolean sparse) {
                String[] addArgs = new String[4];
		String param4Name = "$lpow=";
                                        
                double[][] X = getRandomMatrix(2468, 1007, 0, 1, sparse?sparsity2:sparsity1, -1);
                //<code>cols=1</code> will be enough 
		double[][] B = TestUtils.round(getRandomMatrix(2468, 1, 0, 1, sparse?sparsity2:sparsity1, -1));
                
		switch(type) {
			case POISSON_LOG: //dfam, vpow, link, lpow
				double[][] Y = TestUtils.round(getRandomMatrix(2468, 1, 0, 1, sparse?sparsity2:sparsity1, -1));
				addArgs[0] = "1"; addArgs[1] = "1.0"; addArgs[2] = "1"; addArgs[3] = "0.0";
				break;
				
			case GAMMA_LOG:   //dfam, vpow, link, lpow
				double[][] Y = TestUtils.round(getRandomMatrix(2468, 2, 0, 1, sparse?sparsity2:sparsity1, -1));
				addArgs[0] = "1"; addArgs[1] = "2.0"; addArgs[2] = "1"; addArgs[3] = "0.0";
				break;
				
			case BINOMIAL_PROBIT: //dfam, vpow, link, yneg or lpow
				double[][] Y = TestUtils.round(getRandomMatrix(2468, main?2:3, 0, 1, sparse?sparsity2:sparsity1, -1));
				addArgs[0] = "2"; addArgs[1] = "0.0"; addArgs[2] = "3"; addArgs[3] = "2";
				param4Name = main?"$yneg=":"$lpow=";
				break;
				
		}
                
		if (main) {
                	Script glm = dmlFromFile(TEST_SCRIPT_MAIN);
                        glm.in("X", X).in("Y", Y).in("$dfam", addArgs[0]).in("$vpow", addArgs[1]).in("$link", addArgs[2]).in(param4Name, addArgs[3]).in("$icpt", "0").in("$tol", "0.000000001").in("$moi", "5").out("beta_out");
                        ml.execute(glm);
		} else {
		        Script glmp = dmlFromFile(TEST_SCRIPT_PRED);
                        glmp.in("X", X).in("Y", Y).in("B", B).in("$dfam", addArgs[0]).in("$vpow", addArgs[1]).in("$link", addArgs[2]).in(param4Name, addArgs[3]).in("$icpt", "0").in("$tol", "0.000000001").in("$moi", "5").out("means").out("str");
                        ml.execute(glmp);
		}
		
        }
                       
}
