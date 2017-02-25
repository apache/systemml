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

package org.apache.sysml.runtime.instructions.gpu;

import jcuda.runtime.JCuda;
import org.apache.sysml.lops.runtime.RunMRJobs;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.GPUInstructionParser;
import org.apache.sysml.runtime.instructions.Instruction;
import org.apache.sysml.runtime.matrix.operators.Operator;

public abstract class GPUInstruction extends Instruction 
{
	public enum GPUINSTRUCTION_TYPE { AggregateUnary, AggregateBinary, Convolution, MMTSJ, Reorg, ArithmeticBinary, BuiltinUnary, Builtin };

	// Memory/conversions
	public final static String MISC_TIMER_TO_DEVICE = "2dev";	// time spent in bringing data to gpu (from host)
	public final static String MISC_TIMER_DEVICE_TO = "dev2"; // time spent in bringing data from gpu (to host)
	public final static String MISC_TIMER_DEVICE_TO_DEVICE = "2dev2"; // time spent in copying data from one region on the device to another
	public final static String MISC_TIMER_SPARSE_TO_DENSE = "sp2";	// time spent in converting data from sparse to dense
	public final static String MISC_TIMER_DENSE_TO_SPARSE = "2sp";	// time spent in converting data from dense to sparse
	public final static String MISC_TIMER_CUDA_FREE = "f";	// time spent in calling cudaFree
	public final static String MISC_TIMER_ALLOCATE = "a";		// time spent to allocate memory on gpu

	// Matmult instructions
	public final static String MISC_TIMER_SPARSE_ALLOCATE_LIB = 						"sao";		// time spend in allocating for sparse matrix output
	public final static String MISC_TIMER_DENSE_DOT_LIB = 									"ddot";	// time spent in dot product of 2 dense vectors
	public final static String MISC_TIMER_DENSE_VECTOR_DENSE_MATRIX_LIB = 	"dvdm";	// time spent in matrix mult of dense vector and dense matrix
	public final static String MISC_TIMER_DENSE_MATRIX_DENSE_VECTOR_LIB = 	"dmdv";	// time spent in matrix mult of dense matrix and dense vector
	public final static String MISC_TIMER_DENSE_MATRIX_DENSE_MATRIX_LIB = 	"dmdm";	// time spent in matrix mult of dense matrices
	public final static String MISC_TIMER_SPARSE_MATRIX_DENSE_VECTOR_LIB = 	"smdv";	// time spent in matrix mult of sparse matrix and dense vector
	public final static String MISC_TIMER_SPARSE_MATRIX_SPARSE_MATRIX_LIB = "smsm";  // time spent in matrix mult of sparse matrices
	public final static String MISC_TIMER_SYRK_LIB = 												"syrk"; 	// time spent in symmetric rank-k update

	// Other BLAS instructions
	public final static String MISC_TIMER_DAXPY_LIB = 											"daxpy";	// time spent in daxpy

	// Transpose
	public final static String MISC_TIMER_SPARSE_DGEAM_LIB = "sdgeaml"; // time spent in sparse transpose (and other ops of type a*op(A) + b*op(B))
	public final static String MISC_TIMER_DENSE_DGEAM_LIB = "ddeaml"; 	// time spent in dense transpose (and other ops of type a*op(A) + b*op(B))
	public final static String MISC_TIMER_TRANSPOSE_LIB = "dtl";				// time spent on dense transpose, this includes allocation of output

	// Custom kernels
	public final static String MISC_TIMER_MATRIX_MATRIX_CELLWISE_OP_KERNEL = 	"mmck";	// time spent in matrix-matrix cellwise operations
	public final static String MISC_TIMER_COMPARE_AND_SET_KERNEL = 						"cask";	// time spent in compareAndSet kernel
	public final static String MISC_TIMER_EXP_KERNEL = 												"expk";	// time spent in the exp kernel
	public final static String MISC_TIMER_UPPER_TO_LOWER_TRIANGLE_KERNEL = 		"u2lk"; // time spent in the copy_u2l_dense kernel
	public final static String MISC_TIMER_FILL_KERNEL	=												"fillk"; // time spent in the "fill" kernel
	public final static String MISC_TIMER_MATRIX_SCALAR_OP_KERNEL = 					"msk";	// time spent in the matrix scalar kernel
	public final static String MISC_TIMER_REDUCE_ALL_KERNEL = 								"rallk"; // time spent in reduce all kernel
	public final static String MISC_TIMER_REDUCE_ROW_KERNEL = 								"rrowk"; // time spent in reduce row kernel
	public final static String MISC_TIMER_REDUCE_COL_KERNEL = 								"rcolk";	// time spent in reduce column kernel

	// Deep learning operators
	public final static String MISC_TIMER_ACTIVATION_FORWARD_LIB = 					"nnaf"; // time spent in cudnnActivationForward
	public final static String MISC_TIMER_CONVOLUTION_FORWARD_LIB =					"nncf"; // time spent in cudnnConvolutionForward
	public final static String MISC_TIMER_CONVOLUTION_BACKWARD_FILTER_LIB = "nncbf"; // time spent in cudnnConvolutionBackwardFilter
	public final static String MISC_TIMER_CONVOLUTION_BACKWARD_DATA_LIB = 	"nncbd"; // time spent in cudnnConvolutionBackwardData
	public final static String MISC_TIMER_MAXPOOLING_FORWARD_LIB =					"nnmf"; // time spent in cudnnPoolingForward
	public final static String MISC_TIMER_MAXPOOLING_BACKWARD_LIB = 				"nnmb"; // time spent in cudnnPoolingBackward
	public final static String MISC_TIMER_BIAS_ADD_LIB = 										"nnba"; // time spent in bias_add cuda kernel
	public final static String MISC_TIMER_RELU_BACKWARD_KERNEL= 						"nnrbk"; // time spent in relu_backward cuda kernel
	public final static String MISC_TIMER_RELU_KERNEL = 										"nnrk";	// time spent in the relu kernel



	public final static String MISC_TIMER_CUDNN_INIT = "nni";	// time spent in initializations for cudnn call
	public final static String MISC_TIMER_CUDNN_CLEANUP = "nnc";	// time spent in cleanup for cudnn call

	protected GPUINSTRUCTION_TYPE _gputype;
	protected Operator _optr;
	
	protected boolean _requiresLabelUpdate = false;
	
	public GPUInstruction(String opcode, String istr) {
		type = INSTRUCTION_TYPE.GPU;
		instString = istr;
		
		//prepare opcode and update requirement for repeated usage
		instOpcode = opcode;
		_requiresLabelUpdate = super.requiresLabelUpdate();
	}
	
	public GPUInstruction(Operator op, String opcode, String istr) {
		this(opcode, istr);
		_optr = op;
	}
	
	public GPUINSTRUCTION_TYPE getGPUInstructionType() {
		return _gputype;
	}
	
	@Override
	public boolean requiresLabelUpdate() {
		return _requiresLabelUpdate;
	}

	@Override
	public String getGraphString() {
		return getOpcode();
	}

	@Override
	public Instruction preprocessInstruction(ExecutionContext ec)
		throws DMLRuntimeException 
	{
		//default preprocess behavior (e.g., debug state)
		Instruction tmp = super.preprocessInstruction(ec);
		
		//instruction patching
		if( tmp.requiresLabelUpdate() ) { //update labels only if required
			//note: no exchange of updated instruction as labels might change in the general case
			String updInst = RunMRJobs.updateLabels(tmp.toString(), ec.getVariables());
			tmp = GPUInstructionParser.parseSingleInstruction(updInst);
		}

		return tmp;
	}

	@Override 
	public abstract void processInstruction(ExecutionContext ec)
			throws DMLRuntimeException;

	@Override
	public void postprocessInstruction(ExecutionContext ec)
					throws DMLRuntimeException
	{
		JCuda.cudaDeviceSynchronize();
	}
}
