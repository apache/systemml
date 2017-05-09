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

package org.apache.sysml.runtime.instructions.cp;

import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.compress.CompressedMatrixBlock;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.utils.NativeHelper;

public class AggregateBinaryCPInstruction extends BinaryCPInstruction
{
	
	public AggregateBinaryCPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr ){
		super(op, in1, in2, out, opcode, istr);
		_cptype = CPINSTRUCTION_TYPE.AggregateBinary;
	}

	public static AggregateBinaryCPInstruction parseInstruction( String str ) 
		throws DMLRuntimeException 
	{
		CPOperand in1 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		CPOperand in2 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		CPOperand out = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);

		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];

		if ( !opcode.equalsIgnoreCase("ba+*")) {
			throw new DMLRuntimeException("AggregateBinaryInstruction.parseInstruction():: Unknown opcode " + opcode);
		}
		
		InstructionUtils.checkNumFields( parts, 4 );
		in1.split(parts[1]);
		in2.split(parts[2]);
		out.split(parts[3]);
		int k = Integer.parseInt(parts[4]);
		
		AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg, k);
		return new AggregateBinaryCPInstruction(aggbin, in1, in2, out, opcode, str);	
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException
	{	
		//get inputs
		MatrixBlock matBlock1 = ec.getMatrixInput(input1.getName());
    MatrixBlock matBlock2 = ec.getMatrixInput(input2.getName());
    
    //compute matrix multiplication
    AggregateBinaryOperator ab_op = (AggregateBinaryOperator) _optr;
		MatrixBlock soresBlock = null;
		if( matBlock2 instanceof CompressedMatrixBlock )
			soresBlock = (MatrixBlock) (matBlock2.aggregateBinaryOperations(matBlock1, matBlock2, new MatrixBlock(), ab_op, NativeHelper.isNativeLibraryLoaded()));
		else  {
			soresBlock = (MatrixBlock) (matBlock1.aggregateBinaryOperations(matBlock1, matBlock2, new MatrixBlock(), ab_op, NativeHelper.isNativeLibraryLoaded()));
		}
			
		//release inputs/outputs
		ec.releaseMatrixInput(input1.getName());
		ec.releaseMatrixInput(input2.getName());
		ec.setMatrixOutput(output.getName(), soresBlock);
	}
}
