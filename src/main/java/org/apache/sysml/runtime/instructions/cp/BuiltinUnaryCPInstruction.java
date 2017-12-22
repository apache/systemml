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

import java.util.Arrays;

import org.apache.sysml.lops.LopsException;
import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.functionobjects.Builtin;
import org.apache.sysml.runtime.functionobjects.ValueFunction;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.matrix.data.LibCommonsMath;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.SimpleOperator;
import org.apache.sysml.runtime.matrix.operators.UnaryOperator;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;

import static org.apache.sysml.runtime.instructions.InstructionUtils.isBuiltinFunction;

public class BuiltinUnaryCPInstruction extends UnaryCPInstruction {

	protected CPOperand _out;

	protected BuiltinUnaryCPInstruction(Operator op, CPOperand in, CPOperand out, String opcode,
										String istr) {
		super(CPType.BuiltinUnary, op, in, out, opcode, istr);
		_out = out;
	}

	public static BuiltinUnaryCPInstruction parseInstruction ( String str )
			throws DMLRuntimeException
	{
		CPOperand in = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		CPOperand out = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);

		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = null;

		ValueFunction func = null;

		//print or stop or cumulative aggregates
		if( parts.length==4 )
		{
			opcode = parts[0];
			in.split(parts[1]);
			out.split(parts[2]);
			func = Builtin.getBuiltinFnObject(opcode);

			if( Arrays.asList(new String[]{"ucumk+","ucum*","ucummin","ucummax"}).contains(opcode) )
				return new MatrixBuiltinCPInstruction(new UnaryOperator(func,Integer.parseInt(parts[3])), in, out, opcode, str);
			else
				return new ScalarBuiltinCPInstruction(new SimpleOperator(func), in, out, opcode, str);
		}
		else //2+1, general case
		{
			opcode = parseUnaryInstruction(str, in, out);
			if(LibCommonsMath.isSupportedUnaryOperation(opcode)) {
				return new BuiltinUnaryCPInstruction(null, in, out, opcode, str);
			}
			else if(isBuiltinFunction(opcode)) {
				func = Builtin.getBuiltinFnObject(opcode);

				if(in.getDataType() == DataType.SCALAR)
					return new ScalarBuiltinCPInstruction(new SimpleOperator(func), in, out, opcode, str);
				else if(in.getDataType() == DataType.MATRIX)
					return new MatrixBuiltinCPInstruction(new UnaryOperator(func), in, out, opcode, str);
			}
			else
				throw new DMLRuntimeException("Invalid opcode in Builtin Unary instruction: " + opcode);

		}

		return null;
	}

	@Override
	public void processInstruction(ExecutionContext ec)
			throws DMLRuntimeException
	{
		String opcode = getOpcode();
		MatrixObject mo = ec.getMatrixObject(input1.getName());
		MatrixBlock out = null;

		if(LibCommonsMath.isSupportedUnaryOperation(opcode))
			out = LibCommonsMath.unaryOperations(mo, opcode);
		else
			throw new DMLRuntimeException("Invalid opcode in Builtin Unary instruction: " + opcode);

		ec.setMatrixOutput(_out.getName(), out);
	}
}
