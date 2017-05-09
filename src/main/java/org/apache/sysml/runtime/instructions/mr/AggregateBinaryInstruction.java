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

package org.apache.sysml.runtime.instructions.mr;

import java.util.ArrayList;

import org.apache.sysml.lops.MMCJ.MMCJType;
import org.apache.sysml.lops.MapMult;
import org.apache.sysml.lops.MapMult.CacheType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.MatrixValue;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.mapred.CachedValueMap;
import org.apache.sysml.runtime.matrix.mapred.DistributedCacheInput;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.mapred.MRBaseForCommonInstructions;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;


public class AggregateBinaryInstruction extends BinaryMRInstructionBase implements IDistributedCacheConsumer
{	
	private String _opcode = null;
	
	boolean useNativeBLAS = false; // Since MR is in maintenance mode
	
	//optional argument for cpmm
	private MMCJType _aggType = MMCJType.AGG;
	
	//optional argument for mapmm
	private CacheType _cacheType = null;
	private boolean _outputEmptyBlocks = true;
	
	public AggregateBinaryInstruction(Operator op, String opcode, byte in1, byte in2, byte out, String istr)
	{
		super(op, in1, in2, out);
		mrtype = MRINSTRUCTION_TYPE.AggregateBinary;
		instString = istr;
		
		_opcode = opcode;
	}

	public void setCacheTypeMapMult( CacheType type )
	{
		_cacheType = type;
	}

	public void setOutputEmptyBlocksMapMult( boolean flag )
	{
		_outputEmptyBlocks = flag;
	}
	
	public boolean getOutputEmptyBlocks()
	{
		return _outputEmptyBlocks;
	}
	
	public void setMMCJType( MMCJType type )
	{
		_aggType = type;
	}
	
	public MMCJType getMMCJType()
	{
		return _aggType;
	}

	public static AggregateBinaryInstruction parseInstruction ( String str ) 
		throws DMLRuntimeException 
	{
		String[] parts = InstructionUtils.getInstructionParts ( str );
		
		byte in1, in2, out;
		String opcode = parts[0];
		in1 = Byte.parseByte(parts[1]);
		in2 = Byte.parseByte(parts[2]);
		out = Byte.parseByte(parts[3]);
		
		if ( opcode.equalsIgnoreCase("cpmm") 
				|| opcode.equalsIgnoreCase("rmm") 
				|| opcode.equalsIgnoreCase(MapMult.OPCODE) ) 
		{
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
			AggregateBinaryInstruction inst = new AggregateBinaryInstruction(aggbin, opcode, in1, in2, out, str);
			if( parts.length==5 ){
				inst.setMMCJType(MMCJType.valueOf(parts[4]));
			}
			else if( parts.length==6 ) { //mapmm
				inst.setCacheTypeMapMult( CacheType.valueOf(parts[4]) );
				inst.setOutputEmptyBlocksMapMult( Boolean.parseBoolean(parts[5]) );
			}
			return inst;
		} 
		
		throw new DMLRuntimeException("AggregateBinaryInstruction.parseInstruction():: Unknown opcode " + opcode);
	}
	
	@Override //IDistributedCacheConsumer
	public boolean isDistCacheOnlyIndex( String inst, byte index )
	{
		return _cacheType.isRight() ? 
				(index==input2 && index!=input1) : 
				(index==input1 && index!=input2);
	}

	@Override //IDistributedCacheConsumer
	public void addDistCacheIndex( String inst, ArrayList<Byte> indexes )
	{
		indexes.add( _cacheType.isRight() ? input2 : input1 );
	}
	
	@Override
	public void processInstruction(Class<? extends MatrixValue> valueClass,
			CachedValueMap cachedValues, IndexedMatrixValue tempValue,
			IndexedMatrixValue zeroInput, int blockRowFactor, int blockColFactor)
			throws DMLRuntimeException 
	{	
		IndexedMatrixValue in1=cachedValues.getFirst(input1);
		IndexedMatrixValue in2=cachedValues.getFirst(input2);
		
		if ( _opcode.equals(MapMult.OPCODE) ) 
		{
			//check empty inputs (data for different instructions)
			if( _cacheType.isRight() ? in1==null : in2==null )
				return;
			
			// one of the input is from distributed cache.
			processMapMultInstruction(valueClass, cachedValues, in1, in2, blockRowFactor, blockColFactor);
		}
		else //generic matrix mult
		{
			//check empty inputs (data for different instructions)
			if(in1==null || in2==null)
				return;
			
			//allocate space for the output value
			IndexedMatrixValue out;
			if(output==input1 || output==input2)
				out=tempValue;
			else
				out=cachedValues.holdPlace(output, valueClass);

			//process instruction
			OperationsOnMatrixValues.performAggregateBinary(
					    in1.getIndexes(), in1.getValue(), 
						in2.getIndexes(), in2.getValue(), 
						out.getIndexes(), out.getValue(), 
						((AggregateBinaryOperator)optr), useNativeBLAS);
			
			//put the output value in the cache
			if(out==tempValue)
				cachedValues.add(output, out);				
		}
	}
	
	/**
	 * Helper function to perform map-side matrix-matrix multiplication.
	 * 
	 * @param valueClass matrix value class
	 * @param cachedValues cached value map
	 * @param in1 indexed matrix value 1
	 * @param in2 indexed matrix value 2
	 * @param blockRowFactor ?
	 * @param blockColFactor ?
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	private void processMapMultInstruction(Class<? extends MatrixValue> valueClass, CachedValueMap cachedValues, IndexedMatrixValue in1, IndexedMatrixValue in2, int blockRowFactor, int blockColFactor) 
		throws DMLRuntimeException 
	{
		boolean removeOutput = true;
		
		if( _cacheType.isRight() )
		{
			DistributedCacheInput dcInput = MRBaseForCommonInstructions.dcValues.get(input2);
			
			long in2_cols = dcInput.getNumCols();
			long  in2_colBlocks = (long)Math.ceil(((double)in2_cols)/dcInput.getNumColsPerBlock());
			
			for(int bidx=1; bidx <= in2_colBlocks; bidx++) 
			{	
				// Matrix multiply A[i,k] %*% B[k,bid]
				
				// Setup input2 block
				IndexedMatrixValue in2Block = dcInput.getDataBlock((int)in1.getIndexes().getColumnIndex(), bidx);
							
				MatrixValue in2BlockValue = in2Block.getValue(); 
				MatrixIndexes in2BlockIndex = in2Block.getIndexes();
				
				//allocate space for the output value
				IndexedMatrixValue out = cachedValues.holdPlace(output, valueClass);
				
				//process instruction
				OperationsOnMatrixValues.performAggregateBinary(in1.getIndexes(), in1.getValue(), 
							in2BlockIndex, in2BlockValue, out.getIndexes(), out.getValue(), 
							((AggregateBinaryOperator)optr), useNativeBLAS);	
				
				removeOutput &= ( !_outputEmptyBlocks && out.getValue().isEmpty() );
			}
		}
		else
		{
			DistributedCacheInput dcInput = MRBaseForCommonInstructions.dcValues.get(input1);
			
			long in1_rows = dcInput.getNumRows();
			long  in1_rowsBlocks = (long) Math.ceil(((double)in1_rows)/dcInput.getNumRowsPerBlock());
			
			for(int bidx=1; bidx <= in1_rowsBlocks; bidx++) {
				
				// Matrix multiply A[i,k] %*% B[k,bid]
				
				// Setup input2 block
				IndexedMatrixValue in1Block = dcInput.getDataBlock(bidx, (int)in2.getIndexes().getRowIndex());
							
				MatrixValue in1BlockValue = in1Block.getValue(); 
				MatrixIndexes in1BlockIndex = in1Block.getIndexes();
				
				//allocate space for the output value
				IndexedMatrixValue out = cachedValues.holdPlace(output, valueClass);
				
				//process instruction
				OperationsOnMatrixValues.performAggregateBinary(in1BlockIndex, in1BlockValue, 
						in2.getIndexes(), in2.getValue(),
						out.getIndexes(), out.getValue(), 
							((AggregateBinaryOperator)optr), useNativeBLAS);
			
				removeOutput &= ( !_outputEmptyBlocks && out.getValue().isEmpty() );
			}
		}		
		
		//empty block output filter (enabled by compiler consumer operation is in CP)
		if( removeOutput )
			cachedValues.remove(output);
	}
}
