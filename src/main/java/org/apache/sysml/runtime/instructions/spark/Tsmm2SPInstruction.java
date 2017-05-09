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

package org.apache.sysml.runtime.instructions.spark;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.lops.MMTSJ.MMTSJType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBlock;
import org.apache.sysml.runtime.instructions.spark.functions.IsBlockInRange;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.LibMatrixReorg;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.utils.NativeHelper;

import scala.Tuple2;

public class Tsmm2SPInstruction extends UnarySPInstruction 
{
	private MMTSJType _type = null;
	
	public Tsmm2SPInstruction(Operator op, CPOperand in1, CPOperand out, MMTSJType type, String opcode, String istr )
	{
		super(op, in1, out, opcode, istr);
		_sptype = SPINSTRUCTION_TYPE.TSMM2;		
		_type = type;
	}

	public static Tsmm2SPInstruction parseInstruction( String str ) 
		throws DMLRuntimeException 
	{
		String parts[] = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		
		//check supported opcode 
		if ( !opcode.equalsIgnoreCase("tsmm2") ) {
			throw new DMLRuntimeException("Tsmm2SPInstruction.parseInstruction():: Unknown opcode " + opcode);			
		}
			
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand out = new CPOperand(parts[2]);
		MMTSJType type = MMTSJType.valueOf(parts[3]);
		
		return new Tsmm2SPInstruction(null, in1, out, type, opcode, str);
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException
	{	
		SparkExecutionContext sec = (SparkExecutionContext)ec;
		
		//get input
		JavaPairRDD<MatrixIndexes,MatrixBlock> in = sec.getBinaryBlockRDDHandleForVariable( input1.getName() );
		MatrixCharacteristics mc = sec.getMatrixCharacteristics( input1.getName() );
		
		//execute tsmm2 instruction 
		//step 1: first pass of X, filter-collect-broadcast excess blocks 
		JavaPairRDD<MatrixIndexes,MatrixBlock> tmp1 = 
			in.filter(new IsBlockInRange(_type.isLeft() ? 1 : mc.getRowsPerBlock()+1, mc.getRows(), 
					_type.isLeft() ? mc.getColsPerBlock()+1 : 1, mc.getCols(), mc))
			  .mapToPair(new ShiftTSMMIndexesFunction(_type));		
		PartitionedBlock<MatrixBlock> pmb = SparkExecutionContext.toPartitionedMatrixBlock(tmp1, 
				(int)(_type.isLeft() ? mc.getRows() : mc.getRows() - mc.getRowsPerBlock()), 
				(int)(_type.isLeft() ? mc.getCols()-mc.getColsPerBlock() : mc.getCols()), 
				mc.getRowsPerBlock(), mc.getColsPerBlock(), -1L);
		Broadcast<PartitionedBlock<MatrixBlock>> bpmb = sec.getSparkContext().broadcast(pmb);
		
		//step 2: second pass of X, compute tsmm/mapmm and aggregate result blocks
		int outputDim = (int) (_type.isLeft() ? mc.getCols() : mc.getRows());
		if( OptimizerUtils.estimateSize(outputDim, outputDim) <= 32*1024*1024 ) { //default: <=32MB
			//output large blocks and reduceAll to avoid skew on combineByKey
			JavaRDD<MatrixBlock> tmp2 = in.map(
					new RDDTSMM2ExtFunction(bpmb, _type, outputDim, (int)mc.getRowsPerBlock(), NativeHelper.isNativeLibraryLoaded()));
			MatrixBlock out = RDDAggregateUtils.sumStable(tmp2);
		      
			//put output block into symbol table (no lineage because single block)
			//this also includes implicit maintenance of matrix characteristics
			sec.setMatrixOutput(output.getName(), out);
		}
		else {
			//output individual output blocks and aggregate by key (no action)
			JavaPairRDD<MatrixIndexes,MatrixBlock> tmp2 = in.flatMapToPair(new RDDTSMM2Function(bpmb, _type, NativeHelper.isNativeLibraryLoaded()));
			JavaPairRDD<MatrixIndexes,MatrixBlock> out = RDDAggregateUtils.sumByKeyStable(tmp2, false);
			
			//put output RDD handle into symbol table
			sec.getMatrixCharacteristics(output.getName()).set(outputDim, outputDim, mc.getRowsPerBlock(), mc.getColsPerBlock());
			sec.setRDDHandleForVariable(output.getName(), out);	
			sec.addLineageRDD(output.getName(), input1.getName());	
		}
	}

	private static class RDDTSMM2Function implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 2935770425858019666L;
		
		private Broadcast<PartitionedBlock<MatrixBlock>> _pb = null;
		private MMTSJType _type = null;
		private AggregateBinaryOperator _op = null;
		private boolean _useNativeBLAS;
		
		public RDDTSMM2Function( Broadcast<PartitionedBlock<MatrixBlock>> pb, MMTSJType type, boolean useNativeBLAS ) {
			_pb = pb;
			_type = type;
			_useNativeBLAS = useNativeBLAS;
			
			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}

		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
			throws Exception 
		{
			List<Tuple2<MatrixIndexes,MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes,MatrixBlock>>();
			MatrixIndexes ixin = arg0._1();
			MatrixBlock mbin = arg0._2();
			
			//execute block tsmm operation
			MatrixBlock out1 = mbin.transposeSelfMatrixMultOperations(new MatrixBlock(), _type);
			long ixout = _type.isLeft() ? ixin.getColumnIndex() : ixin.getRowIndex();
			ret.add(new Tuple2<MatrixIndexes, MatrixBlock>(new MatrixIndexes(ixout, ixout), out1));
			
			if( _type.isLeft() ? ixin.getColumnIndex() == 1 : ixin.getRowIndex() == 1 ) {
				//execute block mapmm operation for full block only (output two blocks, due to symmetry)
				MatrixBlock mbin2 = _pb.getValue().getBlock( //lookup broadcast block
						(int)(_type.isLeft()?ixin.getRowIndex():1), 
						(int)(_type.isLeft()?1:ixin.getColumnIndex()));
				MatrixBlock mbin2t = transpose(mbin2, new MatrixBlock()); //prep for transpose rewrite mm
				
				MatrixBlock out2 = (MatrixBlock) OperationsOnMatrixValues.performAggregateBinaryIgnoreIndexes( //mm
						_type.isLeft() ? mbin2t : mbin, _type.isLeft() ? mbin : mbin2t, new MatrixBlock(), _op, _useNativeBLAS);
				MatrixIndexes ixout2 = _type.isLeft() ? new MatrixIndexes(2,1) : new MatrixIndexes(1,2);
				ret.add(new Tuple2<MatrixIndexes,MatrixBlock>(ixout2, out2));
				
				MatrixBlock out3 = transpose(out2, new MatrixBlock()); 
				MatrixIndexes ixout3 = _type.isLeft() ? new MatrixIndexes(1,2) : new MatrixIndexes(2,1);
				ret.add(new Tuple2<MatrixIndexes,MatrixBlock>(ixout3, out3));
			}
			
			return ret.iterator();
		}
	}

	/**
	 * Same semantics as RDDTSMM2Function but output single consolidated block.
	 * 
	 */
	private static class RDDTSMM2ExtFunction implements Function<Tuple2<MatrixIndexes, MatrixBlock>, MatrixBlock> 
	{
		private static final long serialVersionUID = 3284059592407517911L;
		
		private Broadcast<PartitionedBlock<MatrixBlock>> _pb = null;
		private MMTSJType _type = null;
		private AggregateBinaryOperator _op = null;
		private int _outputDim = -1;
		private int _blen = -1; 
		private boolean _useNativeBLAS;
		
		public RDDTSMM2ExtFunction( Broadcast<PartitionedBlock<MatrixBlock>> pb, MMTSJType type, int outputDim, int blen, boolean useNativeBLAS ) {
			_pb = pb;
			_type = type;
			_outputDim = outputDim;
			_blen = blen;
			_useNativeBLAS = useNativeBLAS;
			
			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}

		@Override
		public MatrixBlock call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
			throws Exception 
		{
			MatrixIndexes ixin = arg0._1();
			MatrixBlock mbin = arg0._2();
			
			boolean fullBlock = _type.isLeft() ? ixin.getColumnIndex() == 1 : ixin.getRowIndex() == 1;
			MatrixBlock out = new MatrixBlock(_outputDim, _outputDim, !fullBlock);
			out.allocateDenseOrSparseBlock();
			
			//execute block tsmm operation
			MatrixBlock out1 = mbin.transposeSelfMatrixMultOperations(new MatrixBlock(), _type);
			int ix = (int) ((_type.isLeft() ? ixin.getColumnIndex() : ixin.getRowIndex())-1) * _blen;
			out.copy(ix, ix+out1.getNumRows()-1, ix, ix+out1.getNumColumns()-1, out1, true);
			
			if( fullBlock ) {
				//execute block mapmm operation for full block only (output two blocks, due to symmetry)
				MatrixBlock mbin2 = _pb.getValue().getBlock( //lookup broadcast block
						(int)(_type.isLeft()?ixin.getRowIndex():1), 
						(int)(_type.isLeft()?1:ixin.getColumnIndex()));
				MatrixBlock mbin2t = transpose(mbin2, new MatrixBlock()); //prep for transpose rewrite mm
				
				MatrixBlock out2 = (MatrixBlock) OperationsOnMatrixValues.performAggregateBinaryIgnoreIndexes( //mm
						_type.isLeft() ? mbin2t : mbin, _type.isLeft() ? mbin : mbin2t, new MatrixBlock(), _op, _useNativeBLAS);
				
				MatrixIndexes ixout2 = _type.isLeft() ? new MatrixIndexes(2,1) : new MatrixIndexes(1,2);
				out.copy((int)(ixout2.getRowIndex()-1)*_blen, (int)(ixout2.getRowIndex()-1)*_blen+out2.getNumRows()-1, 
						(int)(ixout2.getColumnIndex()-1)*_blen, (int)(ixout2.getColumnIndex()-1)*_blen+out2.getNumColumns()-1, out2, true);
				MatrixBlock out3 = transpose(out2, new MatrixBlock()); 
				out.copy((int)(ixout2.getColumnIndex()-1)*_blen, (int)(ixout2.getColumnIndex()-1)*_blen+out3.getNumRows()-1, 
						(int)(ixout2.getRowIndex()-1)*_blen, (int)(ixout2.getRowIndex()-1)*_blen+out3.getNumColumns()-1, out3, true);
			}
			
			return out;
		}
	}

	private static class ShiftTSMMIndexesFunction implements PairFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = -3858454295795680100L;
		
		private MMTSJType _type = null;
		
		public ShiftTSMMIndexesFunction( MMTSJType type ) {
			_type = type;
		}

		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
			throws Exception 
		{
			if( _type.isLeft() )
				return new Tuple2<MatrixIndexes,MatrixBlock>(new MatrixIndexes(arg0._1().getRowIndex(), 1), arg0._2());
			else
				return new Tuple2<MatrixIndexes,MatrixBlock>(new MatrixIndexes(1, arg0._1().getColumnIndex()), arg0._2());	
		}
	}	
	
	/**
	 * Helper function to setup output dimensions.
	 * 
	 * @param in input matrix block
	 * @param out output matrix block
	 * @return matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	private static MatrixBlock transpose(MatrixBlock in, MatrixBlock out) throws DMLRuntimeException {
		if( out == null )
			out = new MatrixBlock(in.getNumColumns(), in.getNumRows(), in.getNonZeros());
		else
			out.reset(in.getNumColumns(), in.getNumRows(), in.getNonZeros());
		
		return LibMatrixReorg.transpose(in, out);
	}
}
