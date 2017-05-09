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


package org.apache.sysml.runtime.matrix.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.cp.CM_COV_Object;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.QuaternaryOperator;
import org.apache.sysml.runtime.matrix.operators.ReorgOperator;
import org.apache.sysml.runtime.matrix.operators.ScalarOperator;
import org.apache.sysml.runtime.matrix.operators.UnaryOperator;
import org.apache.sysml.runtime.util.IndexRange;

@SuppressWarnings("rawtypes")
public class CM_N_COVCell extends MatrixValue implements WritableComparable
{	
	private CM_COV_Object cm=new CM_COV_Object();
	
	public String toString()
	{
		return cm.toString();
	}

	@Override
	public MatrixValue aggregateBinaryOperations(MatrixValue m1Value,
			MatrixValue m2Value, MatrixValue result, AggregateBinaryOperator op, boolean useNativeBLAS)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public MatrixValue aggregateUnaryOperations(AggregateUnaryOperator op,
			MatrixValue result, int brlen, int bclen, MatrixIndexes indexesIn)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public MatrixValue binaryOperations(BinaryOperator op,
			MatrixValue thatValue, MatrixValue result)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public void binaryOperationsInPlace(BinaryOperator op, MatrixValue thatValue)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");		
	}

	@Override
	public void copy(MatrixValue that, boolean sp) {
		throw new RuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public void copy(MatrixValue that) {
		throw new RuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public long getNonZeros() {
		return 1;
	}

	@Override
	public int getNumColumns() {
		return 1;
	}

	@Override
	public int getNumRows() {
		return 1;
	}

	@Override
	public double getValue(int r, int c) {
		throw new RuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public void incrementalAggregate(AggregateOperator aggOp,
			MatrixValue correction, MatrixValue newWithCorrection)
			throws DMLRuntimeException {
		throw new RuntimeException("operation not supported fro WeightedCell");
	}
	
	@Override
	public void incrementalAggregate(AggregateOperator aggOp,
			MatrixValue newWithCorrection)
			throws DMLRuntimeException {
		throw new RuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public boolean isInSparseFormat() {
		return false;
	}
	
	@Override
	public boolean isEmpty(){
		return false;
	}

	@Override
	public MatrixValue reorgOperations(ReorgOperator op, MatrixValue result,
			int startRow, int startColumn, int length)
			throws DMLRuntimeException {
		throw new RuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public void reset() {}

	@Override
	public void reset(int rl, int cl) {}

	@Override
	public void reset(int rl, int cl, boolean sp) {}
	
	@Override
	public void reset(int rl, int cl, boolean sp, long nnzs) {}

	@Override
	public void reset(int rl, int cl, double v) {}

	@Override
	public MatrixValue scalarOperations(ScalarOperator op, MatrixValue result)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public void setValue(int r, int c, double v) {
		throw new RuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public MatrixValue unaryOperations(UnaryOperator op, MatrixValue result)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public void unaryOperationsInPlace(UnaryOperator op)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		cm.w=in.readDouble();
		cm.mean.read(in);
		cm.m2.read(in);
		cm.m3.read(in);
		cm.m4.read(in);
		cm.mean_v.read(in);
		cm.c2.read(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(cm.w);
		cm.mean.write(out);
		cm.m2.write(out);
		cm.m3.write(out);
		cm.m4.write(out);
		cm.mean_v.write(out);
		cm.c2.write(out);
	}

	@Override
	public int compareTo(Object o) 
	{
		if(!(o instanceof CM_N_COVCell))
			return -1;
		
		CM_N_COVCell that=(CM_N_COVCell)o;
		return cm.compareTo(that.cm);
	}
	
	@Override 
	public boolean equals(Object o)
	{
		if(!(o instanceof CM_N_COVCell))
			return false;
		
		CM_N_COVCell that=(CM_N_COVCell)o;
		return (cm==that.cm);
	}
	
	@Override
	public int hashCode() {
		throw new RuntimeException("hashCode() should never be called on instances of this class.");
	}
	
	public CM_COV_Object getCM_N_COVObject()
	{
		return cm;
	}

	public void setCM_N_COVObject(double u, double v, double w)
	{
		cm.w=w;
		cm.mean.set(u,0);
		cm.mean_v.set(v, 0);
		cm.m2.set(0,0);
		cm.m3.set(0,0);
		cm.m4.set(0,0);
		cm.c2.set(0,0);
	}
	public void setCM_N_COVObject(CM_COV_Object that)
	{
		cm.set(that);
	}

	@Override
	public MatrixValue zeroOutOperations(MatrixValue result, IndexRange range, boolean complementary)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public void ternaryOperations(Operator op, MatrixValue that,
			MatrixValue that2, CTableMap resultMap, MatrixBlock resultBlock)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
		
	}

	@Override
	public void ternaryOperations(Operator op, MatrixValue that,
			double scalarThat2, boolean ignoreZeros, CTableMap resultMap, MatrixBlock resultBlock)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public void ternaryOperations(Operator op, double scalarThat,
			double scalarThat2, CTableMap resultMap, MatrixBlock resultBlock)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}
	
	@Override
	public void ternaryOperations(Operator op, MatrixIndexes ix1, double scalarThat, boolean left, int brlen,
			CTableMap resultMap, MatrixBlock resultBlock)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public void ternaryOperations(Operator op, double scalarThat,
			MatrixValue that2, CTableMap resultMap, MatrixBlock resultBlock)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}
	
	@Override
	public MatrixValue quaternaryOperations(QuaternaryOperator qop, MatrixValue um, MatrixValue vm, MatrixValue wm, MatrixValue out)
		throws DMLRuntimeException
	{
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}
		

	@Override
	public void sliceOperations(ArrayList<IndexedMatrixValue> outlist,
			IndexRange range, int rowCut, int colCut, int blockRowFactor,
			int blockColFactor, int boundaryRlen, int boundaryClen)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");		
	}
	
	@Override
	public MatrixValue replaceOperations(MatrixValue result, double pattern, double replacement)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");		
	}

	@Override
	public MatrixValue aggregateUnaryOperations(AggregateUnaryOperator op,
			MatrixValue result, int blockingFactorRow, int blockingFactorCol,
			MatrixIndexes indexesIn, boolean inCP)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public MatrixValue aggregateBinaryOperations(MatrixIndexes m1Index,
			MatrixValue m1Value, MatrixIndexes m2Index, MatrixValue m2Value,
			MatrixValue result, AggregateBinaryOperator op, boolean useNativeBLAS)
			throws DMLRuntimeException {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}

	@Override
	public void appendOperations(MatrixValue valueIn2, ArrayList<IndexedMatrixValue> outlist,
			int blockRowFactor, int blockColFactor, boolean cbind, boolean m2IsLast, int nextNCol)
	throws DMLRuntimeException   {
		throw new DMLRuntimeException("operation not supported fro WeightedCell");
	}
}
