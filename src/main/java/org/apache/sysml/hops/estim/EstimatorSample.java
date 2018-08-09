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

package org.apache.sysml.hops.estim;

import org.apache.commons.lang.NotImplementedException;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.DenseBlock;
import org.apache.sysml.runtime.matrix.data.LibMatrixAgg;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.SparseBlock;
import org.apache.sysml.runtime.util.UtilFunctions;

/**
 * This estimator implements an approach based on row/column sampling
 * Yongyang Yu, MingJie Tang, Walid G. Aref, Qutaibah M. Malluhi, Mostafa M. Abbas, Mourad Ouzzani:
 * In-Memory Distributed Matrix Computation Processing and Optimization. ICDE 2017: 1047-1058
 * 
 * The basic idea is to draw random samples of aligned columns SA and rows SB,
 * and compute the output nnz as max(nnz(SA_i)*nnz(SB_i)). However, this estimator is
 * biased toward underestimation as the maximum is unlikely sampled and collisions are
 * not accounted for.
 */
public class EstimatorSample extends SparsityEstimator
{
	private static final double SAMPLE_FRACTION = 0.1; //10%
	
	private final double _frac;
	
	public EstimatorSample() {
		this(SAMPLE_FRACTION);
	}
	
	public EstimatorSample(double sampleFrac) {
		if( sampleFrac < 0 || sampleFrac > 1.0 )
			throw new DMLRuntimeException("Invalid sample fraction: "+sampleFrac);
		_frac = sampleFrac;
	}
	
	@Override
	public MatrixCharacteristics estim(MMNode root) {
		LOG.warn("Recursive estimates not supported by EstimatorSample, falling back to EstimatorBasicAvg.");
		return new EstimatorBasicAvg().estim(root);
	}

	@Override
	public double estim(MatrixBlock m1, MatrixBlock m2) {
		return estim(m1, m2, OpCode.MM);
	}
	
	@Override
	public double estim(MatrixBlock m1, MatrixBlock m2, OpCode op) {
		int k;
		int[] ix;
		int[] cnnz;
		double NnzOut = 0;
		long nnzOut = 0;
		double csize = m1.getNumColumns();
		double rsize = m1.getNumRows();
		switch(op) {
		case MM:
			k =  m1.getNumColumns();
			ix = UtilFunctions.getSortedSampleIndexes(
					k, (int)Math.max(k*_frac, 1));
			cnnz = computeColumnNnz(m1, ix);
			for(int i=0; i<ix.length; i++)
				nnzOut = Math.max(nnzOut, cnnz[i] * m2.recomputeNonZeros(ix[i], ix[i]));
			return OptimizerUtils.getSparsity( 
				m1.getNumRows(), m2.getNumColumns(), nnzOut);
		case MULT:
			k = m1.getNumColumns() > m1.getNumRows() ? 
					m1.getNumColumns() : 
					m1.getNumRows();
			ix = UtilFunctions.getSortedSampleIndexes(
				k, (int)Math.max(k*_frac, 1));
			if(m1.getNumColumns() > m1.getNumRows()) {
				cnnz = computeColumnNnz(m1, ix);
				for(int i=0; i<ix.length; i++) {
					NnzOut = (double) Math.max(NnzOut, cnnz[i]/csize * m2.recomputeNonZeros(0, m1.getNumRows()-1, ix[i], ix[i])/csize);
				}
			}
			else {
				cnnz = computeRowNnz(m1, ix);
				for(int i=0; i<ix.length; i++) {
					NnzOut = (double) Math.max(NnzOut, cnnz[i]/rsize * m2.recomputeNonZeros(ix[i], ix[i])/rsize);
				}
			}
			return NnzOut;
		case PLUS:
			k = m1.getNumColumns() > m1.getNumRows() ? 
					m1.getNumColumns() : 
					m1.getNumRows();
			ix = UtilFunctions.getSortedSampleIndexes(
				k, (int)Math.max(k*_frac, 1));
			if(m1.getNumColumns() > m1.getNumRows()) {
				cnnz = computeColumnNnz(m1, ix);
				for(int i=0; i<ix.length; i++) {
					NnzOut = (double) Math.max(NnzOut, cnnz[i]/csize + m2.recomputeNonZeros(0, m1.getNumRows()-1, ix[i], ix[i])/csize - cnnz[i]/csize * m2.recomputeNonZeros(0, m1.getNumRows()-1, ix[i], ix[i])/csize);
				}
			}
			else {
				cnnz = computeRowNnz(m1, ix);
				for(int i=0; i<ix.length; i++) {
					NnzOut = (double) Math.max(NnzOut, cnnz[i]/rsize + m2.recomputeNonZeros(ix[i], ix[i])/rsize - cnnz[i]/rsize * m2.recomputeNonZeros(ix[i], ix[i])/rsize);
				}
			}
			return NnzOut;
		case RBIND:
			return OptimizerUtils.getSparsity(m1.getNumRows() + m2.getNumRows(), 
					m1.getNumColumns(), m1.getNonZeros() + m2.getNonZeros());
		case CBIND:
			return OptimizerUtils.getSparsity(m1.getNumRows(), 
					m1.getNumColumns() + m2.getNumColumns(), m1.getNonZeros() + m2.getNonZeros());
		case EQZERO:
			return 1-m1.getSparsity();
		case NEQZERO:
			return m1.getSparsity();
		case TRANS:
			return m1.getSparsity();
		case DIAG:
			//TODO
		case RESHAPE:
			return m1.getSparsity();
		default:
			throw new NotImplementedException();
		}
	}
	
	@Override
	public double estim(MatrixBlock m, OpCode op) {
		return estim(m, null, op);
	}
	
	private int[] computeColumnNnz(MatrixBlock in, int[] ix) {
		int[] nnz = new int[in.getNumColumns()];
		//count column nnz brute force or selective
		if( in.isInSparseFormat() ) {
			SparseBlock sblock = in.getSparseBlock();
			for( int i=0; i<in.getNumRows(); i++ ) {
				if( sblock.isEmpty(i) ) continue;
				LibMatrixAgg.countAgg(sblock.values(i), nnz,
					sblock.indexes(i), sblock.pos(i), sblock.size(i));
			}
		}
		else {
			DenseBlock dblock = in.getDenseBlock();
			for( int i=0; i<in.getNumRows(); i++ ) {
				double[] avals = dblock.values(i);
				int aix = dblock.pos(i);
				for( int j=0; j<in.getNumColumns(); j++ )
					nnz[j] += (avals[aix+j] != 0) ? 1 : 0;
			}
		}
		
		//copy nnz into reduced vector
		int[] ret = new int[ix.length];
		for(int i=0; i<ix.length; i++)
			ret[i] = nnz[ix[i]];
		return ret;
	}
	
	private int[] computeRowNnz(MatrixBlock in, int[] ix) {
		int[] nnz = new int[in.getNumRows()];
		//count column nnz brute force or selective
		if( in.isInSparseFormat() ) {
			SparseBlock sblock = in.getSparseBlock();
			for( int i=0; i<in.getNumColumns(); i++ ) {
				if( sblock.isEmpty(i) ) continue;
				LibMatrixAgg.countAgg(sblock.values(i), nnz,
					sblock.indexes(i), sblock.pos(i), sblock.size(i));
			}
		}
		else {
			DenseBlock dblock = in.getDenseBlock();
			for( int i=0; i<in.getNumColumns(); i++ ) {
				double[] avals = dblock.values(i);
				int aix = dblock.pos(i);
				for( int j=0; j<in.getNumRows(); j++ )
					nnz[j] += (avals[aix+j] != 0) ? 1 : 0;
			}
		}
		
		//copy nnz into reduced vector
		int[] ret = new int[ix.length];
		for(int i=0; i<ix.length; i++)
			ret[i] = nnz[ix[i]];
		return ret;
	}
}
