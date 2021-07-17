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

package org.apache.sysds.runtime.compress.lib;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.compress.CompressedMatrixBlock;
import org.apache.sysds.runtime.compress.colgroup.AColGroup;
import org.apache.sysds.runtime.compress.colgroup.ColGroupValue;
import org.apache.sysds.runtime.compress.utils.LinearAlgebraUtils;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.matrix.data.LibMatrixReorg;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.util.CommonThreadPool;

public class CLALibLeftMultBy {
	private static final Log LOG = LogFactory.getLog(CLALibLeftMultBy.class.getName());

	public static MatrixBlock leftMultByMatrixTransposed(CompressedMatrixBlock m1, MatrixBlock m2, MatrixBlock ret,
		int k) {
		if(m2.isEmpty())
			return ret;
		MatrixBlock transposed = new MatrixBlock(m2.getNumColumns(), m2.getNumRows(), false);
		LibMatrixReorg.transpose(m2, transposed);
		ret = leftMultByMatrix(m1, transposed, ret, k);
		ret.recomputeNonZeros();
		return ret;
	}

	public static MatrixBlock leftMultByMatrixTransposed(CompressedMatrixBlock m1, CompressedMatrixBlock m2,
		MatrixBlock ret, int k) {

		prepareReturnMatrix(m1, m2, ret, true);
		leftMultByCompressedTransposedMatrix(m1.getColGroups(), m2, ret, k, m1.getNumColumns(), m1.getMaxNumValues(),
			m1.isOverlapping());

		ret.recomputeNonZeros();
		return ret;
	}

	public static MatrixBlock leftMultByMatrix(CompressedMatrixBlock m1, MatrixBlock m2, MatrixBlock ret, int k) {
		prepareReturnMatrix(m1, m2, ret, false);
		if(m2.isEmpty())
			return ret;
		ret = leftMultByMatrix(m1.getColGroups(), m2, ret, k, m1.getNumColumns(), m1.getMaxNumValues().getLeft(),
			m1.isOverlapping());
		ret.recomputeNonZeros();
		return ret;
	}

	private static MatrixBlock prepareReturnMatrix(MatrixBlock m1, MatrixBlock m2, MatrixBlock ret,
		boolean doTranspose) {
		int numRowsOutput = doTranspose ? m2.getNumColumns() : m2.getNumRows();
		int numColumnsOutput = m1.getNumColumns();
		if(ret == null)
			ret = new MatrixBlock(numRowsOutput, numColumnsOutput, false, numRowsOutput * numColumnsOutput);
		else if(!(ret.getNumColumns() == numColumnsOutput && ret.getNumRows() == numRowsOutput && ret.isAllocated()))
			ret.reset(numRowsOutput, numColumnsOutput, false, numRowsOutput * numColumnsOutput);
		return ret;
	}

	public static void leftMultByTransposeSelf(CompressedMatrixBlock cmb, MatrixBlock result, int k) {
		final int numColumns = cmb.getNumColumns();
		final boolean overlapping = cmb.isOverlapping();
		result.allocateDenseBlock();
		List<AColGroup> groups = new ArrayList<>(cmb.getColGroups().size());
		
		if(overlapping) {
			LOG.warn("Inefficient TSMM with overlapping matrix could be implemented multi-threaded but is not yet.");
			leftMultByCompressedTransposedMatrix(groups, groups, result);
		}
		else if(k <= 1) {
			for(int i = 0; i < groups.size(); i++)
				leftMultByCompressedTransposedMatrix(groups.get(i), groups, result, i, groups.size());
		}
		else {
			try {
				ExecutorService pool = CommonThreadPool.get(k);
				ArrayList<Callable<Object>> tasks = new ArrayList<>();

				for(int i = 0; i < groups.size(); i++) {
					final AColGroup g = groups.get(i);
					tasks.add(new LeftMultByCompressedTransposedMatrixTask(groups, g, result, i, groups.size()));
				}
				
				for(Future<Object> tret : pool.invokeAll(tasks))
					tret.get();
				pool.shutdown();
			}
			catch(InterruptedException | ExecutionException e) {
				throw new DMLRuntimeException(e);
			}
		}

		// Move values in the lower part of the matrix to the upper part
		copyToUpperTriangle(result.getDenseBlockValues(), numColumns);
		// calculate the number of non zeros, and allocate all value locations by copying upper triangle back to bottom.
		long nnz = LinearAlgebraUtils.copyUpperToLowerTriangle(result);
		result.setNonZeros(nnz);
		// Evaluate if the output should be sparsely allocated.
		result.examSparsity();
	}

	private static void copyToUpperTriangle(final double[] c, final int cols) {
		for(int i = 0, offC = 0; i < cols; i++, offC += cols)
			for(int j = i, offR = i * cols; j < cols; j++, offR += cols) {
				final double prev = c[offC + j];
				if(prev == 0)
					c[offC + j] = c[i + offR];
			}

	}

	private static MatrixBlock leftMultByCompressedTransposedMatrix(List<AColGroup> colGroups,
		CompressedMatrixBlock that, MatrixBlock ret, int k, int numColumns, Pair<Integer, int[]> v,
		boolean overlapping) {

		ret.allocateDenseBlock();
		List<AColGroup> thatCGs = that.getColGroups();

		if(k <= 1 || overlapping || that.isOverlapping()) {
			if(overlapping || that.isOverlapping())
				LOG.warn("Inefficient Compressed multiplication with overlapping matrix"
					+ " could be implemented multi-threaded but is not yet.");
			leftMultByCompressedTransposedMatrix(colGroups, thatCGs, ret);
		}
		else {

			try {
				ExecutorService pool = CommonThreadPool.get(k);
				ArrayList<Callable<Object>> tasks = new ArrayList<>();
				for(int i = 0; i < thatCGs.size(); i++) {
					tasks.add(new LeftMultByCompressedTransposedMatrixTask(colGroups, thatCGs.get(i), ret));
				}

				for(Future<Object> tret : pool.invokeAll(tasks))
					tret.get();
				pool.shutdown();
			}
			catch(InterruptedException | ExecutionException e) {
				throw new DMLRuntimeException(e);
			}
		}
		ret.recomputeNonZeros();
		return ret;
	}

	private static class LeftMultByCompressedTransposedMatrixTask implements Callable<Object> {
		private final List<AColGroup> _groups;
		private final AColGroup _left;
		private final MatrixBlock _ret;
		private final int _start;
		private final int _end;

		protected LeftMultByCompressedTransposedMatrixTask(List<AColGroup> groups, AColGroup left, MatrixBlock ret,
			int start, int end) {
			_groups = groups;
			_left = left;
			_ret = ret;
			_start = start;
			_end = end;
		}

		protected LeftMultByCompressedTransposedMatrixTask(List<AColGroup> groups, AColGroup left, MatrixBlock ret) {
			_groups = groups;
			_left = left;
			_ret = ret;
			_start = 0;
			_end = groups.size();
		}

		@Override
		public Object call() {
			try {
				leftMultByCompressedTransposedMatrix(_left, _groups, _ret, _start, _end);
			}
			catch(Exception e) {
				e.printStackTrace();
				throw new DMLRuntimeException(e);
			}
			return null;
		}
	}

	private static void leftMultByCompressedTransposedMatrix(List<AColGroup> thisCG, List<AColGroup> thatCG,
		MatrixBlock ret) {
		for(AColGroup lhs : thatCG)
			leftMultByCompressedTransposedMatrix(lhs, thisCG, ret, 0, thisCG.size());
	}

	private static void leftMultByCompressedTransposedMatrix(AColGroup lhs, List<AColGroup> thisCG, MatrixBlock ret,
		int colGroupStart, int colGroupEnd) {

		for(; colGroupStart < colGroupEnd; colGroupStart++) {
			AColGroup rhs = thisCG.get(colGroupStart);
			if(rhs != lhs)
				rhs.leftMultByAColGroup(lhs, ret);
			else
				rhs.tsmm(ret);
		}

	}

	private static MatrixBlock leftMultByMatrix(List<AColGroup> colGroups, MatrixBlock that, MatrixBlock ret, int k,
		int numColumns, int maxNumValues, boolean overlapping) {

		if(that.isEmpty()) {
			ret.setNonZeros(0);
			return ret;
		}

		ret.allocateDenseBlock();

		if(k == 1)
			leftMultByMatrixPrimitive(colGroups, that, ret, numColumns, 0, that.getNumRows());
		else {
			try {
				final ExecutorService pool = CommonThreadPool.get(k);
				final ArrayList<Callable<MatrixBlock>> tasks = new ArrayList<>();
				final int rowBlockSize = that.getNumRows() < 8 ? 1 : Math.min(Math.max(that.getNumRows() / k, 1), 8);
				// final int rowBlockSize = 4;

				if(overlapping) {
					for(AColGroup g : colGroups) {
						MatrixBlock tmpRet = new MatrixBlock(ret.getNumRows(), ret.getNumColumns(), false);
						tmpRet.allocateDenseBlock();
						for(int blo = 0; blo < that.getNumRows(); blo += rowBlockSize)
							tasks.add(new LeftMatrixColGroupMultTaskOld(g, that, tmpRet, blo,
								Math.min(blo + rowBlockSize, that.getNumRows()), maxNumValues));

					}
					List<Future<MatrixBlock>> futures = pool.invokeAll(tasks);
					pool.shutdown();
					BinaryOperator op = new BinaryOperator(Plus.getPlusFnObject());
					for(Future<MatrixBlock> future : futures)
						ret.binaryOperationsInPlace(op, future.get());
				}
				else {
					if(rowBlockSize > 2) {
						for(int blo = 0; blo < that.getNumRows(); blo += rowBlockSize) {
							tasks.add(new LeftMatrixColGroupMultTaskNew(colGroups, that, ret, numColumns, blo,
								Math.min(blo + rowBlockSize, that.getNumRows())));
						}
					}
					else {

						List<List<AColGroup>> split = split(colGroups, Math.max(k / that.getNumRows(), 1));
						for(int blo = 0; blo < that.getNumRows(); blo += rowBlockSize) {
							for(List<AColGroup> gr : split)
								tasks.add(new LeftMatrixColGroupMultTaskNew(gr, that, ret, numColumns, blo,
									Math.min(blo + rowBlockSize, that.getNumRows())));
						}

						// for(AColGroup g : colGroups)
						// for(int blo = 0; blo < that.getNumRows(); blo += rowBlockSize)
						// tasks.add(new LeftMatrixColGroupMultTaskOld(g, that, ret, blo,
						// Math.min(blo + rowBlockSize, that.getNumRows()), maxNumValues));
					}

					List<Future<MatrixBlock>> futures = pool.invokeAll(tasks);
					pool.shutdown();
					for(Future<MatrixBlock> future : futures)
						future.get();
				}

			}
			catch(InterruptedException | ExecutionException e) {
				throw new DMLRuntimeException(e);
			}
		}
		ret.recomputeNonZeros();
		return ret;
	}

	private static List<List<AColGroup>> split(List<AColGroup> groups, int splits) {
		Collections.sort(groups, Comparator.comparing(AColGroup::getNumValues).reversed());

		List<List<AColGroup>> ret = new ArrayList<>();
		for(int i = 0; i < splits; i++)
			ret.add(new ArrayList<>());

		for(int j = 0; j < groups.size(); j++)
			ret.get(j % splits).add(groups.get(j));

		return ret;
	}

	private static class LeftMatrixColGroupMultTaskOld implements Callable<MatrixBlock> {
		private final AColGroup _group;
		private final MatrixBlock _that;
		private final MatrixBlock _ret;
		private final int _rl;
		private final int _ru;
		private final int _maxNrValues;

		protected LeftMatrixColGroupMultTaskOld(AColGroup group, MatrixBlock that, MatrixBlock ret, int rl, int ru,
			int maxNrValues) {
			_group = group;
			_that = that;
			_ret = ret;
			_rl = rl;
			_ru = ru;
			_maxNrValues = maxNrValues;
		}

		@Override
		public MatrixBlock call() {
			try {
				ColGroupValue.setupThreadLocalMemory(_maxNrValues * (_ru - _rl));
				ColGroupValue.setupLeftMultThreadLocalMemory(_group.getNumCols() * (_ru - _rl));
				_group.leftMultByMatrix(_that, _ret, _rl, _ru);
			}
			catch(Exception e) {
				throw new DMLRuntimeException(e);
			}
			return _ret;
		}
	}

	private static class LeftMatrixColGroupMultTaskNew implements Callable<MatrixBlock> {
		private final List<AColGroup> _groups;
		private final MatrixBlock _that;
		private final MatrixBlock _ret;
		private final int _rl;
		private final int _ru;
		private final int _numColumns;

		protected LeftMatrixColGroupMultTaskNew(List<AColGroup> groups, MatrixBlock that, MatrixBlock ret,
			int numColumns, int rl, int ru) {
			_groups = groups;
			_that = that;
			_ret = ret;
			_rl = rl;
			_ru = ru;
			_numColumns = numColumns;
		}

		@Override
		public MatrixBlock call() {
			try {
				leftMultByMatrixPrimitive(_groups, _that, _ret, _numColumns, _rl, _ru);
			}
			catch(Exception e) {
				throw new DMLRuntimeException(e);
			}
			return _ret;
		}
	}

	private static void leftMultByMatrixPrimitive(List<AColGroup> colGroups, MatrixBlock that, MatrixBlock ret,
		int numColumns, int rl, int ru) {

		if(that.isInSparseFormat()) {
			for(int i = rl; i < ru; i++) {
				for(int j = 0; j < colGroups.size(); j++) {
					colGroups.get(j).leftMultByMatrix(that, ret, i, i + 1);
				}
			}
		}
		else {
			List<ColGroupValue> v = new ArrayList<>();
			int rowBlockSize = 1;
			List<MatrixBlock> preAgg = new ArrayList<>();
			int colGroupBlocking = 16;
			for(int j = 0; j < colGroupBlocking; j++) {
				MatrixBlock m = new MatrixBlock(1, 1, false);
				m.allocateDenseBlock();
				preAgg.add(m);
			}

			MatrixBlock tmpRes = new MatrixBlock(rowBlockSize, numColumns, false);

			for(int j = 0; j < colGroups.size(); j++) {
				AColGroup a = colGroups.get(j);
				if(a instanceof ColGroupValue) {
					ColGroupValue av = (ColGroupValue) a;
					v.add(av);
				}
				else
					a.leftMultByMatrix(that, ret, rl, ru);
			}
			Collections.sort(v, Comparator.comparing(AColGroup::getNumValues).reversed());
			// LOG.error(v);
			for(int g = 0; g < v.size(); g += colGroupBlocking) {
				final int gEnd = Math.min(g + colGroupBlocking, colGroups.size());
				for(int j = g; j < gEnd && j < v.size(); j++) {
					ColGroupValue cg = v.get(j);
					preAgg.get(j % colGroupBlocking).reset(rowBlockSize, cg.getNumValues(), false);
				}
				// int colBlockSize = 16000;
				int colBlockSize = 64000;

				for(int h = rl; h < ru; h += rowBlockSize) {
					for(int i = 0; i < that.getNumColumns(); i += colBlockSize) {
						for(int j = g; j < gEnd && j < v.size(); j++) {
							v.get(j).preAggregateDense(that, preAgg.get(j % colGroupBlocking), h,
								Math.min(h + rowBlockSize, ru), i, Math.min(i + colBlockSize, that.getNumColumns()));
						}
					}
					for(int j = g; j < gEnd && j < v.size(); j++) {
						ColGroupValue vj = v.get(j);
						MatrixBlock preAggJ = preAgg.get(j % colGroupBlocking);
						preAggJ.recomputeNonZeros();
						tmpRes.reset(rowBlockSize, vj.getNumCols(), false);
						MatrixBlock tmp = vj.leftMultByPreAggregateMatrix(preAggJ, tmpRes);
						vj.addMatrixToResult(tmp, ret, h, Math.min(h + rowBlockSize, ru));
						preAggJ.reset();
					}
				}
			}
		}
	}
}
