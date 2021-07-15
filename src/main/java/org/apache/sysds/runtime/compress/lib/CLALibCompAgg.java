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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.compress.CompressedMatrixBlock;
import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.compress.DMLCompressionException;
import org.apache.sysds.runtime.compress.colgroup.AColGroup;
import org.apache.sysds.runtime.functionobjects.Builtin;
import org.apache.sysds.runtime.functionobjects.Builtin.BuiltinCode;
import org.apache.sysds.runtime.functionobjects.IndexFunction;
import org.apache.sysds.runtime.functionobjects.KahanFunction;
import org.apache.sysds.runtime.functionobjects.KahanPlus;
import org.apache.sysds.runtime.functionobjects.KahanPlusSq;
import org.apache.sysds.runtime.functionobjects.Mean;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.functionobjects.ReduceAll;
import org.apache.sysds.runtime.functionobjects.ReduceCol;
import org.apache.sysds.runtime.functionobjects.ReduceRow;
import org.apache.sysds.runtime.instructions.cp.KahanObject;
import org.apache.sysds.runtime.matrix.data.LibMatrixAgg;
import org.apache.sysds.runtime.matrix.data.LibMatrixBincell;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.data.MatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.util.CommonThreadPool;

public class CLALibCompAgg {

	private static final Log LOG = LogFactory.getLog(CLALibCompAgg.class.getName());

	// private static final long MIN_PAR_AGG_THRESHOLD = 8 * 1024 * 1024;
	private static final long MIN_PAR_AGG_THRESHOLD = 8 * 1024;

	private static ThreadLocal<MatrixBlock> memPool = new ThreadLocal<MatrixBlock>() {
		@Override
		protected MatrixBlock initialValue() {
			return null;
		}
	};

	public static MatrixBlock aggregateUnary(CompressedMatrixBlock inputMatrix, MatrixValue result,
		AggregateUnaryOperator op, int blen, MatrixIndexes indexesIn, boolean inCP) {

		// prepare output dimensions
		CellIndex tempCellIndex = new CellIndex(-1, -1);
		op.indexFn.computeDimension(inputMatrix.getNumRows(), inputMatrix.getNumColumns(), tempCellIndex);

		// initialize and allocate the result
		if(result == null)
			result = new MatrixBlock(tempCellIndex.row, tempCellIndex.column, false);
		else
			result.reset(tempCellIndex.row, tempCellIndex.column, false);
		MatrixBlock ret = (MatrixBlock) result;

		ret.allocateDenseBlock();

		AggregateUnaryOperator opm = replaceKahnOperations(op);

		if(inputMatrix.getColGroups() != null) {
			fillStart(ret, opm);

			if(inputMatrix.isOverlapping() &&
				(opm.aggOp.increOp.fn instanceof KahanPlusSq || (opm.aggOp.increOp.fn instanceof Builtin &&
					(((Builtin) opm.aggOp.increOp.fn).getBuiltinCode() == BuiltinCode.MIN ||
						((Builtin) opm.aggOp.increOp.fn).getBuiltinCode() == BuiltinCode.MAX))))
				aggregateUnaryOverlapping(inputMatrix, ret, opm, indexesIn, inCP);
			else
				aggregateUnaryNormalCompressedMatrixBlock(inputMatrix, ret, opm, blen, indexesIn, inCP);
		}
		ret.recomputeNonZeros();
		if(!inCP) {
			ret = addCorrection(ret, op);
			if(op.aggOp.increOp.fn instanceof Mean)
				ret = addCellCount(ret, op, inputMatrix.getNumRows(), inputMatrix.getNumColumns());
		}
		return ret;

	}

	private static MatrixBlock addCorrection(MatrixBlock ret, AggregateUnaryOperator op) {
		MatrixBlock resWithCorrection;
		switch(op.aggOp.correction) {
			case LASTCOLUMN:
				resWithCorrection = new MatrixBlock(ret.getNumRows(), ret.getNumColumns() + 1, false);
				resWithCorrection.allocateDenseBlock();
				for(int i = 0; i < ret.getNumRows(); i++)
					resWithCorrection.setValue(i, 0, ret.quickGetValue(i, 0));
				break;
			case LASTROW:
				resWithCorrection = new MatrixBlock(ret.getNumRows() + 1, ret.getNumColumns(), false);
				resWithCorrection.allocateDenseBlock();
				for(int i = 0; i < ret.getNumColumns(); i++)
					resWithCorrection.setValue(0, i, ret.quickGetValue(0, i));
				break;
			case LASTFOURCOLUMNS:
			case LASTFOURROWS:
			case LASTTWOCOLUMNS:
				resWithCorrection = new MatrixBlock(ret.getNumRows(), ret.getNumColumns() + 2, false);
				resWithCorrection.allocateDenseBlock();
				for(int i = 0; i < ret.getNumRows(); i++)
					resWithCorrection.setValue(i, 0, ret.quickGetValue(i, 0));
				break;
			case LASTTWOROWS:
				resWithCorrection = new MatrixBlock(ret.getNumRows() + 2, ret.getNumColumns(), false);
				resWithCorrection.allocateDenseBlock();
				for(int i = 0; i < ret.getNumColumns(); i++)
					resWithCorrection.setValue(0, i, ret.quickGetValue(0, i));
				break;
			case INVALID:
			case NONE:
			default:
				resWithCorrection = ret;
		}

		return resWithCorrection;
	}

	private static MatrixBlock addCellCount(MatrixBlock ret, AggregateUnaryOperator op, int nRow, int nCol) {
		if(op.indexFn instanceof ReduceAll)
			ret.setValue(0, 1, (long)nRow * (long)nCol);
		else if(op.indexFn instanceof ReduceCol)
			for(int i = 0; i < nRow; i++)
				ret.setValue(i, 1, nCol);
		else if(op.indexFn instanceof ReduceRow)
			for(int i = 0; i < nCol; i++)
				ret.setValue(1, i, nRow);
		return ret;
	}

	private static AggregateUnaryOperator replaceKahnOperations(AggregateUnaryOperator op) {
		if(op.aggOp.increOp.fn instanceof KahanPlus)
			return new AggregateUnaryOperator(new AggregateOperator(0, Plus.getPlusFnObject()), op.indexFn,
				op.getNumThreads());
		// We dont have an alternative to KahnPlusSq without kahn.
		return op;
	}

	private static void aggregateUnaryNormalCompressedMatrixBlock(CompressedMatrixBlock m, MatrixBlock o,
		AggregateUnaryOperator op, int blen, MatrixIndexes indexesIn, boolean inCP) {

		int k = op.getNumThreads();
		// replace mean operation with plus.
		AggregateUnaryOperator opm = (op.aggOp.increOp.fn instanceof Mean) ? new AggregateUnaryOperator(
			new AggregateOperator(0, Plus.getPlusFnObject()), op.indexFn) : op;

		if(isValidForParallelProcessing(m, op))
			aggregateInParallel(m, o, opm, k);
		else
			aggregateUnaryOperations(opm, m.getColGroups(), o.getDenseBlockValues(), 0, m.getNumRows(),
				m.getNumColumns());

		postProcessAggregate(m, o, op);

	}

	private static boolean isValidForParallelProcessing(CompressedMatrixBlock m1, AggregateUnaryOperator op) {
		return op.getNumThreads() > 1 && m1.getExactSizeOnDisk() > MIN_PAR_AGG_THRESHOLD;
	}

	private static void aggregateInParallel(CompressedMatrixBlock m1, MatrixBlock ret, AggregateUnaryOperator op,
		int k) {

		ExecutorService pool = CommonThreadPool.get(k);
		ArrayList<UnaryAggregateTask> tasks = new ArrayList<>();
		try {
			// compute all compressed column groups
			if(op.indexFn instanceof ReduceCol) {
				ret.allocateDenseBlock();
				final int blkz = CompressionSettings.BITMAP_BLOCK_SZ;
				int blklen = Math.max((int) Math.ceil((double) m1.getNumRows() / (k * 2)), blkz);
				for(int i = 0; i * blklen < m1.getNumRows(); i++)
					tasks.add(new UnaryAggregateTask(m1.getColGroups(), ret, i * blklen,
						Math.min((i + 1) * blklen, m1.getNumRows()), op, m1.getNumColumns()));

			}
			else {
				List<List<AColGroup>> grpParts = createTaskPartition(m1.getColGroups(), k);
				for(List<AColGroup> grp : grpParts)
					tasks.add(new UnaryAggregateTask(grp, ret, 0, m1.getNumRows(), op, m1.getNumColumns(),
						m1.isOverlapping()));
			}

			List<Future<MatrixBlock>> futures = pool.invokeAll(tasks);
			pool.shutdown();

			// aggregate partial results
			if(op.indexFn instanceof ReduceAll)
				if(op.aggOp.increOp.fn instanceof Builtin)
					aggregateResults(ret, futures, op);
				else
					sumResults(ret, futures);
			else if(op.indexFn instanceof ReduceRow && m1.isOverlapping()) {
				if(op.aggOp.increOp.fn instanceof Builtin)
					aggregateResultVectors(ret, futures, op);
				else
					sumResultVectors(ret, futures);
			}
			else
				for(Future<MatrixBlock> f : futures)
					f.get();
		}
		catch(InterruptedException | ExecutionException e) {
			LOG.error("Aggregate In parallel failed.");
			throw new DMLRuntimeException(e);
		}
	}

	private static void sumResults(MatrixBlock ret, List<Future<MatrixBlock>> futures)
		throws InterruptedException, ExecutionException {
		double val = ret.quickGetValue(0, 0);
		for(Future<MatrixBlock> rtask : futures) {
			double tmp = rtask.get().quickGetValue(0, 0);
			val = val + tmp;
		}
		ret.quickSetValue(0, 0, val);

	}

	private static void sumResultVectors(MatrixBlock ret, List<Future<MatrixBlock>> futures)
		throws InterruptedException, ExecutionException {

		double[] retVals = ret.getDenseBlockValues();
		for(Future<MatrixBlock> rtask : futures) {
			double[] taskResult = rtask.get().getDenseBlockValues();
			for(int i = 0; i < retVals.length; i++) {
				retVals[i] += taskResult[i];
			}
		}
		ret.setNonZeros(ret.getNumColumns());
	}

	private static void aggregateResults(MatrixBlock ret, List<Future<MatrixBlock>> futures, AggregateUnaryOperator op)
		throws InterruptedException, ExecutionException {
		double val = ret.quickGetValue(0, 0);
		for(Future<MatrixBlock> rtask : futures) {
			double tmp = rtask.get().quickGetValue(0, 0);
			val = op.aggOp.increOp.fn.execute(val, tmp);
		}
		ret.quickSetValue(0, 0, val);
	}

	private static void aggregateResultVectors(MatrixBlock ret, List<Future<MatrixBlock>> futures,
		AggregateUnaryOperator op) throws InterruptedException, ExecutionException {
		double[] retVals = ret.getDenseBlockValues();
		for(Future<MatrixBlock> rtask : futures) {
			double[] taskResult = rtask.get().getDenseBlockValues();
			for(int i = 0; i < retVals.length; i++) {
				retVals[i] = op.aggOp.increOp.fn.execute(retVals[i], taskResult[i]);
			}
		}
		ret.setNonZeros(ret.getNumColumns());
	}

	private static void divideByNumberOfCellsForMean(CompressedMatrixBlock m1, MatrixBlock ret, IndexFunction idxFn) {
		if(idxFn instanceof ReduceAll)
			divideByNumberOfCellsForMeanAll(m1, ret);
		else if(idxFn instanceof ReduceCol)
			divideByNumberOfCellsForMeanRows(m1, ret);
		else if(idxFn instanceof ReduceRow)
			divideByNumberOfCellsForMeanCols(m1, ret);
	}

	private static void divideByNumberOfCellsForMeanRows(CompressedMatrixBlock m1, MatrixBlock ret) {
		double[] values = ret.getDenseBlockValues();
		for(int i = 0; i < m1.getNumRows(); i++)
			values[i] = values[i] / m1.getNumColumns();

	}

	private static void divideByNumberOfCellsForMeanCols(CompressedMatrixBlock m1, MatrixBlock ret) {
		double[] values = ret.getDenseBlockValues();
		for(int i = 0; i < m1.getNumColumns(); i++)
			values[i] = values[i] / m1.getNumRows();

	}

	private static void divideByNumberOfCellsForMeanAll(CompressedMatrixBlock m1, MatrixBlock ret) {
		ret.quickSetValue(0, 0, ret.quickGetValue(0, 0) / ((long)m1.getNumColumns() * (long)m1.getNumRows()));
	}

	private static void postProcessAggregate(CompressedMatrixBlock m1, MatrixBlock ret, AggregateUnaryOperator op) {
		if(op.aggOp.increOp.fn instanceof Mean)
			divideByNumberOfCellsForMean(m1, ret, op.indexFn);

	}

	private static void aggregateUnaryOverlapping(CompressedMatrixBlock m1, MatrixBlock ret, AggregateUnaryOperator op,
		MatrixIndexes indexesIn, boolean inCP) {
		try {
			List<Future<MatrixBlock>> rtasks = generateUnaryAggregateOverlappingFutures(m1, ret, op);
			reduceOverlappingFutures(rtasks, ret, op);
		}
		catch(InterruptedException | ExecutionException e) {
			throw new DMLRuntimeException(e);
		}

	}

	private static void reduceOverlappingFutures(List<Future<MatrixBlock>> rtasks, MatrixBlock ret,
		AggregateUnaryOperator op) throws InterruptedException, ExecutionException {
		if(isReduceAll(ret, op.indexFn))
			reduceAllOverlappingFutures(rtasks, ret, op);
		else if(op.indexFn instanceof ReduceRow)
			reduceColOverlappingFutures(rtasks, ret, op);
		else
			reduceRowOverlappingFutures(rtasks, ret, op);
	}

	private static void reduceColOverlappingFutures(List<Future<MatrixBlock>> rtasks, MatrixBlock ret,
		AggregateUnaryOperator op) throws InterruptedException, ExecutionException {
		for(Future<MatrixBlock> rtask : rtasks) {
			LibMatrixBincell.bincellOpInPlace(ret, rtask.get(),
				(op.aggOp.increOp.fn instanceof KahanFunction) ? new BinaryOperator(
					Plus.getPlusFnObject()) : op.aggOp.increOp);
		}
	}

	private static void reduceRowOverlappingFutures(List<Future<MatrixBlock>> rtasks, MatrixBlock ret,
		AggregateUnaryOperator op) throws InterruptedException, ExecutionException {
		for(Future<MatrixBlock> rtask : rtasks)
			rtask.get();

	}

	private static boolean isReduceAll(MatrixBlock ret, IndexFunction idxFn) {
		return idxFn instanceof ReduceAll || (ret.getNumColumns() == 1 && ret.getNumRows() == 1);
	}

	private static void reduceAllOverlappingFutures(List<Future<MatrixBlock>> rtasks, MatrixBlock ret,
		AggregateUnaryOperator op) throws InterruptedException, ExecutionException {

		if(op.aggOp.increOp.fn instanceof KahanFunction) {
			KahanObject kbuff = new KahanObject(ret.quickGetValue(0, 0), 0);
			KahanPlus kplus = KahanPlus.getKahanPlusFnObject();
			for(Future<MatrixBlock> rtask : rtasks) {
				double tmp = rtask.get().quickGetValue(0, 0);

				kplus.execute2(kbuff, tmp);
			}
			ret.quickSetValue(0, 0, kbuff._sum);
		}
		else {
			double val = ret.quickGetValue(0, 0);
			for(Future<MatrixBlock> rtask : rtasks) {
				double tmp = rtask.get().quickGetValue(0, 0);
				val = op.aggOp.increOp.fn.execute(val, tmp);
			}
			ret.quickSetValue(0, 0, val);
		}
	}

	private static List<Future<MatrixBlock>> generateUnaryAggregateOverlappingFutures(CompressedMatrixBlock m1,
		MatrixBlock ret, AggregateUnaryOperator op) throws InterruptedException {

		ExecutorService pool = CommonThreadPool.get(op.getNumThreads());
		ArrayList<UnaryAggregateOverlappingTask> tasks = new ArrayList<>();

		final int blklen = Math.min(m1.getNumRows() / op.getNumThreads(), CompressionSettings.BITMAP_BLOCK_SZ);

		for(int i = 0; i * blklen < m1.getNumRows(); i++)
			tasks.add(new UnaryAggregateOverlappingTask(m1, ret, i * blklen,
				Math.min((i + 1) * blklen, m1.getNumRows()), op));

		List<Future<MatrixBlock>> futures = pool.invokeAll(tasks);
		pool.shutdown();
		return futures;
	}

	private static List<List<AColGroup>> createTaskPartition(List<AColGroup> colGroups, int k) {
		int numTasks = Math.min(k, colGroups.size());
		List<List<AColGroup>> grpParts = new ArrayList<>();
		for(int i = 0; i < numTasks; i++) {
			grpParts.add(new ArrayList<>());
		}
		int pos = 0;
		for(AColGroup grp : colGroups) {
			List<AColGroup> g = grpParts.get(pos);
			g.add(grp);
			pos = (pos + 1) % numTasks;
		}

		return grpParts;
	}

	private static void aggregateUnaryOperations(AggregateUnaryOperator op, List<AColGroup> groups, double[] ret,
		int rl, int ru, int numColumns) {
		if(op.indexFn instanceof ReduceCol && op.aggOp.increOp.fn instanceof Builtin)
			aggregateUnaryBuiltinRowOperation(op, groups, ret, rl, ru, numColumns);
		else
			aggregateUnaryNormalOperation(op, groups, ret, rl, ru, numColumns);
	}

	private static void aggregateUnaryNormalOperation(AggregateUnaryOperator op, List<AColGroup> groups, double[] ret,
		int rl, int ru, int numColumns) {
		for(AColGroup grp : groups)
			grp.unaryAggregateOperations(op, ret, rl, ru);

	}

	private static void aggregateUnaryBuiltinRowOperation(AggregateUnaryOperator op, List<AColGroup> groups,
		double[] ret, int rl, int ru, int numColumns) {

		boolean isDense = true;
		for(AColGroup g : groups) {
			isDense &= g.isDense();
		}
		if(isDense) {
			for(AColGroup grp : groups) {
				grp.unaryAggregateOperations(op, ret, rl, ru);
			}
		}
		else {

			int[] rnnz = new int[ru - rl];
			int numberDenseColumns = 0;
			for(AColGroup grp : groups) {
				grp.unaryAggregateOperations(op, ret, rl, ru);
				if(grp.isDense())
					numberDenseColumns += grp.getNumCols();
				else {
					grp.countNonZerosPerRow(rnnz, rl, ru);
				}
			}
			for(int row = rl; row < ru; row++)
				if(rnnz[row - rl] + numberDenseColumns < numColumns)
					ret[row] = op.aggOp.increOp.fn.execute(ret[row], 0.0);
		}

	}

	private static void fillStart(MatrixBlock ret, AggregateUnaryOperator op) {
		if(op.aggOp.increOp.fn instanceof Builtin) {
			Double val = null;
			switch(((Builtin) op.aggOp.increOp.fn).getBuiltinCode()) {
				case MAX:
					val = Double.NEGATIVE_INFINITY;
					break;
				case MIN:
					val = Double.POSITIVE_INFINITY;
					break;
				default:
					break;
			}
			if(val != null) {
				ret.getDenseBlock().set(val);
			}
		}
	}

	private static class UnaryAggregateTask implements Callable<MatrixBlock> {
		private final List<AColGroup> _groups;
		private final int _rl;
		private final int _ru;
		private final MatrixBlock _ret;
		private final int _numColumns;
		private final AggregateUnaryOperator _op;

		protected UnaryAggregateTask(List<AColGroup> groups, MatrixBlock ret, int rl, int ru, AggregateUnaryOperator op,
			int numColumns) {
			_groups = groups;
			_op = op;
			_rl = rl;
			_ru = ru;
			_numColumns = numColumns;

			if(_op.indexFn instanceof ReduceAll) { // sum
				_ret = new MatrixBlock(1, 1, false);
				_ret.allocateDenseBlock();
				if(_op.aggOp.increOp.fn instanceof Builtin)
					System.arraycopy(ret.getDenseBlockValues(), 0, _ret.getDenseBlockValues(), 0,
						ret.getNumRows() * ret.getNumColumns());
			}
			else // colSums / rowSums
				_ret = ret;

		}

		protected UnaryAggregateTask(List<AColGroup> groups, MatrixBlock ret, int rl, int ru, AggregateUnaryOperator op,
			int numColumns, boolean overlapping) {
			_groups = groups;
			_op = op;
			_rl = rl;
			_ru = ru;
			_numColumns = numColumns;

			if(_op.indexFn instanceof ReduceAll || (_op.indexFn instanceof ReduceRow && overlapping)) {
				_ret = new MatrixBlock(ret.getNumRows(), ret.getNumColumns(), false);
				_ret.allocateDenseBlock();
				if(_op.aggOp.increOp.fn instanceof Builtin)
					System.arraycopy(ret.getDenseBlockValues(), 0, _ret.getDenseBlockValues(), 0,
						ret.getNumRows() * ret.getNumColumns());
			}
			else {
				// colSums / rowSums
				_ret = ret;
			}

		}

		@Override
		public MatrixBlock call() {
			aggregateUnaryOperations(_op, _groups, _ret.getDenseBlockValues(), _rl, _ru, _numColumns);
			return _ret;
		}
	}

	private static class UnaryAggregateOverlappingTask implements Callable<MatrixBlock> {
		private final CompressedMatrixBlock _m1;
		private final int _rl;
		private final int _ru;
		private final MatrixBlock _ret;
		private final AggregateUnaryOperator _op;

		protected UnaryAggregateOverlappingTask(CompressedMatrixBlock m1, MatrixBlock ret, int rl, int ru,
			AggregateUnaryOperator op) {
			_m1 = m1;
			_op = op;
			_rl = rl;
			_ru = ru;
			_ret = ret;
		}

		private MatrixBlock getTmp() {
			MatrixBlock tmp = memPool.get();
			if(tmp == null) {
				memPool.set(new MatrixBlock(_ru - _rl, _m1.getNumColumns(), false, -1).allocateBlock());
				tmp = memPool.get();
			}
			else
				tmp.reset(_ru - _rl, _m1.getNumColumns(), false, -1);

			return tmp;
		}

		private MatrixBlock decompressToTemp() {
			MatrixBlock tmp = getTmp();
			for(AColGroup g : _m1.getColGroups())
				g.decompressToBlockUnSafe(tmp, _rl, _ru, 0);
			tmp.setNonZeros(_rl + _ru);
			return tmp;
		}

		@Override
		public MatrixBlock call() {
			MatrixBlock tmp = decompressToTemp();
			MatrixBlock outputBlock = tmp.prepareAggregateUnaryOutput(_op, null,
				Math.max(tmp.getNumColumns(), tmp.getNumRows()));
			LibMatrixAgg.aggregateUnaryMatrix(tmp, outputBlock, _op);
			outputBlock.dropLastRowsOrColumns(_op.aggOp.correction);

			if(_op.indexFn instanceof ReduceCol) {
				if(outputBlock.isEmpty())
					return null;
				else if(outputBlock.isInSparseFormat()) {
					throw new DMLCompressionException("Not implemented sparse and "
						+ "not something that should ever happen" + " because we dont use sparse for column matrices");
				}
				else {
					double[] retValues = _ret.getDenseBlockValues();
					int currentIndex = _rl * _ret.getNumColumns();
					double[] outputBlockValues = outputBlock.getDenseBlockValues();
					System.arraycopy(outputBlockValues, 0, retValues, currentIndex, outputBlockValues.length);
					return null;
				}
			}
			else
				return outputBlock;

		}
	}
}
