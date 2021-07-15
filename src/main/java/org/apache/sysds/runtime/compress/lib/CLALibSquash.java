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

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.compress.CompressedMatrixBlock;
import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.compress.CompressionSettingsBuilder;
import org.apache.sysds.runtime.compress.DMLCompressionException;
import org.apache.sysds.runtime.compress.colgroup.AColGroup;
import org.apache.sysds.runtime.compress.colgroup.AColGroup.CompressionType;
import org.apache.sysds.runtime.compress.colgroup.ColGroupFactory;
import org.apache.sysds.runtime.compress.colgroup.ColGroupValue;
import org.apache.sysds.runtime.compress.readers.ReaderColumnSelection;
import org.apache.sysds.runtime.compress.utils.ABitmap;
import org.apache.sysds.runtime.compress.utils.DblArrayIntListHashMap;
import org.apache.sysds.runtime.util.CommonThreadPool;

public class CLALibSquash {

	// private static final Log LOG = LogFactory.getLog(CLALibSquash.class.getName());

	public static CompressedMatrixBlock squash(CompressedMatrixBlock m, int k) {

		CompressedMatrixBlock ret = new CompressedMatrixBlock(m.getNumRows(), m.getNumColumns());
		CompressionSettings cs = new CompressionSettingsBuilder().create();

		double[] minMaxes = extractMinMaxes(m);
		List<AColGroup> retCg = (k <= 1) ? singleThreadSquash(m, cs, minMaxes) : multiThreadSquash(m, cs, k, minMaxes);

		ret.allocateColGroupList(retCg);
		ret.recomputeNonZeros();

		if(ret.isOverlapping())
			throw new DMLCompressionException("Squash should output compressed nonOverlapping matrix");
		return ret;
	}

	private static double[] extractMinMaxes(CompressedMatrixBlock m) {
		double[] ret = new double[m.getNumColumns() * 2];
		for(AColGroup g : m.getColGroups())
			if(g instanceof ColGroupValue)
				((ColGroupValue) g).addMinMax(ret);
			else
				throw new DMLCompressionException(
					"Not valid to squash if not all colGroups are of ColGroupValue type.");

		return ret;
	}

	private static List<AColGroup> singleThreadSquash(CompressedMatrixBlock m, CompressionSettings cs,
		double[] minMaxes) {
		List<AColGroup> retCg = new ArrayList<>();

		int blkSz = 1;
		for(int i = 0; i < m.getNumColumns(); i += blkSz) {
			int[] columnIds = new int[Math.min(blkSz, m.getNumColumns() - i)];
			for(int j = 0; j < Math.min(blkSz, m.getNumColumns() - i); j++)
				columnIds[j] = i + j;
			retCg.add(extractNewGroup(m, cs, columnIds, minMaxes));
		}
		return retCg;
	}

	private static List<AColGroup> multiThreadSquash(CompressedMatrixBlock m, CompressionSettings cs, int k,
		double[] minMaxes) {
		List<AColGroup> retCg = new ArrayList<>();
		ExecutorService pool = CommonThreadPool.get(k);
		ArrayList<SquashTask> tasks = new ArrayList<>();

		try {
			int blkSz = 1;
			for(int i = 0; i < m.getNumColumns(); i += blkSz) {
				int[] columnIds = new int[Math.min(blkSz, m.getNumColumns() - i)];
				for(int j = 0; j < Math.min(blkSz, m.getNumColumns() - i); j++)
					columnIds[j] = i + j;
				tasks.add(new SquashTask(m, cs, columnIds, minMaxes));
			}

			for(Future<AColGroup> future : pool.invokeAll(tasks))
				retCg.add(future.get());
			pool.shutdown();
		}
		catch(InterruptedException | ExecutionException e) {
			throw new DMLRuntimeException(e);
		}

		return retCg;
	}

	private static AColGroup extractNewGroup(CompressedMatrixBlock m, CompressionSettings cs, int[] columnIds,
		double[] minMaxes) {

		ABitmap map = extractBitmap(columnIds, m);

		AColGroup newGroup = ColGroupFactory.compress(columnIds, m.getNumRows(), map, CompressionType.DDC, cs, m, 1);
		return newGroup;
	}

	private static ABitmap extractBitmap(int[] colIndices, CompressedMatrixBlock compressedBlock) {
		ReaderColumnSelection r = ReaderColumnSelection.createCompressedReader(compressedBlock, colIndices);
		DblArrayIntListHashMap map = new DblArrayIntListHashMap(256);
		ABitmap x = BitmapEncoder.extractBitmapMultiColumns(colIndices,r, compressedBlock.getNumRows(),map);
		return BitmapLossyEncoder.makeBitmapLossy(x, compressedBlock.getNumRows());
	}

	private static class SquashTask implements Callable<AColGroup> {
		private final CompressedMatrixBlock _m;
		private final CompressionSettings _cs;
		private final int[] _columnIds;
		private final double[] _minMaxes;

		protected SquashTask(CompressedMatrixBlock m, CompressionSettings cs, int[] columnIds, double[] minMaxes) {
			_m = m;
			_cs = cs;
			_columnIds = columnIds;
			_minMaxes = minMaxes;
		}

		@Override
		public AColGroup call() {
			return extractNewGroup(_m, _cs, _columnIds, _minMaxes);
		}
	}
}
