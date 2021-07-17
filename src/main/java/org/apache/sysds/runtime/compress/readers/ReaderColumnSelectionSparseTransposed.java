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

package org.apache.sysds.runtime.compress.readers;

import org.apache.sysds.runtime.compress.utils.DblArray;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

/**
 * Used to extract the values at certain indexes from each row in a sparse matrix
 * 
 * Keeps returning all-zeros arrays until reaching the last possible index. The current compression algorithm treats the
 * zero-value in a sparse matrix like any other value.
 */
public class ReaderColumnSelectionSparseTransposed extends ReaderColumnSelection {

	private SparseBlock a;
	// current sparse skip positions.
	private int[] sparsePos = null;

	/**
	 * Reader of sparse matrix blocks for compression.
	 * 
	 * This reader should not be used if the input data is not transposed and sparse
	 * 
	 * @param data       The transposed and sparse matrix
	 * @param colIndexes The column indexes to combine
	 */
	public ReaderColumnSelectionSparseTransposed(MatrixBlock data, int[] colIndexes) {
		super(colIndexes, data.getNumColumns());

		sparsePos = new int[colIndexes.length];

		a = data.getSparseBlock();
		int maxSize = 0;
		for(int i = 0; i < colIndexes.length; i++) {
			if(a.isEmpty(_colIndexes[i]))
				// Use -1 to indicate that this column is done.
				sparsePos[i] = -1;
			else {
				sparsePos[i] = a.pos(_colIndexes[i]);
				maxSize += a.size(_colIndexes[i]);
			}
		}

		if(maxSize == 0) {
			_lastRow = _numRows - 1;
		}

	}

	protected DblArray getNextRow() {
		if(_lastRow == _numRows - 1) {
			return null;
		}
		_lastRow++;

		boolean zeroResult = true;
		for(int i = 0; i < _colIndexes.length; i++) {
			int colidx = _colIndexes[i];
			if(sparsePos[i] != -1) {
				final int alen = a.size(colidx) + a.pos(colidx);
				final int[] aix = a.indexes(colidx);
				final double[] avals = a.values(colidx);
				while(sparsePos[i] < alen && aix[sparsePos[i]] < _lastRow) {
					sparsePos[i] += 1;
				}

				if(sparsePos[i] >= alen) {
					// Mark this column as done.
					sparsePos[i] = -1;
					reusableArr[i] = 0;
				}
				else if(aix[sparsePos[i]] == _lastRow) {
					reusableArr[i] = avals[sparsePos[i]];
					zeroResult = false;
				}
				else {
					reusableArr[i] = 0;
				}
			}
		}

		return zeroResult ? emptyReturn : reusableReturn;

	}
}
