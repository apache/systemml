package org.apache.sysml.runtime.controlprogram.paramserv;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.sysml.parser.Expression;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContextFactory;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.cp.AggregateBinaryCPInstruction;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;

/**
 * Data partitioner Disjoint_Random:
 * for each worker, use a permutation multiply P[beg:end,] %*% X,
 * where P is constructed for example with P=table(seq(1,nrow(X),sample(nrow(X), nrow(X)))),
 * i.e., sampling without replacement to ensure disjointness.
 *
 */
public class DataPartitionerDR extends DataPartitioner {

	@Override
	public List<MatrixObject> doPartition(int k, MatrixObject mo) {
		ExecutionContext ec = ExecutionContextFactory.createContext();

		// Generate the permutation
		MatrixObject permutation = ParamservUtils.generatePermutation(mo, ec);

		// Slice the original matrix and make data partition by permutation multiply
		AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg, 1);
		AggregateBinaryCPInstruction multiInst = new AggregateBinaryCPInstruction(aggbin,
				new CPOperand("permutation", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
				new CPOperand("data"), new CPOperand("result", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
				"ba+*", "permutation multiply");
		ec.setVariable("data", mo);

		int batchSize = (int) Math.ceil(mo.getNumRows() / k);
		return IntStream.range(0, k).mapToObj(i -> {
			long begin = i * batchSize + 1;
			long end = Math.min(begin + batchSize, mo.getNumRows());
			MatrixObject partialMO = ParamservUtils.sliceMatrix(permutation, begin, end);
			ec.setVariable("permutation", partialMO);
			MatrixObject result = ParamservUtils.newMatrixObject();
			ec.setVariable("result", result);
			multiInst.processInstruction(ec);
			ParamservUtils.cleanupData(partialMO);
			return result;
		}).collect(Collectors.toList());
	}
}
