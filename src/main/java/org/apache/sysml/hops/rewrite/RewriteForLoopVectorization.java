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

package org.apache.sysml.hops.rewrite;

import java.util.ArrayList;

import org.apache.sysml.hops.AggUnaryOp;
import org.apache.sysml.hops.BinaryOp;
import org.apache.sysml.hops.DataOp;
import org.apache.sysml.hops.Hop;
import org.apache.sysml.hops.Hop.AggOp;
import org.apache.sysml.hops.Hop.Direction;
import org.apache.sysml.hops.Hop.OpOp1;
import org.apache.sysml.hops.HopsException;
import org.apache.sysml.hops.IndexingOp;
import org.apache.sysml.hops.LeftIndexingOp;
import org.apache.sysml.hops.LiteralOp;
import org.apache.sysml.hops.Hop.OpOp2;
import org.apache.sysml.hops.UnaryOp;
import org.apache.sysml.parser.ForStatement;
import org.apache.sysml.parser.ForStatementBlock;
import org.apache.sysml.parser.IfStatementBlock;
import org.apache.sysml.parser.StatementBlock;
import org.apache.sysml.parser.WhileStatementBlock;
import org.apache.sysml.parser.Expression.DataType;

/**
 * Rule: Simplify program structure by pulling if or else statement body out
 * (removing the if statement block ifself) in order to allow intra-procedure
 * analysis to propagate exact statistics.
 * 
 */
public class RewriteForLoopVectorization extends StatementBlockRewriteRule
{

	private static final OpOp2[] MAP_SCALAR_AGGREGATE_SOURCE_OPS = new OpOp2[]{OpOp2.PLUS, OpOp2.MULT, OpOp2.MIN, OpOp2.MAX};
	private static final AggOp[] MAP_SCALAR_AGGREGATE_TARGET_OPS = new AggOp[]{AggOp.SUM,  AggOp.PROD, AggOp.MIN, AggOp.MAX};
	
	
	@Override
	public ArrayList<StatementBlock> rewriteStatementBlock(StatementBlock sb, ProgramRewriteStatus state)
		throws HopsException 
	{
		ArrayList<StatementBlock> ret = new ArrayList<StatementBlock>();
		
		if( sb instanceof ForStatementBlock &&  ((ForStatementBlock) sb).getIterPredicate().getIterVar().size() == 1 )
		{
			ForStatementBlock fsb = (ForStatementBlock) sb;
			ForStatement fs = (ForStatement) fsb.getStatement(0);
			Hop from = fsb.getFromHops();
			Hop to = fsb.getToHops();
			Hop incr = fsb.getIncrementHops();
			String iterVar = fsb.getIterPredicate().getIterVar().get(0).getName();
			
			if( fs.getBody()!=null && fs.getBody().size()==1 ) //single child block
			{
				StatementBlock csb = (StatementBlock) fs.getBody().get(0);
				if( !(   csb instanceof WhileStatementBlock  //last level block
					  || csb instanceof IfStatementBlock 
					  || csb instanceof ForStatementBlock ) )
				{
					//AUTO VECTORIZATION PATTERNS
					//Note: unnecessary row or column indexing then later removed via hop rewrites
					
					//e.g., for(i in a:b){s = s + as.scalar(X[i,2])} -> s = sum(X[a:b,2])
					sb = vectorizeScalarAggregate(sb, csb, from, to, incr, iterVar);  
					
					//e.g., for(i in a:b){X[i,2] = Y[i,1] + Z[i,3]} -> X[a:b,2] = Y[a:b,1] + Z[a:b,3];
					sb = vectorizeElementwiseBinary(sb, csb, from, to, incr, iterVar);
					
					//e.g., for(i in a:b){X[i,2] = abs(Y[i,1])} -> X[a:b,2] = abs(Y[a:b,1]);
					sb = vectorizeElementwiseUnary(sb, csb, from, to, incr, iterVar);
				
					//e.g., for(i in a:b){X[7,i] = Y[1,i]} -> X[7,a:b] = Y[1,a:b];
					sb = vectorizeIndexedCopy(sb, csb, from, to, incr, iterVar);
				}	
			}	
		}	
		
		//if no rewrite applied sb is the original for loop otherwise a last level statement block
		//that includes the equivalent vectorized operations.
		ret.add( sb );
		
		return ret;
	}
	
	private StatementBlock vectorizeScalarAggregate( StatementBlock sb, StatementBlock csb, Hop from, Hop to, Hop increment, String itervar ) 
		throws HopsException
	{
		StatementBlock ret = sb;
		
		//check missing and supported increment values
		if( !(increment!=null && increment instanceof LiteralOp 
				&& ((LiteralOp)increment).getDoubleValue()==1.0) ) {
			return ret;
		}
			
		//check for applicability
		boolean leftScalar = false;
		boolean rightScalar = false;
		boolean rowIx = false; //row or col
		
		if( csb.get_hops()!=null && csb.get_hops().size()==1 ){
			Hop root = csb.get_hops().get(0);
			
			if( root.getDataType()==DataType.SCALAR && root.getInput().get(0) instanceof BinaryOp ) {
				BinaryOp bop = (BinaryOp) root.getInput().get(0);
				Hop left = bop.getInput().get(0);
				Hop right = bop.getInput().get(1);
				
				//check for left scalar plus
				if( HopRewriteUtils.isValidOp(bop.getOp(), MAP_SCALAR_AGGREGATE_SOURCE_OPS) 
					&& left instanceof DataOp && left.getDataType() == DataType.SCALAR
					&& root.getName().equals(left.getName()) 
					&& right instanceof UnaryOp && ((UnaryOp) right).getOp() == OpOp1.CAST_AS_SCALAR
					&& right.getInput().get(0) instanceof IndexingOp )
				{
					IndexingOp ix = (IndexingOp)right.getInput().get(0);
					if( ix.isRowLowerEqualsUpper() && ix.getInput().get(1) instanceof DataOp
						&& ix.getInput().get(1).getName().equals(itervar) ){
						leftScalar = true;
						rowIx = true;
					}
					else if( ix.isColLowerEqualsUpper() && ix.getInput().get(3) instanceof DataOp
						&& ix.getInput().get(3).getName().equals(itervar) ){
						leftScalar = true;
						rowIx = false;
					}
				}
				//check for right scalar plus
				else if( HopRewriteUtils.isValidOp(bop.getOp(), MAP_SCALAR_AGGREGATE_SOURCE_OPS)  
					&& right instanceof DataOp && right.getDataType() == DataType.SCALAR
					&& root.getName().equals(right.getName()) 
					&& left instanceof UnaryOp && ((UnaryOp) left).getOp() == OpOp1.CAST_AS_SCALAR
					&& left.getInput().get(0) instanceof IndexingOp )
				{
					IndexingOp ix = (IndexingOp)left.getInput().get(0);
					if( ix.isRowLowerEqualsUpper() && ix.getInput().get(1) instanceof DataOp
						&& ix.getInput().get(1).getName().equals(itervar) ){
						rightScalar = true;
						rowIx = true;
					}
					else if( ix.isColLowerEqualsUpper() && ix.getInput().get(3) instanceof DataOp
						&& ix.getInput().get(3).getName().equals(itervar) ){
						rightScalar = true;
						rowIx = false;
					}
				}
			}
		}
		
		//apply rewrite if possible
		if( leftScalar || rightScalar ) 
		{
			Hop root = csb.get_hops().get(0);
			BinaryOp bop = (BinaryOp) root.getInput().get(0);
			Hop cast = bop.getInput().get( leftScalar?1:0 );
			Hop ix = cast.getInput().get(0);
			int aggOpPos = HopRewriteUtils.getValidOpPos(bop.getOp(), MAP_SCALAR_AGGREGATE_SOURCE_OPS);
			AggOp aggOp = MAP_SCALAR_AGGREGATE_TARGET_OPS[aggOpPos];
			
			//replace cast with sum
			AggUnaryOp newSum = HopRewriteUtils.createAggUnaryOp(ix, aggOp, Direction.RowCol);
			HopRewriteUtils.removeChildReference(cast, ix);
			HopRewriteUtils.removeChildReference(bop, cast);
			HopRewriteUtils.addChildReference(bop, newSum, leftScalar?1:0 );
			
			//modify indexing expression according to loop predicate from-to
			//NOTE: any redundant index operations are removed via dynamic algebraic simplification rewrites
			int index1 = rowIx ? 1 : 3;
			int index2 = rowIx ? 2 : 4;
			HopRewriteUtils.replaceChildReference(ix, ix.getInput().get(index1), from, index1);
			HopRewriteUtils.replaceChildReference(ix, ix.getInput().get(index2), to, index2);
			
			//update indexing size information
			if( rowIx )
				((IndexingOp)ix).setRowLowerEqualsUpper(false);
			else
				((IndexingOp)ix).setColLowerEqualsUpper(false);	
			ix.refreshSizeInformation();
			
			ret = csb;
			LOG.debug("Applied vectorizeScalarSumForLoop.");
		}
		
		return ret;
	}
	
	private StatementBlock vectorizeElementwiseBinary( StatementBlock sb, StatementBlock csb, Hop from, Hop to, Hop increment, String itervar ) 
		throws HopsException
	{
		StatementBlock ret = sb;
		
		//check supported increment values
		if( !(increment instanceof LiteralOp && ((LiteralOp)increment).getDoubleValue()==1.0) ){
			return ret;
		}
			
		//check for applicability
		boolean apply = false;
		boolean rowIx = false; //row or col
		if( csb.get_hops()!=null && csb.get_hops().size()==1 )
		{
			Hop root = csb.get_hops().get(0);
			
			if( root.getDataType()==DataType.MATRIX && root.getInput().get(0) instanceof LeftIndexingOp )
			{
				LeftIndexingOp lix = (LeftIndexingOp) root.getInput().get(0);
				Hop lixlhs = lix.getInput().get(0);
				Hop lixrhs = lix.getInput().get(1);
				
				if( lixlhs instanceof DataOp && lixrhs instanceof BinaryOp
					&& lixrhs.getInput().get(0) instanceof IndexingOp	
					&& lixrhs.getInput().get(1) instanceof IndexingOp
					&& lixrhs.getInput().get(0).getInput().get(0) instanceof DataOp
					&& lixrhs.getInput().get(1).getInput().get(0) instanceof DataOp)
				{			
					IndexingOp rix0 = (IndexingOp) lixrhs.getInput().get(0);
					IndexingOp rix1 = (IndexingOp) lixrhs.getInput().get(1);
					
					//check for rowwise
					if(    lix.getRowLowerEqualsUpper() && rix0.isRowLowerEqualsUpper() && rix1.isRowLowerEqualsUpper() 
						&& lix.getInput().get(2).getName().equals(itervar)
						&& rix0.getInput().get(1).getName().equals(itervar)
						&& rix1.getInput().get(1).getName().equals(itervar))
					{
						apply = true;
						rowIx = true;
					}
					//check for colwise
					if(    lix.getColLowerEqualsUpper() && rix0.isColLowerEqualsUpper() && rix1.isColLowerEqualsUpper() 
						&& lix.getInput().get(4).getName().equals(itervar)
						&& rix0.getInput().get(3).getName().equals(itervar)
						&& rix1.getInput().get(3).getName().equals(itervar))
					{
						apply = true;
						rowIx = false;
					}
				}
			}
		}	
		
		//apply rewrite if possible
		if( apply ) 
		{
			Hop root = csb.get_hops().get(0);
			LeftIndexingOp lix = (LeftIndexingOp) root.getInput().get(0);
			BinaryOp bop = (BinaryOp) lix.getInput().get(1);
			IndexingOp rix0 = (IndexingOp) bop.getInput().get(0);
			IndexingOp rix1 = (IndexingOp) bop.getInput().get(1);
			int index1 = rowIx ? 2 : 4;
			int index2 = rowIx ? 3 : 5;
			//modify left indexing bounds
			HopRewriteUtils.replaceChildReference(lix, lix.getInput().get(index1),from, index1);
			HopRewriteUtils.replaceChildReference(lix, lix.getInput().get(index2),to, index2);
			//modify both right indexing
			HopRewriteUtils.replaceChildReference(rix0, rix0.getInput().get(index1-1), from, index1-1);
			HopRewriteUtils.replaceChildReference(rix0, rix0.getInput().get(index2-1), to, index2-1);
			HopRewriteUtils.replaceChildReference(rix1, rix1.getInput().get(index1-1), from, index1-1);
			HopRewriteUtils.replaceChildReference(rix1, rix1.getInput().get(index2-1), to, index2-1);
			updateLeftAndRightIndexingSizes(rowIx, lix, rix0, rix1);
			bop.refreshSizeInformation();
			lix.refreshSizeInformation(); //after bop update
			
			ret = csb;
			//ret.liveIn().removeVariable(itervar);
			LOG.debug("Applied vectorizeElementwiseBinaryForLoop.");
		}
		
		return ret;
	}
	
	private StatementBlock vectorizeElementwiseUnary( StatementBlock sb, StatementBlock csb, Hop from, Hop to, Hop increment, String itervar )
		throws HopsException
	{
		StatementBlock ret = sb;
		
		//check supported increment values
		if( !(increment instanceof LiteralOp && ((LiteralOp)increment).getDoubleValue()==1.0) ){
			return ret;
		}
			
		//check for applicability
		boolean apply = false;
		boolean rowIx = false; //row or col
		if( csb.get_hops()!=null && csb.get_hops().size()==1 )
		{
			Hop root = csb.get_hops().get(0);
			
			if( root.getDataType()==DataType.MATRIX && root.getInput().get(0) instanceof LeftIndexingOp )
			{
				LeftIndexingOp lix = (LeftIndexingOp) root.getInput().get(0);
				Hop lixlhs = lix.getInput().get(0);
				Hop lixrhs = lix.getInput().get(1);
				
				if( lixlhs instanceof DataOp && lixrhs instanceof UnaryOp 
					&& lixrhs.getInput().get(0) instanceof IndexingOp
					&& lixrhs.getInput().get(0).getInput().get(0) instanceof DataOp )
				{
					boolean[] tmp = checkLeftAndRightIndexing(lix, 
							(IndexingOp) lixrhs.getInput().get(0), itervar);
					apply = tmp[0];
					rowIx = tmp[1];
				}
			}
		}	
		
		//apply rewrite if possible
		if( apply ) {
			Hop root = csb.get_hops().get(0);
			LeftIndexingOp lix = (LeftIndexingOp) root.getInput().get(0);
			UnaryOp uop = (UnaryOp) lix.getInput().get(1);
			IndexingOp rix = (IndexingOp) uop.getInput().get(0);
			int index1 = rowIx ? 2 : 4;
			int index2 = rowIx ? 3 : 5;
			//modify left indexing bounds
			HopRewriteUtils.replaceChildReference(lix, lix.getInput().get(index1), from, index1);
			HopRewriteUtils.replaceChildReference(lix, lix.getInput().get(index2), to, index2);
			//modify right indexing
			HopRewriteUtils.replaceChildReference(rix, rix.getInput().get(index1-1), from, index1-1);
			HopRewriteUtils.replaceChildReference(rix, rix.getInput().get(index2-1), to, index2-1);
			updateLeftAndRightIndexingSizes(rowIx, lix, rix);
			uop.refreshSizeInformation();
			lix.refreshSizeInformation(); //after uop update
			
			ret = csb;
			LOG.debug("Applied vectorizeElementwiseUnaryForLoop.");
		}
		
		return ret;
	}
	
	private StatementBlock vectorizeIndexedCopy( StatementBlock sb, StatementBlock csb, Hop from, Hop to, Hop increment, String itervar )
		throws HopsException
	{
		StatementBlock ret = sb;
		
		//check supported increment values
		if( !(increment instanceof LiteralOp && ((LiteralOp)increment).getDoubleValue()==1.0) ) {
			return ret;
		}
			
		//check for applicability
		boolean apply = false;
		boolean rowIx = false; //row or col
		if( csb.get_hops()!=null && csb.get_hops().size()==1 )
		{
			Hop root = csb.get_hops().get(0);
			
			if( root.getDataType()==DataType.MATRIX && root.getInput().get(0) instanceof LeftIndexingOp )
			{
				LeftIndexingOp lix = (LeftIndexingOp) root.getInput().get(0);
				Hop lixlhs = lix.getInput().get(0);
				Hop lixrhs = lix.getInput().get(1);
				
				if( lixlhs instanceof DataOp && lixrhs instanceof IndexingOp
					&& lixrhs.getInput().get(0) instanceof DataOp )
				{
					boolean[] tmp = checkLeftAndRightIndexing(lix, (IndexingOp)lixrhs, itervar);
					apply = tmp[0];
					rowIx = tmp[1];
				}
			}
		}	
		
		//apply rewrite if possible
		if( apply ) {
			Hop root = csb.get_hops().get(0);
			LeftIndexingOp lix = (LeftIndexingOp) root.getInput().get(0);
			IndexingOp rix = (IndexingOp) lix.getInput().get(1);
			int index1 = rowIx ? 2 : 4;
			int index2 = rowIx ? 3 : 5;
			//modify left indexing bounds
			HopRewriteUtils.replaceChildReference(lix, lix.getInput().get(index1), from, index1);
			HopRewriteUtils.replaceChildReference(lix, lix.getInput().get(index2), to, index2);
			//modify right indexing
			HopRewriteUtils.replaceChildReference(rix, rix.getInput().get(index1-1), from, index1-1);
			HopRewriteUtils.replaceChildReference(rix, rix.getInput().get(index2-1), to, index2-1);
			updateLeftAndRightIndexingSizes(rowIx, lix, rix);
			
			ret = csb;
			LOG.debug("Applied vectorizeIndexedCopy.");
		}
		
		return ret;
	}
	
	private static boolean[] checkLeftAndRightIndexing(LeftIndexingOp lix, IndexingOp rix, String itervar) {
		boolean[] ret = new boolean[2]; //apply, rowIx
		
		//check for rowwise
		if(    lix.getRowLowerEqualsUpper() && rix.isRowLowerEqualsUpper() 
			&& lix.getInput().get(2).getName().equals(itervar)
			&& rix.getInput().get(1).getName().equals(itervar) ) {
			ret[0] = true;
			ret[1] = true;
		}
		//check for colwise
		if(    lix.getColLowerEqualsUpper() && rix.isColLowerEqualsUpper() 
			&& lix.getInput().get(4).getName().equals(itervar)
			&& rix.getInput().get(3).getName().equals(itervar) ) {
			ret[0] = true;
			ret[1] = false;
		}
		
		return ret;
	} 
	
	private static void updateLeftAndRightIndexingSizes(boolean rowIx, LeftIndexingOp lix, IndexingOp... rix) {
		//unset special flags
		if( rowIx ) {
			lix.setRowLowerEqualsUpper(false);
			for( IndexingOp rixi : rix )
				rixi.setRowLowerEqualsUpper(false);
		}
		else {
			lix.setColLowerEqualsUpper(false);
			for( IndexingOp rixi : rix )
				rixi.setColLowerEqualsUpper(false);
		}
		for( IndexingOp rixi : rix )
			rixi.refreshSizeInformation();
		lix.refreshSizeInformation();
	}
}
