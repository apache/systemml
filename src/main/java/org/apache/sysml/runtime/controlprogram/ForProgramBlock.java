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

package org.apache.sysml.runtime.controlprogram;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.sysml.api.DMLScript;
import org.apache.sysml.hops.Hop;
import org.apache.sysml.parser.ForStatementBlock;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLScriptException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject.UpdateType;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.util.BatchIteratorFactory;
import org.apache.sysml.runtime.instructions.Instruction;
import org.apache.sysml.runtime.instructions.cp.Data;
import org.apache.sysml.runtime.instructions.cp.IntObject;
import org.apache.sysml.runtime.instructions.cp.ScalarObject;
import org.apache.sysml.runtime.util.UtilFunctions;
import org.apache.sysml.yarn.DMLAppMasterUtils;

public class ForProgramBlock extends ProgramBlock
{	
	protected ArrayList<Instruction> 	_fromInstructions;
	protected ArrayList<Instruction> 	_toInstructions;
	protected ArrayList<Instruction> 	_incrementInstructions;
	
	protected ArrayList <Instruction> 	_exitInstructions ;
	protected ArrayList<ProgramBlock> 	_childBlocks;

	protected String[]                  _iterablePredicateVars; //from,to,where constants/internal vars not captured via instructions

	
	public ForProgramBlock(Program prog, String[] iterPredVars) {
		super(prog);
		
		_exitInstructions = new ArrayList<Instruction>();
		_childBlocks = new ArrayList<ProgramBlock>();
		_iterablePredicateVars = iterPredVars;
	}
	
	public ArrayList<Instruction> getFromInstructions() {
		return _fromInstructions;
	}
	
	public void setFromInstructions(ArrayList<Instruction> instructions) {
		_fromInstructions = instructions;
	}
	
	public ArrayList<Instruction> getToInstructions() {
		return _toInstructions;
	}
	
	public void setToInstructions(ArrayList<Instruction> instructions) {
		_toInstructions = instructions;
	}
	
	public ArrayList<Instruction> getIncrementInstructions() {
		return _incrementInstructions;
	}
	
	public void setIncrementInstructions(ArrayList<Instruction> instructions) {
		_incrementInstructions = instructions;
	}

	public ArrayList<Instruction> getExitInstructions() {
		return _exitInstructions;
	}
	
	public void setExitInstructions(ArrayList<Instruction> inst) {
		_exitInstructions = inst;
	}
	
	public void addProgramBlock(ProgramBlock childBlock) {
		_childBlocks.add(childBlock);
	}
	
	public ArrayList<ProgramBlock> getChildBlocks() {
		return _childBlocks;
	}
	
	public void setChildBlocks(ArrayList<ProgramBlock> pbs) {
		_childBlocks = pbs;
	}
	
	public String[] getIterablePredicateVars() {
		return _iterablePredicateVars;
	}
	
	public void setIterablePredicateVars(String[] iterPredVars) {
		_iterablePredicateVars = iterPredVars;
	}
	
	@Override	
	public void execute(ExecutionContext ec) 
		throws DMLRuntimeException
	{
		// add the iterable predicate variable to the variable set
		String iterVarName = _iterablePredicateVars[0];
		
		Iterable<Data> genericIterator = null;
				
		if( _iterablePredicateVars[1] != null ) {
			//probe for matrix variables
			Data ldat = ec.getVariable( _iterablePredicateVars[1] );
			if( ldat != null && ldat instanceof MatrixObject ) {
				// Iterable for loop
				genericIterator = BatchIteratorFactory.getBatchIterator(ec, _iterablePredicateVars);
			}
		}		

		if(genericIterator == null) {
			// evaluate from, to, incr only once (assumption: known at for entry)
			IntObject from = executePredicateInstructions( 1, _fromInstructions, ec );
			IntObject to   = executePredicateInstructions( 2, _toInstructions, ec );
			IntObject incr = (_incrementInstructions == null || _incrementInstructions.isEmpty()) && _iterablePredicateVars[3]==null ? 
					new IntObject((from.getLongValue()<=to.getLongValue()) ? 1 : -1) :
					executePredicateInstructions( 3, _incrementInstructions, ec );
			if ( incr.getLongValue() == 0 ) //would produce infinite loop
				throw new DMLRuntimeException(this.printBlockErrorLocation() + "Expression for increment of variable '" + iterVarName + "' must evaluate to a non-zero value.");
			genericIterator = (Iterable<Data>)(new SequenceIterator(iterVarName, from, to, incr));
		}
		
		// execute for loop
		try 
		{
			// prepare update in-place variables
			UpdateType[] flags = prepareUpdateInPlaceVariables(ec, _tid);
			
			// run for loop body for each instance of predicate sequence
			for( Data iterVar : genericIterator )  {
				//set iteration variable
				ec.setVariable(iterVarName, iterVar); 
				
				//execute all child blocks
				for(int i=0 ; i < this._childBlocks.size() ; i++) {
					ec.updateDebugState( i );
					_childBlocks.get(i).execute(ec);
				}				
			}
			
			// reset update-in-place variables
			resetUpdateInPlaceVariableFlags(ec, flags);
		}
		catch (DMLScriptException e) {
			//propagate stop call
			throw e;
		}
		catch (Exception e) {
			throw new DMLRuntimeException(printBlockErrorLocation() + "Error evaluating for program block", e);
		}
		
		//execute exit instructions
		try {
			executeInstructions(_exitInstructions, ec);	
		}
		catch (Exception e){
			throw new DMLRuntimeException(printBlockErrorLocation() + "Error evaluating for exit instructions", e);
		}
	}

	protected IntObject executePredicateInstructions( int pos, ArrayList<Instruction> instructions, ExecutionContext ec ) 
		throws DMLRuntimeException
	{
		ScalarObject tmp = null;
		IntObject ret = null;
		
		try
		{
			if( _iterablePredicateVars[pos] != null )
			{
				//probe for scalar variables
				Data ldat = ec.getVariable( _iterablePredicateVars[pos] );
				if( ldat != null && ldat instanceof ScalarObject )
					tmp = (ScalarObject)ldat;
				else //handle literals
					tmp = new IntObject( UtilFunctions.parseToLong(_iterablePredicateVars[pos]) );
			}		
			else
			{
				if( _sb!=null )
				{
					if( DMLScript.isActiveAM() ) //set program block specific remote memory
						DMLAppMasterUtils.setupProgramBlockRemoteMaxMemory(this);
					
					ForStatementBlock fsb = (ForStatementBlock)_sb;
					Hop predHops = null;
					boolean recompile = false;
					if (pos == 1){ 
						predHops = fsb.getFromHops();
						recompile = fsb.requiresFromRecompilation();
					}
					else if (pos == 2) {
						predHops = fsb.getToHops();
						recompile = fsb.requiresToRecompilation();
					}
					else if (pos == 3){
						predHops = fsb.getIncrementHops();
						recompile = fsb.requiresIncrementRecompilation();
					}
					tmp = (IntObject) executePredicate(instructions, predHops, recompile, ValueType.INT, ec);
				}
				else
					tmp = (IntObject) executePredicate(instructions, null, false, ValueType.INT, ec);
			}
		}
		catch(Exception ex)
		{
			String predNameStr = null;
			if 		(pos == 1) predNameStr = "from";
			else if (pos == 2) predNameStr = "to";
			else if (pos == 3) predNameStr = "increment";
			
			throw new DMLRuntimeException(this.printBlockErrorLocation() +"Error evaluating '" + predNameStr + "' predicate", ex);
		}
		
		//final check of resulting int object (guaranteed to be non-null, see executePredicate)
		if( tmp instanceof IntObject )
			ret = (IntObject)tmp;
		else //downcast to int if necessary
			ret = new IntObject(tmp.getName(),tmp.getLongValue()); 
		
		return ret;
	}
	
	public String printBlockErrorLocation(){
		return "ERROR: Runtime error in for program block generated from for statement block between lines " + _beginLine + " and " + _endLine + " -- ";
	}
	
	/**
	 * Utility class for iterating over positive or negative predicate sequences.
	 */
	protected class SequenceIterator implements Iterator<Data>, Iterable<Data>
	{
		private String _varName = null;
		private long _cur = -1;
		private long _to = -1;
		private long _incr = -1;
		private boolean _inuse = false;
		
		protected SequenceIterator(String varName, IntObject from, IntObject to, IntObject incr) {
			_varName = varName;
			_cur = from.getLongValue();
			_to = to.getLongValue();
			_incr = incr.getLongValue();
		}

		@Override
		public boolean hasNext() {
			return _incr > 0 ? _cur <= _to : _cur >= _to;
		}

		@Override
		public IntObject next() {
			IntObject ret = new IntObject( _varName, _cur );
			_cur += _incr; //update current val
			return ret;
		}

		@Override
		public Iterator<Data> iterator() {
			if( _inuse )
				throw new RuntimeException("Unsupported reuse of iterator.");				
			_inuse = true;
			return this;
		}

		@Override
		public void remove() {
			throw new RuntimeException("Unsupported remove on iterator.");
		}
	}
}
