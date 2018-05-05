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

package org.apache.sysml.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.hops.Hop;
import org.apache.sysml.hops.recompile.Recompiler;
import org.apache.sysml.hops.rewrite.StatementBlockRewriteRule;
import org.apache.sysml.lops.Lop;
import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.parser.Expression.FormatType;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.parser.LanguageException.LanguageErrorCodes;
import org.apache.sysml.parser.PrintStatement.PRINTTYPE;
import org.apache.sysml.runtime.controlprogram.parfor.util.IDSequence;
import org.apache.sysml.utils.MLContextProxy;


public class StatementBlock extends LiveVariableAnalysis implements ParseInfo
{

	protected static final Log LOG = LogFactory.getLog(StatementBlock.class.getName());
	protected static IDSequence _seq = new IDSequence();

	protected DMLProgram _dmlProg;
	protected ArrayList<Statement> _statements;
	ArrayList<Hop> _hops = null;
	ArrayList<Lop> _lops = null;
	HashMap<String,ConstIdentifier> _constVarsIn;
	HashMap<String,ConstIdentifier> _constVarsOut;

	private ArrayList<String> _updateInPlaceVars = null;
	private boolean _requiresRecompile = false;
	private boolean _splitDag = false;

	public StatementBlock() {
		_dmlProg = null;
		_statements = new ArrayList<>();
		_read = new VariableSet();
		_updated = new VariableSet();
		_gen = new VariableSet();
		_kill = new VariableSet();
		_warnSet = new VariableSet();
		_initialized = true;
		_constVarsIn = new HashMap<>();
		_constVarsOut = new HashMap<>();
		_updateInPlaceVars = new ArrayList<>();
	}
	
	public StatementBlock(StatementBlock sb) {
		this();
		setParseInfo(sb);
		_dmlProg = sb._dmlProg;
	}

	public void setDMLProg(DMLProgram dmlProg){
		_dmlProg = dmlProg;
	}

	public DMLProgram getDMLProg(){
		return _dmlProg;
	}

	public void addStatement(Statement s){
		_statements.add(s);

		if (_statements.size() == 1){
			this._filename      = s.getFilename();
			this._beginLine 	= s.getBeginLine();
			this._beginColumn 	= s.getBeginColumn();
		}

		this._endLine 		= s.getEndLine();
		this._endColumn		= s.getEndColumn();

	}

	public void addStatementBlock(StatementBlock s){
		for (int i = 0; i < s.getNumStatements(); i++){
			_statements.add(s.getStatement(i));
		}

		this._beginLine 	= _statements.get(0).getBeginLine();
		this._beginColumn 	= _statements.get(0).getBeginColumn();

		this._endLine 		= _statements.get(_statements.size() - 1).getEndLine();
		this._endColumn		= _statements.get(_statements.size() - 1).getEndColumn();
	}

	public int getNumStatements(){
		return _statements.size();
	}

	public Statement getStatement(int i){
		return _statements.get(i);
	}

	public ArrayList<Statement> getStatements() {
		return _statements;
	}

	public void setStatements( ArrayList<Statement> s )
	{
		_statements = s;
	}

	public ArrayList<Hop> getHops() {
		return _hops;
	}

	public ArrayList<Lop> getLops() {
		return _lops;
	}

	public void setHops(ArrayList<Hop> hops) {
		_hops = hops;
	}

	public void setLops(ArrayList<Lop> lops) {
		_lops = lops;
	}

	public boolean mergeable(){
		for (Statement s : _statements){
			if (s.controlStatement())
				return false;
		}
		return true;
	}
	
	public void setSplitDag(boolean flag) {
		_splitDag = flag;
	}
	
	public boolean isSplitDag() {
		return _splitDag;
	}

	private boolean isMergeablePrintStatement(Statement stmt) {
		return ( stmt instanceof PrintStatement && (((PrintStatement)stmt).getType() == PRINTTYPE.STOP || ((PrintStatement)stmt).getType() == PRINTTYPE.ASSERT) );
	}

    public boolean isMergeableFunctionCallBlock(DMLProgram dmlProg) {

//    	if (DMLScript.ENABLE_DEBUG_MODE && !DMLScript.ENABLE_DEBUG_OPTIMIZER)
    	if (DMLScript.ENABLE_DEBUG_MODE)
			return false;

		// check whether targetIndex stmt block is for a mergable function call
		Statement stmt = this.getStatement(0);

		// Check whether targetIndex block is: control stmt block or stmt block for un-mergable function call
		if (   stmt instanceof WhileStatement || stmt instanceof IfStatement || stmt instanceof ForStatement
			|| stmt instanceof FunctionStatement || isMergeablePrintStatement(stmt) /*|| stmt instanceof ELStatement*/ )
		{
			return false;
		}

		if (stmt instanceof AssignmentStatement || stmt instanceof MultiAssignmentStatement){
			Expression sourceExpr = null;
			if (stmt instanceof AssignmentStatement) {
				AssignmentStatement astmt = (AssignmentStatement)stmt;
				// for now, ensure that an assignment statement containing a read from csv ends up in own statement block
				if(astmt.getSource().toString().contains(DataExpression.FORMAT_TYPE + "=" + DataExpression.FORMAT_TYPE_VALUE_CSV) && astmt.getSource().toString().contains("read"))
					return false;
				sourceExpr = astmt.getSource();
			}
			else
				sourceExpr = ((MultiAssignmentStatement)stmt).getSource();
			if ( (sourceExpr instanceof BuiltinFunctionExpression && ((BuiltinFunctionExpression)sourceExpr).multipleReturns())
				|| (sourceExpr instanceof ParameterizedBuiltinFunctionExpression && ((ParameterizedBuiltinFunctionExpression)sourceExpr).multipleReturns()))
				return false;

			// function calls (only mergable if inlined dml-bodied function)
			if (sourceExpr instanceof FunctionCallIdentifier) {
				FunctionCallIdentifier fcall = (FunctionCallIdentifier) sourceExpr;
				FunctionStatementBlock fblock = dmlProg.getFunctionStatementBlock(fcall.getNamespace(),
						fcall.getName());
				if (fblock == null) {
					if (DMLProgram.DEFAULT_NAMESPACE.equals(fcall.getNamespace())) {
						throw new LanguageException(
								sourceExpr.printErrorLocation() + "Function " + fcall.getName() + "() is undefined.");
					} else {
						throw new LanguageException(sourceExpr.printErrorLocation() + "Function " + fcall.getName()
								+ "() is undefined in namespace '" + fcall.getNamespace() + "'.");
					}
				}
				if (!rIsInlineableFunction(fblock, dmlProg))
					return false;
			}
		}

		// regular function block
		return true;
	}

    public boolean isRewritableFunctionCall(Statement stmt, DMLProgram dmlProg) {

		// for regular stmt, check if this is a function call stmt block
		if (stmt instanceof AssignmentStatement || stmt instanceof MultiAssignmentStatement){
			Expression sourceExpr = null;
			if (stmt instanceof AssignmentStatement)
				sourceExpr = ((AssignmentStatement)stmt).getSource();
			else
				sourceExpr = ((MultiAssignmentStatement)stmt).getSource();

			if (sourceExpr instanceof FunctionCallIdentifier){
				FunctionCallIdentifier fcall = (FunctionCallIdentifier) sourceExpr;
				FunctionStatementBlock fblock = dmlProg.getFunctionStatementBlock(fcall.getNamespace(),fcall.getName());
				if (fblock == null) {
					throw new LanguageException(sourceExpr.printErrorLocation() + "function " + fcall.getName() + " is undefined in namespace " + fcall.getNamespace());
				}

				//check for unsupported target indexed identifiers (for consistent error handling)
				if( stmt instanceof AssignmentStatement
					&& ((AssignmentStatement)stmt).getTarget() instanceof IndexedIdentifier ) {
					return false;
				}

				//check if function can be inlined
				if( rIsInlineableFunction(fblock, dmlProg) ) {
					return true;
				}
			}
		}

		// regular statement
		return false;
	}


    private boolean rIsInlineableFunction( FunctionStatementBlock fblock, DMLProgram prog )
    {
    	boolean ret = true;

    	//reject external functions and function bodies with multiple blocks
    	if(    fblock.getStatements().isEmpty() //empty blocks
    		|| fblock.getStatement(0) instanceof ExternalFunctionStatement
    		|| ((FunctionStatement)fblock.getStatement(0)).getBody().size() > 1 )
    	{
			return false;
		}

    	//reject control flow and non-inlinable functions
    	if(!fblock.getStatements().isEmpty() && !((FunctionStatement)fblock.getStatement(0)).getBody().isEmpty())
    	{
    		StatementBlock stmtBlock = ((FunctionStatement)fblock.getStatement(0)).getBody().get(0);

    		//reject control flow blocks
        	if (stmtBlock instanceof IfStatementBlock || stmtBlock instanceof WhileStatementBlock || stmtBlock instanceof ForStatementBlock)
				 return false;

        	//recursively check that functions are inlinable
        	for( Statement s : stmtBlock.getStatements() ){
        		if( s instanceof AssignmentStatement && ((AssignmentStatement)s).getSource() instanceof FunctionCallIdentifier )
        		{
        			AssignmentStatement as = (AssignmentStatement)s;
        			FunctionCallIdentifier fcall = (FunctionCallIdentifier) as.getSource();
    				FunctionStatementBlock fblock2 = prog.getFunctionStatementBlock(fcall.getNamespace(), fcall.getName());
    				ret &= rIsInlineableFunction(fblock2, prog);
    				if( as.getSource().toString().contains(DataExpression.FORMAT_TYPE + "=" + DataExpression.FORMAT_TYPE_VALUE_CSV) && as.getSource().toString().contains("read"))
    					return false;

    				if( !ret ) return false;
        		}
        		if( s instanceof MultiAssignmentStatement && ((MultiAssignmentStatement)s).getSource() instanceof FunctionCallIdentifier )
        		{
        			FunctionCallIdentifier fcall = (FunctionCallIdentifier) ((MultiAssignmentStatement)s).getSource();
    				FunctionStatementBlock fblock2 = prog.getFunctionStatementBlock(fcall.getNamespace(), fcall.getName());
    				ret &= rIsInlineableFunction(fblock2, prog);
    				if( !ret ) return false;
        		}
        	}
		}

    	return ret;
    }

	public static ArrayList<StatementBlock> mergeFunctionCalls(ArrayList<StatementBlock> body, DMLProgram dmlProg) 
	{
		for(int i = 0; i <body.size(); i++){

			StatementBlock currBlock = body.get(i);

			// recurse to children function statement blocks
			if (currBlock instanceof WhileStatementBlock){
				WhileStatement wstmt = (WhileStatement)((WhileStatementBlock)currBlock).getStatement(0);
				wstmt.setBody(mergeFunctionCalls(wstmt.getBody(),dmlProg));
			}

			else if (currBlock instanceof ForStatementBlock){
				ForStatement fstmt = (ForStatement)((ForStatementBlock)currBlock).getStatement(0);
				fstmt.setBody(mergeFunctionCalls(fstmt.getBody(),dmlProg));
			}

			else if (currBlock instanceof IfStatementBlock){
				IfStatement ifstmt = (IfStatement)((IfStatementBlock)currBlock).getStatement(0);
				ifstmt.setIfBody(mergeFunctionCalls(ifstmt.getIfBody(),dmlProg));
				ifstmt.setElseBody(mergeFunctionCalls(ifstmt.getElseBody(),dmlProg));
			}

			else if (currBlock instanceof FunctionStatementBlock){
				FunctionStatement functStmt = (FunctionStatement)((FunctionStatementBlock)currBlock).getStatement(0);
				functStmt.setBody(mergeFunctionCalls(functStmt.getBody(),dmlProg));
			}
		}

		ArrayList<StatementBlock> result = new ArrayList<>();
		StatementBlock currentBlock = null;

		for (int i = 0; i < body.size(); i++){
			StatementBlock current = body.get(i);
			if (current.isMergeableFunctionCallBlock(dmlProg)){
				if (currentBlock != null) {
					currentBlock.addStatementBlock(current);
				} else {
					currentBlock = current;
				}
			} else {
				if (currentBlock != null) {
					result.add(currentBlock);
				}
				result.add(current);
				currentBlock = null;
			}
		}

		if (currentBlock != null) {
			result.add(currentBlock);
		}

		return result;
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("statements\n");
		for (Statement s : _statements){
			sb.append(s);
			sb.append("\n");
		}
		if (_liveOut != null) sb.append("liveout " + _liveOut.toString() + "\n");
		if (_liveIn!= null) sb.append("livein " + _liveIn.toString()+ "\n");
		if (_gen != null && !_gen.getVariables().isEmpty()) sb.append("gen " + _gen.toString()+ "\n");
		if (_kill != null && !_kill.getVariables().isEmpty()) sb.append("kill " + _kill.toString()+ "\n");
		if (_read != null && !_read.getVariables().isEmpty()) sb.append("read " + _read.toString()+ "\n");
		if (_updated != null && !_updated.getVariables().isEmpty()) sb.append("updated " + _updated.toString()+ "\n");
		return sb.toString();
	}

	public static ArrayList<StatementBlock> mergeStatementBlocks(ArrayList<StatementBlock> sb){
		if (sb == null || sb.isEmpty())
			return new ArrayList<>();

		ArrayList<StatementBlock> result = new ArrayList<>();
		StatementBlock currentBlock = null;

		for (int i = 0; i < sb.size(); i++){
			StatementBlock current = sb.get(i);
			if (current.mergeable()){
				if (currentBlock != null) {
					currentBlock.addStatementBlock(current);
				} else {
					currentBlock = current;
				}
			} else {
				if (currentBlock != null) {
					result.add(currentBlock);
				}
				result.add(current);
				currentBlock = null;
			}
		}

		if (currentBlock != null) {
			result.add(currentBlock);
		}

		return result;

	}
	
	public static List<StatementBlock> rHoistFunctionCallsFromExpressions(StatementBlock current) {
		if (current instanceof FunctionStatementBlock) {
			FunctionStatementBlock fsb = (FunctionStatementBlock)current;
			FunctionStatement fstmt = (FunctionStatement)fsb.getStatement(0);
			ArrayList<StatementBlock> tmp = new ArrayList<>();
			for (StatementBlock sb : fstmt.getBody())
				tmp.addAll(rHoistFunctionCallsFromExpressions(sb));
			fstmt.setBody(tmp);
		}
		else if (current instanceof WhileStatementBlock) {
			WhileStatementBlock wsb = (WhileStatementBlock) current;
			WhileStatement wstmt = (WhileStatement)wsb.getStatement(0);
			//TODO handle predicates
			ArrayList<StatementBlock> tmp = new ArrayList<>();
			for (StatementBlock sb : wstmt.getBody())
				tmp.addAll(rHoistFunctionCallsFromExpressions(sb));
			wstmt.setBody(tmp);
		}
		else if (current instanceof IfStatementBlock) {
			IfStatementBlock isb = (IfStatementBlock) current;
			IfStatement istmt = (IfStatement)isb.getStatement(0);
			//TODO handle predicates
			ArrayList<StatementBlock> tmp = new ArrayList<>();
			for (StatementBlock sb : istmt.getIfBody())
				tmp.addAll(rHoistFunctionCallsFromExpressions(sb));
			istmt.setIfBody(tmp);
			if( istmt.getElseBody() != null && !istmt.getElseBody().isEmpty() ) {
				ArrayList<StatementBlock> tmp2 = new ArrayList<>();
				for (StatementBlock sb : istmt.getElseBody())
					tmp2.addAll(rHoistFunctionCallsFromExpressions(sb));
				istmt.setElseBody(tmp2);
			}
		}
		else if (current instanceof ForStatementBlock) { //incl parfor
			ForStatementBlock fsb = (ForStatementBlock) current;
			ForStatement fstmt = (ForStatement)fsb.getStatement(0);
			//TODO handle predicates
			ArrayList<StatementBlock> tmp = new ArrayList<>();
			for (StatementBlock sb : fstmt.getBody())
				tmp.addAll(rHoistFunctionCallsFromExpressions(sb));
			fstmt.setBody(tmp);
		}
		else { //generic (last-level)
			ArrayList<Statement> tmp = new ArrayList<>();
			for(Statement stmt : current.getStatements())
				tmp.addAll(rHoistFunctionCallsFromExpressions(stmt));
			if( current.getStatements().size() != tmp.size() )
				return createStatementBlocks(current, tmp);
		}
		return Arrays.asList(current);
	}

	public static List<Statement> rHoistFunctionCallsFromExpressions(Statement stmt) {
		ArrayList<Statement> tmp = new ArrayList<>();
		if( stmt instanceof AssignmentStatement ) {
			AssignmentStatement astmt = (AssignmentStatement)stmt;
			boolean ix = (astmt.getTargetList().get(0) instanceof IndexedIdentifier);
			rHoistFunctionCallsFromExpressions(astmt.getSource(), !ix, tmp);
			if( ix && astmt.getSource() instanceof FunctionCallIdentifier ) {
				AssignmentStatement lstmt = (AssignmentStatement) tmp.get(tmp.size()-1);
				astmt.setSource(copy(lstmt.getTarget()));
			}
		}
		else if( stmt instanceof MultiAssignmentStatement ) {
			MultiAssignmentStatement mstmt = (MultiAssignmentStatement)stmt;
			rHoistFunctionCallsFromExpressions(mstmt.getSource(), true, tmp);
		}
		else if( stmt instanceof PrintStatement ) {
			PrintStatement pstmt = (PrintStatement)stmt;
			for(int i=0; i<pstmt.expressions.size(); i++) {
				Expression lexpr = pstmt.getExpressions().get(i);
				rHoistFunctionCallsFromExpressions(lexpr, false, tmp);
				if( lexpr instanceof FunctionCallIdentifier ) {
					AssignmentStatement lstmt = (AssignmentStatement) tmp.get(tmp.size()-1);
					pstmt.getExpressions().set(i, copy(lstmt.getTarget()));
				}
			}
		}
		
		//most statements will be returned unchanged, while expressions with
		//function calls are split into potentially many statements
		List<Statement> ret = tmp.isEmpty() ? Arrays.asList(stmt) : tmp;
		if( !tmp.isEmpty() ) {
			for( Statement ltmp : tmp )
				ltmp.setParseInfo(stmt);
			tmp.add(stmt);
		}
		return ret;
	}
	
	public static Expression rHoistFunctionCallsFromExpressions(Expression expr, boolean root, ArrayList<Statement> tmp) {
		if( expr == null || expr instanceof ConstIdentifier )
			return expr; //do nothing
		if( expr instanceof BinaryExpression ) {
			BinaryExpression lexpr = (BinaryExpression) expr;
			lexpr.setLeft(rHoistFunctionCallsFromExpressions(lexpr.getLeft(), false, tmp));
			lexpr.setRight(rHoistFunctionCallsFromExpressions(lexpr.getRight(), false, tmp));
		}
		else if( expr instanceof RelationalExpression ) {
			RelationalExpression lexpr = (RelationalExpression) expr;
			lexpr.setLeft(rHoistFunctionCallsFromExpressions(lexpr.getLeft(), false, tmp));
			lexpr.setRight(rHoistFunctionCallsFromExpressions(lexpr.getRight(), false, tmp));
		}
		else if( expr instanceof BooleanExpression ) {
			BooleanExpression lexpr = (BooleanExpression) expr;
			lexpr.setLeft(rHoistFunctionCallsFromExpressions(lexpr.getLeft(), false, tmp));
			lexpr.setRight(rHoistFunctionCallsFromExpressions(lexpr.getRight(), false, tmp));
		}
		else if( expr instanceof BuiltinFunctionExpression ) {
			BuiltinFunctionExpression lexpr = (BuiltinFunctionExpression) expr;
			Expression[] clexpr = lexpr.getAllExpr();
			for( int i=0; i<clexpr.length; i++ )
				clexpr[i] = rHoistFunctionCallsFromExpressions(clexpr[i], false, tmp);
		}
		else if( expr instanceof ParameterizedBuiltinFunctionExpression ) {
			ParameterizedBuiltinFunctionExpression lexpr = (ParameterizedBuiltinFunctionExpression) expr;
			HashMap<String, Expression> clexpr = lexpr.getVarParams();
			for( String key : clexpr.keySet() )
				clexpr.put(key, rHoistFunctionCallsFromExpressions(clexpr.get(key), false, tmp));
		}
		else if( expr instanceof DataExpression ) {
			DataExpression lexpr = (DataExpression) expr;
			HashMap<String, Expression> clexpr = lexpr.getVarParams();
			for( String key : clexpr.keySet() )
				clexpr.put(key, rHoistFunctionCallsFromExpressions(clexpr.get(key), false, tmp));
		}
		else if( expr instanceof FunctionCallIdentifier ) {
			FunctionCallIdentifier fexpr = (FunctionCallIdentifier) expr;
			for( ParameterExpression pexpr : fexpr.getParamExprs() )
				pexpr.setExpr(rHoistFunctionCallsFromExpressions(pexpr.getExpr(), false, tmp));
			if( !root ) { //core hoisting
				String varname = StatementBlockRewriteRule.createCutVarName(true);
				DataIdentifier di = new DataIdentifier(varname);
				di.setDataType(fexpr.getDataType());
				di.setValueType(fexpr.getValueType());
				tmp.add(new AssignmentStatement(di, fexpr, di));
				return di;
			}
		}
		//note: all remaining expressions data identifiers remain unchanged
		return expr;
	}
	
	private static DataIdentifier copy(DataIdentifier di) {
		return new DataIdentifier(di);
	}
	
	private static List<StatementBlock> createStatementBlocks(StatementBlock sb, List<Statement> stmts) {
		List<StatementBlock> ret = new ArrayList<StatementBlock>();
		StatementBlock current = new StatementBlock(sb);
		for(Statement stmt : stmts) {
			current.addStatement(stmt);
			//cut the statement block after the current function
			if( stmt instanceof AssignmentStatement
				&& ((AssignmentStatement)stmt).getSource()
				instanceof FunctionCallIdentifier ) {
				ret.add(current);
				current = new StatementBlock(sb);
			}
		}
		if( current.getNumStatements() > 0 )
			ret.add(current);
		return ret;
	}
	
	public ArrayList<Statement> rewriteFunctionCallStatements (DMLProgram dmlProg, ArrayList<Statement> statements) {

		ArrayList<Statement> newStatements = new ArrayList<>();
		for (Statement current : statements){
			if (isRewritableFunctionCall(current, dmlProg)){

				Expression sourceExpr = null;
				if (current instanceof AssignmentStatement)
					sourceExpr = ((AssignmentStatement)current).getSource();
				else
					sourceExpr = ((MultiAssignmentStatement)current).getSource();

				FunctionCallIdentifier fcall = (FunctionCallIdentifier) sourceExpr;
				FunctionStatementBlock fblock = dmlProg.getFunctionStatementBlock(fcall.getNamespace(), fcall.getName());
				if (fblock == null){
					fcall.raiseValidateError("function " + fcall.getName() + " is undefined in namespace " + fcall.getNamespace(), false);
				}
				FunctionStatement fstmt = (FunctionStatement)fblock.getStatement(0);

				// recursive inlining (no memo required because update-inplace of function statement blocks, so no redundant inlining)
				if( rIsInlineableFunction(fblock, dmlProg) ){
					fstmt.getBody().get(0).setStatements(rewriteFunctionCallStatements(dmlProg, fstmt.getBody().get(0).getStatements()));
				}

				//MB: we cannot use the hash since multiple interleaved inlined functions should be independent.
				//String prefix = new Integer(fblock.hashCode()).toString() + "_";
				String prefix = _seq.getNextID() + "_";

				if (fstmt.getBody().size() > 1){
					sourceExpr.raiseValidateError("rewritable function can only have 1 statement block", false);
				}
				StatementBlock sblock = fstmt.getBody().get(0);

				if( fcall.getParamExprs().size() != fstmt.getInputParams().size() ) {
					sourceExpr.raiseValidateError("Wrong number of function input arguments: "+
						fcall.getParamExprs().size() + " found, but " + fstmt.getInputParams().size()+" expected.");
				}

				for (int i = 0; i < fstmt.getInputParams().size(); i++) {
					DataIdentifier currFormalParam = fstmt.getInputParams().get(i);
					Expression exp = fcall.getParamExprs().get(i).getExpr();
					bindScopeVariable(newStatements, exp, prefix, currFormalParam);
				}

				// copy the referenced global variables into function scope
				for (DataIdentifier v : fstmt.getScopeVariables()) {
					// create new assignment statement
					bindScopeVariable(newStatements, v, prefix, v);
				}

				for (Statement stmt : sblock._statements){

					// rewrite the statement to use the "rewritten" name
					Statement rewrittenStmt = stmt.rewriteStatement(prefix);
					newStatements.add(rewrittenStmt);
				}

				if (current instanceof AssignmentStatement) {
					if (fstmt.getOutputParams().size() == 0) {
						AssignmentStatement as = (AssignmentStatement) current;
						if ((as.getTargetList().size() == 1) && (as.getTargetList().get(0) != null)) {
							raiseValidateError("Function '" + fcall.getName()
									+ "' does not return a value but is assigned to " + as.getTargetList().get(0),
									true);
						}
					}
				} else if (current instanceof MultiAssignmentStatement) {
					if (fstmt.getOutputParams().size() == 0) {
						MultiAssignmentStatement mas = (MultiAssignmentStatement) current;
						raiseValidateError("Function '" + fcall.getName()
								+ "' does not return a value but is assigned to " + mas.getTargetList(), true);
					}
				}
				// handle the return values
				for (int i = 0; i < fstmt.getOutputParams().size(); i++){

					// get the target (return parameter from function)
					DataIdentifier currReturnParam = fstmt.getOutputParams().get(i);
					String newSourceName = prefix + currReturnParam.getName();
					DataIdentifier newSource = new DataIdentifier(currReturnParam);
					newSource.setName(newSourceName);

					// get binding
					DataIdentifier newTarget = null;
					if (current instanceof AssignmentStatement){
						if (i > 0) {
							fstmt.raiseValidateError("Assignment statement cannot return multiple values", false);
						}
						AssignmentStatement as = (AssignmentStatement) current;
						DataIdentifier targ = as.getTarget();
						if (targ == null) {
							Expression exp = as.getSource();
							FunctionCallIdentifier fci = (FunctionCallIdentifier) exp;
							String functionName = fci.getName();
							fstmt.raiseValidateError(functionName + " requires LHS value", false);
						} else {
							newTarget = new DataIdentifier(((AssignmentStatement)current).getTarget());
						}
					}
					else{
						newTarget = new DataIdentifier(((MultiAssignmentStatement)current).getTargetList().get(i));
					}

					//auto casting of inputs on inlining (always, redundant cast removed during Hop Rewrites)
					ValueType sourceVT = newSource.getValueType();
					if (newSource.getDataType() == DataType.SCALAR && sourceVT != ValueType.STRING) {
						newSource = new BuiltinFunctionExpression(
								BuiltinFunctionExpression.getValueTypeCastOperator(sourceVT),
								new Expression[] { newSource }, newTarget);
					}

					// create the assignment statement to bind the call parameter to formal parameter
					AssignmentStatement binding = new AssignmentStatement(newTarget, newSource, newTarget);

					newStatements.add(binding);
				}

			} // end if (isRewritableFunctionCall(current, dmlProg)

			else {
				newStatements.add(current);
			}
		}

		return newStatements;
	}

	private void bindScopeVariable(ArrayList<Statement> newStatements, Expression currCallParam, String prefix, DataIdentifier currFormalParam) {
		// create new assignment statement
		String newFormalParameterName = prefix + currFormalParam.getName();
		DataIdentifier newTarget = new DataIdentifier(currFormalParam);
		newTarget.setName(newFormalParameterName);

		//auto casting of inputs on inlining (if required)
		ValueType targetVT = newTarget.getValueType();
		if (newTarget.getDataType() == DataType.SCALAR && currCallParam.getOutput() != null
				&& targetVT != currCallParam.getOutput().getValueType() && targetVT != ValueType.STRING) {
			currCallParam = new BuiltinFunctionExpression(
					BuiltinFunctionExpression.getValueTypeCastOperator(targetVT),
					new Expression[] { currCallParam }, newTarget);
		}

		// create the assignment statement to bind the call parameter to formal parameter
		AssignmentStatement binding = new AssignmentStatement(newTarget, currCallParam, newTarget);
		newStatements.add(binding);
	}

	public VariableSet validate(DMLProgram dmlProg, VariableSet ids, HashMap<String, ConstIdentifier> constVars, boolean conditional)
	{
		_constVarsIn.putAll(constVars);
		_statements = rewriteFunctionCallStatements(dmlProg, _statements);
		_dmlProg = dmlProg;
		
		HashMap<String, ConstIdentifier> currConstVars = new HashMap<>(constVars);
		for (Statement current : _statements) {
			if (current instanceof OutputStatement) {
				OutputStatement os = (OutputStatement)current;
				// validate variable being written by output statement exists
				DataIdentifier target = (DataIdentifier)os.getIdentifier();
				if (ids.getVariable(target.getName()) == null) {
					//undefined variables are always treated unconditionally as error in order to prevent common script-level bugs
					raiseValidateError("Undefined Variable (" + target.getName() + ") used in statement", false, LanguageErrorCodes.INVALID_PARAMETERS);
				}
				if ( ids.getVariable(target.getName()).getDataType() == DataType.SCALAR) {
					boolean paramsOkay = true;
					for (String key : os.getSource().getVarParams().keySet()){
						if (! (key.equals(DataExpression.IO_FILENAME) || key.equals(DataExpression.FORMAT_TYPE)))
							paramsOkay = false;
					}
					if( !paramsOkay ) {
						raiseValidateError("Invalid parameters in write statement: " + os.toString(), conditional);
					}
				}

				Expression source = os.getSource();
				source.setOutput(target);
				source.validateExpression(ids.getVariables(), currConstVars, conditional);
				setStatementFormatType(os, conditional);
				target.setDimensionValueProperties(ids.getVariable(target.getName()));
			}
			else if (current instanceof AssignmentStatement){
				validateAssignmentStatement(current, dmlProg, ids, currConstVars, conditional);
			}
			else if (current instanceof MultiAssignmentStatement){
				validateMultiAssignmentStatement(current, dmlProg, ids, currConstVars, conditional);
			}
			else if(current instanceof ForStatement || current instanceof IfStatement || current instanceof WhileStatement ){
				raiseValidateError("control statement (WhileStatement, IfStatement, ForStatement) should not be in generic statement block. Likely a parsing error", conditional);
			}
			else if (current instanceof PrintStatement) {
				PrintStatement pstmt = (PrintStatement) current;
				List<Expression> expressions = pstmt.getExpressions();
				for (Expression expression : expressions) {
					expression.validateExpression(ids.getVariables(), currConstVars, conditional);
					if (expression.getOutput().getDataType() != Expression.DataType.SCALAR) {
						if (expression.getOutput().getDataType() == Expression.DataType.MATRIX) {
							pstmt.raiseValidateError("Print statements can only print scalars. To print a matrix, please wrap it in a toString() function.", conditional);
						} else {
							pstmt.raiseValidateError("Print statements can only print scalars.", conditional);
						}
					}
				}
			}
			// no work to perform for PathStatement or ImportStatement
			else if (current instanceof PathStatement){}
			else if (current instanceof ImportStatement){}
			else {
				raiseValidateError("cannot process statement of type " + current.getClass().getSimpleName(), conditional);
			}
		}
		_constVarsOut.putAll(currConstVars);
		return ids;
	}
	
	private void validateAssignmentStatement(Statement current, DMLProgram dmlProg, 
		VariableSet ids, HashMap<String, ConstIdentifier> currConstVars, boolean conditional) 
	{
		AssignmentStatement as = (AssignmentStatement)current;
		DataIdentifier target = as.getTarget();
		Expression source = as.getSource();
		
		// check if target is builtin constant
		if (target != null && BuiltinConstant.contains(target.getName())) {
			target.raiseValidateError(String.format(
				"Cannot assign a value to the builtin constant %s.", target.getName()), false);
		}
		
		if (source instanceof FunctionCallIdentifier) {
			((FunctionCallIdentifier) source).validateExpression(
				dmlProg, ids.getVariables(),currConstVars, conditional);
		}
		else { //all builtin functions and expressions
			if( target == null  )
				raiseValidateError("Missing variable assignment.", false);
			
			if( MLContextProxy.isActive() )
				MLContextProxy.setAppropriateVarsForRead(source, target._name);
			
			source.validateExpression(ids.getVariables(), currConstVars, conditional);
		}
		
		if (source instanceof DataExpression && ((DataExpression)source).getOpCode() == Expression.DataOp.READ)
			setStatementFormatType(as, conditional);
		
		// Handle const vars: (a) basic constant propagation, and (b) transitive constant propagation over assignments
		if (target != null) {
			currConstVars.remove(target.getName());
			if(source instanceof ConstIdentifier && !(target instanceof IndexedIdentifier)){ //basic
				currConstVars.put(target.getName(), (ConstIdentifier)source);
			}
			if( source instanceof DataIdentifier && !(target instanceof IndexedIdentifier) ){ //transitive
				DataIdentifier diSource = (DataIdentifier) source;
				if( currConstVars.containsKey(diSource.getName()) ){
					currConstVars.put(target.getName(), currConstVars.get(diSource.getName()));
				}
			}
		}
		
		if (source instanceof BuiltinFunctionExpression){
			BuiltinFunctionExpression bife = (BuiltinFunctionExpression)source;
			if (   bife.getOpCode() == Expression.BuiltinFunctionOp.NROW
				|| bife.getOpCode() == Expression.BuiltinFunctionOp.NCOL )
			{
				DataIdentifier id = (DataIdentifier)bife.getFirstExpr();
				DataIdentifier currVal = ids.getVariable(id.getName());
				if (currVal == null){
					//undefined variables are always treated unconditionally as error in order to prevent common script-level bugs
					bife.raiseValidateError("Undefined Variable (" + id.getName() + ") used in statement", false, LanguageErrorCodes.INVALID_PARAMETERS);
				}
				IntIdentifier intid = null;
				if (bife.getOpCode() == Expression.BuiltinFunctionOp.NROW) {
					intid = new IntIdentifier((currVal instanceof IndexedIdentifier)
							? ((IndexedIdentifier) currVal).getOrigDim1() : currVal.getDim1(), bife);
				} else {
					intid = new IntIdentifier((currVal instanceof IndexedIdentifier)
							? ((IndexedIdentifier) currVal).getOrigDim2() : currVal.getDim2(), bife);
				}
				
				// handle case when nrow / ncol called on variable with size unknown (dims == -1)
				//	--> const prop NOT possible
				if (intid.getValue() != -1)
					currConstVars.put(target.getName(), intid);
			}
		}
		if (target == null) {
			// function has no return value
		}
		// CASE: target NOT indexed identifier
		else if (!(target instanceof IndexedIdentifier)){
			target.setProperties(source.getOutput());
			if (source.getOutput() instanceof IndexedIdentifier)
				target.setDimensions(source.getOutput().getDim1(), source.getOutput().getDim2());
		}
		// CASE: target is indexed identifier
		else
		{
			// process the "target" being indexed
			DataIdentifier targetAsSeen = ids.getVariable(target.getName());
			if (targetAsSeen == null){
				target.raiseValidateError("cannot assign value to indexed identifier " + target.toString() + " without first initializing " + target.getName(), conditional);
			}
			target.setProperties(targetAsSeen);
			
			// process the expressions for the indexing
			if ( ((IndexedIdentifier)target).getRowLowerBound() != null  )
				((IndexedIdentifier)target).getRowLowerBound().validateExpression(ids.getVariables(), currConstVars, conditional);
			if ( ((IndexedIdentifier)target).getRowUpperBound() != null  )
				((IndexedIdentifier)target).getRowUpperBound().validateExpression(ids.getVariables(), currConstVars, conditional);
			if ( ((IndexedIdentifier)target).getColLowerBound() != null  )
				((IndexedIdentifier)target).getColLowerBound().validateExpression(ids.getVariables(), currConstVars, conditional);
			if ( ((IndexedIdentifier)target).getColUpperBound() != null  )
				((IndexedIdentifier)target).getColUpperBound().validateExpression(ids.getVariables(), currConstVars, conditional);
			
			// validate that size of LHS index ranges is being assigned:
			//	(a) a matrix value of same size as LHS
			//	(b) singleton value (semantics: initialize enitre submatrix with this value)
			IndexPair targetSize = ((IndexedIdentifier)target).calculateIndexedDimensions(ids.getVariables(), currConstVars, conditional);
			
			if (targetSize._row >= 1 && source.getOutput().getDim1() > 1 && targetSize._row != source.getOutput().getDim1()){
				target.raiseValidateError("Dimension mismatch. Indexed expression " + target.toString() + " can only be assigned matrix with dimensions "
						+ targetSize._row + " rows and " + targetSize._col + " cols. Attempted to assign matrix with dimensions "
						+ source.getOutput().getDim1() + " rows and " + source.getOutput().getDim2() + " cols ", conditional);
			}
			
			if (targetSize._col >= 1 && source.getOutput().getDim2() > 1 && targetSize._col != source.getOutput().getDim2()){
				target.raiseValidateError("Dimension mismatch. Indexed expression " + target.toString() + " can only be assigned matrix with dimensions "
						+ targetSize._row + " rows and " + targetSize._col + " cols. Attempted to assign matrix with dimensions "
						+ source.getOutput().getDim1() + " rows and " + source.getOutput().getDim2() + " cols ", conditional);
			}
			((IndexedIdentifier)target).setDimensions(targetSize._row, targetSize._col);
		}
		
		if (target != null)
			ids.addVariable(target.getName(), target);
	}
	
	private void validateMultiAssignmentStatement(Statement current, DMLProgram dmlProg, 
		VariableSet ids, HashMap<String, ConstIdentifier> currConstVars, boolean conditional) 
	{
		MultiAssignmentStatement mas = (MultiAssignmentStatement) current;
		ArrayList<DataIdentifier> targetList = mas.getTargetList();
		Expression source = mas.getSource();

		// check if target list contains builtin constant
		targetList.forEach(target -> {
			if (target != null && BuiltinConstant.contains(target.getName()))
				target.raiseValidateError(String.format(
					"Cannot assign a value to the builtin constant %s.", target.getName()), false);
		});
		
		//MultiAssignmentStatments currently supports only External,
		//User-defined, and Multi-return Builtin function expressions
		if (!(source instanceof DataIdentifier)
				|| (source instanceof DataIdentifier && !((DataIdentifier)source).multipleReturns()) ) {
			source.raiseValidateError("can only use user-defined functions with multi-assignment statement", conditional);
		}
		if ( source instanceof FunctionCallIdentifier) {
			FunctionCallIdentifier fci = (FunctionCallIdentifier)source;
			fci.validateExpression(dmlProg, ids.getVariables(), currConstVars, conditional);
		}
		else if ( (source instanceof BuiltinFunctionExpression || source instanceof ParameterizedBuiltinFunctionExpression)
				&& ((DataIdentifier)source).multipleReturns()) {
			source.validateExpression(mas, ids.getVariables(), currConstVars, conditional);
		}
		else
			throw new LanguageException("Unexpected error.");
		
		if ( source instanceof FunctionCallIdentifier ) {
			for (int j =0; j< targetList.size(); j++) {
				DataIdentifier target = targetList.get(j);
				// set target properties (based on type info in function call statement return params)
				FunctionCallIdentifier fci = (FunctionCallIdentifier)source;
				FunctionStatement fstmt = (FunctionStatement)_dmlProg
					.getFunctionStatementBlock(fci.getNamespace(), fci.getName()).getStatement(0);
				if (fstmt == null){
					fci.raiseValidateError(" function " + fci.getName() 
						+ " is undefined in namespace " + fci.getNamespace(), conditional);
				}
				if (!(target instanceof IndexedIdentifier)){
					target.setProperties(fstmt.getOutputParams().get(j));
				}
				else{
					DataIdentifier targetAsSeen = ids.getVariable(target.getName());
					if (targetAsSeen == null){
						raiseValidateError(target.printErrorLocation() + "cannot assign value to indexed identifier " 
							+ target.toString() + " without first initializing " + target.getName(), conditional);
					}
					target.setProperties(targetAsSeen);
				}
				ids.addVariable(target.getName(), target);
			}
		}
		else if ( source instanceof BuiltinFunctionExpression || source instanceof ParameterizedBuiltinFunctionExpression ) {
			Identifier[] outputs = source.getOutputs();
			for (int j=0; j < targetList.size(); j++) {
				ids.addVariable(targetList.get(j).getName(), (DataIdentifier)outputs[j]);
			}
		}
	}
	
	public void setStatementFormatType(OutputStatement s, boolean conditionalValidate)
	{
		//case of specified format parameter
		if (s.getExprParam(DataExpression.FORMAT_TYPE)!= null )
		{
	 		Expression formatTypeExpr = s.getExprParam(DataExpression.FORMAT_TYPE);
			if (!(formatTypeExpr instanceof StringIdentifier)){
				raiseValidateError("IO statement parameter " + DataExpression.FORMAT_TYPE
						+ " can only be a string with one of following values: binary, text, mm, csv.", false, LanguageErrorCodes.INVALID_PARAMETERS);
			}
			String ft = formatTypeExpr.toString();
			if (ft.equalsIgnoreCase(DataExpression.FORMAT_TYPE_VALUE_BINARY)){
				s.getIdentifier().setFormatType(FormatType.BINARY);
			} else if (ft.equalsIgnoreCase(DataExpression.FORMAT_TYPE_VALUE_TEXT)){
				s.getIdentifier().setFormatType(FormatType.TEXT);
			} else if (ft.equalsIgnoreCase(DataExpression.FORMAT_TYPE_VALUE_MATRIXMARKET)){
				s.getIdentifier().setFormatType(FormatType.MM);
			} else if (ft.equalsIgnoreCase(DataExpression.FORMAT_TYPE_VALUE_CSV)){
				s.getIdentifier().setFormatType(FormatType.CSV);
			} else{
				raiseValidateError("IO statement parameter " + DataExpression.FORMAT_TYPE
						+ " can only be a string with one of following values: binary, text, mm, csv; invalid format: '"+ft+"'.", false, LanguageErrorCodes.INVALID_PARAMETERS);
			}
		}
		//case of unspecified format parameter, use default
		else {
			s.addExprParam(DataExpression.FORMAT_TYPE, new StringIdentifier(FormatType.TEXT.toString(), s), true);
			s.getIdentifier().setFormatType(FormatType.TEXT);
		}
	}

	public void setStatementFormatType(AssignmentStatement s, boolean conditionalValidate)
	{

		if (!(s.getSource() instanceof DataExpression))
			return;
		DataExpression dataExpr = (DataExpression)s.getSource();

		if (dataExpr.getVarParam(DataExpression.FORMAT_TYPE)!= null ){

	 		Expression formatTypeExpr = dataExpr.getVarParam(DataExpression.FORMAT_TYPE);
			if (!(formatTypeExpr instanceof StringIdentifier)){
				raiseValidateError("IO statement parameter " + DataExpression.FORMAT_TYPE
						+ " can only be a string with one of following values: binary, text", conditionalValidate, LanguageErrorCodes.INVALID_PARAMETERS);
			}
			String ft = formatTypeExpr.toString();
			if (ft.equalsIgnoreCase(DataExpression.FORMAT_TYPE_VALUE_BINARY)){
				s.getTarget().setFormatType(FormatType.BINARY);
			} else if (ft.equalsIgnoreCase(DataExpression.FORMAT_TYPE_VALUE_TEXT)){
				s.getTarget().setFormatType(FormatType.TEXT);
			} else if (ft.equalsIgnoreCase(DataExpression.FORMAT_TYPE_VALUE_MATRIXMARKET)){
				s.getTarget().setFormatType(FormatType.MM);
			} else if (ft.equalsIgnoreCase(DataExpression.FORMAT_TYPE_VALUE_CSV)){
				s.getTarget().setFormatType(FormatType.CSV);
			} else{
				raiseValidateError("IO statement parameter " + DataExpression.FORMAT_TYPE
						+ " can only be a string with one of following values: binary, text, mm, csv", conditionalValidate, LanguageErrorCodes.INVALID_PARAMETERS);
			}
		} else {
			dataExpr.addVarParam(DataExpression.FORMAT_TYPE,
					new StringIdentifier(FormatType.TEXT.toString(), dataExpr));
			s.getTarget().setFormatType(FormatType.TEXT);
		}
	}


	/**
	 * For each statement:
	 *
	 * gen rule: for each variable read in current statement but not updated in any PRIOR statement, add to gen
	 * Handles case where variable both read and updated in same statement (i = i + 1, i needs to be added to gen)
	 *
	 * kill rule:  for each variable updated in current statement but not read in this or any PRIOR statement,
	 * add to kill.
	 *
	 */
	@Override
	public VariableSet initializeforwardLV(VariableSet activeIn) {

		for (Statement s : _statements){
			s.initializeforwardLV(activeIn);
			VariableSet read = s.variablesRead();
			VariableSet updated = s.variablesUpdated();

			if (s instanceof WhileStatement || s instanceof IfStatement || s instanceof ForStatement){
				raiseValidateError("control statement (while / for / if) cannot be in generic statement block", false);
			}

			if (read != null){
				// for each variable read in this statement but not updated in
				// 		any prior statement, add to sb._gen

				for (String var : read.getVariableNames()) {
					if (!_updated.containsVariable(var)) {
						_gen.addVariable(var, read.getVariable(var));
					}
				}
			}

			_read.addVariables(read);
			_updated.addVariables(updated);

			if (updated != null) {
				// for each updated variable that is not read
				for (String var : updated.getVariableNames())
				{
					//NOTE MB: always add updated vars to kill (in order to prevent side effects
					//of implicitly updated statistics over common data identifiers, propagated from
					//downstream operators to its inputs due to 'livein = gen \cup (liveout-kill))'.
					_kill.addVariable(var, _updated.getVariable(var));

					//if (!_read.containsVariable(var)) {
					//	_kill.addVariable(var, _updated.getVariable(var));
					//}
				}
			}
		}
		_liveOut = new VariableSet();
		_liveOut.addVariables(activeIn);
		_liveOut.addVariables(_updated);
		return _liveOut;
	}

	@Override
	public VariableSet initializebackwardLV(VariableSet loPassed) {
		int numStatements = _statements.size();
		VariableSet lo = new VariableSet(loPassed);
		for (int i = numStatements-1; i>=0; i--)
			lo = _statements.get(i).initializebackwardLV(lo);
		return new VariableSet(lo);
	}

	public HashMap<String, ConstIdentifier> getConstIn(){
		return _constVarsIn;
	}

	public HashMap<String, ConstIdentifier> getConstOut(){
		return _constVarsOut;
	}

	@Override
	public VariableSet analyze(VariableSet loPassed) {
		VariableSet candidateLO = new VariableSet();
		candidateLO.addVariables(loPassed);
		//candidateLO.addVariables(_gen);

		VariableSet origLiveOut = new VariableSet();
		origLiveOut.addVariables(_liveOut);

		_liveOut = new VariableSet();
	 	for (String name : candidateLO.getVariableNames()){
	 		if (origLiveOut.containsVariable(name)){
	 			_liveOut.addVariable(name, candidateLO.getVariable(name));
	 		}
	 	}

		initializebackwardLV(_liveOut);

		_liveIn = new VariableSet();
		_liveIn.addVariables(_liveOut);
		_liveIn.removeVariables(_kill);
		_liveIn.addVariables(_gen);

		VariableSet liveInReturn = new VariableSet();
		liveInReturn.addVariables(_liveIn);
		return liveInReturn;
	}

	///////////////////////////////////////////////////////////////
	// validate error handling (consistent for all expressions)

	public void raiseValidateError( String msg, boolean conditional ) {
		raiseValidateError(msg, conditional, null);
	}

	public void raiseValidateError( String msg, boolean conditional, String errorCode )
	{
		if( conditional )  //warning if conditional
		{
			String fullMsg = this.printWarningLocation() + msg;

			LOG.warn( fullMsg );
		}
		else  //error and exception if unconditional
		{
			String fullMsg = this.printErrorLocation() + msg;

			//LOG.error( fullMsg ); //no redundant error
			if( errorCode != null )
				throw new LanguageException( fullMsg, errorCode );
			else
				throw new LanguageException( fullMsg );
		}
	}

	///////////////////////////////////////////////////////////////////////////
	// store position information for statement blocks
	///////////////////////////////////////////////////////////////////////////
	private String _filename = "MAIN SCRIPT";
	private int _beginLine = 0, _beginColumn = 0;
	private int _endLine = 0, _endColumn = 0;
	private String _text;

	public void setFilename (String fname)  { _filename = fname;	}
	public void setBeginLine(int passed)    { _beginLine = passed;  }
	public void setBeginColumn(int passed) 	{ _beginColumn = passed; }
	public void setEndLine(int passed) 		{ _endLine = passed;   }
	public void setEndColumn(int passed)	{ _endColumn = passed; }
	public void setText(String text) { _text = text; }

	/**
	 * Set parse information.
	 *
	 * @param parseInfo
	 *            parse information, such as beginning line position, beginning
	 *            column position, ending line position, ending column position,
	 *            text, and filename
	 * @param filename
	 *            the DML/PYDML filename (if it exists)
	 */
	public void setParseInfo(ParseInfo parseInfo) {
		_beginLine = parseInfo.getBeginLine();
		_beginColumn = parseInfo.getBeginColumn();
		_endLine = parseInfo.getEndLine();
		_endColumn = parseInfo.getEndColumn();
		_text = parseInfo.getText();
		_filename = parseInfo.getFilename();
	}

	public String getFilename() { return _filename;	   }
	public int getBeginLine()	{ return _beginLine;   }
	public int getBeginColumn() { return _beginColumn; }
	public int getEndLine() 	{ return _endLine;   }
	public int getEndColumn()	{ return _endColumn; }
	public String getText() { return _text; }

	public String printErrorLocation(){
		return "ERROR: " + _filename + " -- line " + _beginLine + ", column " + _beginColumn + " -- ";
	}

	public String printBlockErrorLocation(){
		return "ERROR: "  + _filename + " -- statement block between lines " + _beginLine + " and " + _endLine + " -- ";
	}

	public String printWarningLocation(){
		return "WARNING: " + _filename + " -- line " + _beginLine + ", column " + _beginColumn + " -- ";
	}


	/////////
	// materialized hops recompilation / updateinplace flags
	////

	public boolean updateRecompilationFlag() {
		return (_requiresRecompile =
			ConfigurationManager.isDynamicRecompilation()
			&& Recompiler.requiresRecompilation(getHops()));
	}

	public boolean requiresRecompilation() {
		return _requiresRecompile;
	}

	public ArrayList<String> getUpdateInPlaceVars() {
		return _updateInPlaceVars;
	}

	public void setUpdateInPlaceVars( ArrayList<String> vars ) {
		_updateInPlaceVars = vars;
	}
}
