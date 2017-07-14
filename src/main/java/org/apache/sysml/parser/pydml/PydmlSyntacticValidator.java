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

package org.apache.sysml.parser.pydml;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringUtils;
import org.apache.sysml.conf.CompilerConfig.ConfigType;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.parser.AssignmentStatement;
import org.apache.sysml.parser.BinaryExpression;
import org.apache.sysml.parser.BuiltinFunctionExpression;
import org.apache.sysml.parser.ConditionalPredicate;
import org.apache.sysml.parser.DMLProgram;
import org.apache.sysml.parser.DataIdentifier;
import org.apache.sysml.parser.DoubleIdentifier;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.parser.ExternalFunctionStatement;
import org.apache.sysml.parser.ForStatement;
import org.apache.sysml.parser.FunctionCallIdentifier;
import org.apache.sysml.parser.FunctionStatement;
import org.apache.sysml.parser.IfStatement;
import org.apache.sysml.parser.ImportStatement;
import org.apache.sysml.parser.IndexedIdentifier;
import org.apache.sysml.parser.IntIdentifier;
import org.apache.sysml.parser.IterablePredicate;
import org.apache.sysml.parser.LanguageException;
import org.apache.sysml.parser.ParForStatement;
import org.apache.sysml.parser.ParameterExpression;
import org.apache.sysml.parser.ParseException;
import org.apache.sysml.parser.PathStatement;
import org.apache.sysml.parser.Statement;
import org.apache.sysml.parser.StatementBlock;
import org.apache.sysml.parser.StringIdentifier;
import org.apache.sysml.parser.WhileStatement;
import org.apache.sysml.parser.common.CommonSyntacticValidator;
import org.apache.sysml.parser.common.CustomErrorListener;
import org.apache.sysml.parser.common.ExpressionInfo;
import org.apache.sysml.parser.common.StatementInfo;
import org.apache.sysml.parser.pydml.PydmlParser.AddSubExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.AssignmentStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.AtomicExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.BooleanAndExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.BooleanNotExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.BooleanOrExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.BuiltinFunctionExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.CommandlineParamExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.CommandlinePositionExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.ConstDoubleIdExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.ConstFalseExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.ConstIntIdExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.ConstStringIdExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.ConstTrueExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.DataIdExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.DataIdentifierContext;
import org.apache.sysml.parser.pydml.PydmlParser.ElifBranchContext;
import org.apache.sysml.parser.pydml.PydmlParser.ExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.ExternalFunctionDefExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.ForStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.FunctionCallAssignmentStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.FunctionCallMultiAssignmentStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.FunctionStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.IfStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.IfdefAssignmentStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.IgnoreNewLineContext;
import org.apache.sysml.parser.pydml.PydmlParser.ImportStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.IndexedExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.InternalFunctionDefExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.IterableForStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.IterablePredicateColonExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.IterablePredicateSeqExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.MatrixDataTypeCheckContext;
import org.apache.sysml.parser.pydml.PydmlParser.Ml_typeContext;
import org.apache.sysml.parser.pydml.PydmlParser.ModIntDivExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.MultDivExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.ParForStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.ParameterizedExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.PathStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.PowerExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.ProgramrootContext;
import org.apache.sysml.parser.pydml.PydmlParser.RelationalExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.SimpleDataIdentifierExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.StatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.StrictParameterizedExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.StrictParameterizedKeyValueStringContext;
import org.apache.sysml.parser.pydml.PydmlParser.TypedArgNoAssignContext;
import org.apache.sysml.parser.pydml.PydmlParser.UnaryExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.ValueDataTypeCheckContext;
import org.apache.sysml.parser.pydml.PydmlParser.WhileStatementContext;

/**
 * TODO: Refactor duplicated parser code dml/pydml (entire package).
 *
 */
public class PydmlSyntacticValidator extends CommonSyntacticValidator implements PydmlListener {

	public PydmlSyntacticValidator(CustomErrorListener errorListener, Map<String,String> argVals, String sourceNamespace, Set<String> prepFunctions) {
		super(errorListener, argVals, sourceNamespace, prepFunctions);
	}

	@Override public String namespaceResolutionOp() { return "."; }
	@Override public String trueStringLiteral() { return "True"; }
	@Override public String falseStringLiteral() { return "False"; }

	protected ArrayList<ParameterExpression> getParameterExpressionList(List<ParameterizedExpressionContext> paramExprs) {
		ArrayList<ParameterExpression> retVal = new ArrayList<ParameterExpression>();
		for(ParameterizedExpressionContext ctx : paramExprs) {
			String paramName = null;
			if(ctx.paramName != null && ctx.paramName.getText() != null && !ctx.paramName.getText().isEmpty()) {
				paramName = ctx.paramName.getText();
			}
			ParameterExpression myArg = new ParameterExpression(paramName, ctx.paramVal.info.expr);
			retVal.add(myArg);
		}
		return retVal;
	}

	@Override
	public void enterEveryRule(ParserRuleContext arg0) {
		if(arg0 instanceof StatementContext) {
			if(((StatementContext) arg0).info == null) {
				((StatementContext) arg0).info = new StatementInfo();
			}
		}
		if(arg0 instanceof FunctionStatementContext) {
			if(((FunctionStatementContext) arg0).info == null) {
				((FunctionStatementContext) arg0).info = new StatementInfo();
			}
		}
		if(arg0 instanceof ExpressionContext) {
			if(((ExpressionContext) arg0).info == null) {
				((ExpressionContext) arg0).info = new ExpressionInfo();
			}
		}
		if(arg0 instanceof DataIdentifierContext) {
			if(((DataIdentifierContext) arg0).dataInfo == null) {
				((DataIdentifierContext) arg0).dataInfo = new ExpressionInfo();
			}
		}
	}


	// -----------------------------------------------------------------
	// 			Binary, Unary & Relational Expressions
	// -----------------------------------------------------------------

	// For now do no type checking, let validation handle it.
	// This way parser doesn't have to open metadata file
	@Override
	public void exitAddSubExpression(AddSubExpressionContext ctx) {
		binaryExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitModIntDivExpression(ModIntDivExpressionContext ctx) {
		String op = ctx.op.getText();
		String dmlOperator = "";
		if(op.equals("//")) {
			dmlOperator = "%/%";
		}
		else if(op.equals("%")) {
			dmlOperator = "%%";
		}
		else {
			notifyErrorListeners("Incorrect operator (expected // or %)", ctx.op);
			return;
		}
		binaryExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, dmlOperator);
	}

	@Override
	public void exitUnaryExpression(UnaryExpressionContext ctx) {
		unaryExpressionHelper(ctx, ctx.left.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitMultDivExpression(MultDivExpressionContext ctx) {
		binaryExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitPowerExpression(PowerExpressionContext ctx) {
		String dmlOperator = "";
		String op = ctx.op.getText();
		if(op.equals("**")) {
			dmlOperator = "^";
		}
		else {
			notifyErrorListeners("Incorrect operator (expected **)", ctx.op);
			return;
		}
		binaryExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, dmlOperator);
	}

	@Override
	public void exitRelationalExpression(RelationalExpressionContext ctx) {
		relationalExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitBooleanAndExpression(BooleanAndExpressionContext ctx) {
		String op = ctx.op.getText();
		String dmlOperator = "";
		if(op.equals("&") || op.equals("and")) {
			dmlOperator = "&";
		}
		else {
			notifyErrorListeners("Incorrect operator (expected &)", ctx.op);
			return;
		}
		booleanExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, dmlOperator);
	}

	@Override
	public void exitBooleanOrExpression(BooleanOrExpressionContext ctx) {
		String op = ctx.op.getText();
		String dmlOperator = "";
		if(op.equals("|") || op.equals("or")) {
			dmlOperator = "|";
		}
		else {
			notifyErrorListeners("Incorrect operator (expected |)", ctx.op);
			return;
		}
		booleanExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, dmlOperator);
	}

	@Override
	public void exitBooleanNotExpression(BooleanNotExpressionContext ctx) {
		unaryBooleanExpressionHelper(ctx, ctx.left.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitAtomicExpression(AtomicExpressionContext ctx) {
		ctx.info.expr = ctx.left.info.expr;
		setFileLineColumn(ctx.info.expr, ctx);
	}


	// -----------------------------------------------------------------
	// 			Constant Expressions
	// -----------------------------------------------------------------

	@Override
	public void exitConstFalseExpression(ConstFalseExpressionContext ctx) {
		booleanIdentifierHelper(ctx, false, ctx.info);
	}

	@Override
	public void exitConstTrueExpression(ConstTrueExpressionContext ctx) {
		booleanIdentifierHelper(ctx, true, ctx.info);
	}

	@Override
	public void exitConstDoubleIdExpression(ConstDoubleIdExpressionContext ctx) {
		constDoubleIdExpressionHelper(ctx, ctx.info);
	}

	@Override
	public void exitConstIntIdExpression(ConstIntIdExpressionContext ctx) {
		constIntIdExpressionHelper(ctx, ctx.info);
	}

	@Override
	public void exitConstStringIdExpression(ConstStringIdExpressionContext ctx) {
		constStringIdExpressionHelper(ctx, ctx.info);
	}


	// -----------------------------------------------------------------
	//          Identifier Based Expressions
	// -----------------------------------------------------------------

	@Override
	public void exitDataIdExpression(DataIdExpressionContext ctx) {
		exitDataIdExpressionHelper(ctx, ctx.info, ctx.dataIdentifier().dataInfo);
	}

	@Override
	public void exitSimpleDataIdentifierExpression(SimpleDataIdentifierExpressionContext ctx) {
		// This is either a function, or variable with namespace
		// By default, it assigns to a data type
		ctx.dataInfo.expr = new DataIdentifier(ctx.getText());
		setFileLineColumn(ctx.dataInfo.expr, ctx);
	}


	/**
	 * PyDML uses 0-based indexing, so we increment lower indices by 1
	 * when translating to DML.
	 *
	 * @param ctx the parse tree
	 */
	@Override
	public void exitIndexedExpression(IndexedExpressionContext ctx) {
		boolean isRowLower = (ctx.rowLower != null && !ctx.rowLower.isEmpty() && (ctx.rowLower.info.expr != null));
		boolean isRowUpper = (ctx.rowUpper != null && !ctx.rowUpper.isEmpty() && (ctx.rowUpper.info.expr != null));
		boolean isColLower = (ctx.colLower != null && !ctx.colLower.isEmpty() && (ctx.colLower.info.expr != null));
		boolean isColUpper = (ctx.colUpper != null && !ctx.colUpper.isEmpty() && (ctx.colUpper.info.expr != null));
		boolean isRowSliceImplicit = ctx.rowImplicitSlice != null;
		boolean isColSliceImplicit = ctx.colImplicitSlice != null;
        ExpressionInfo rowLower = isRowLower ? ctx.rowLower.info : null;
        ExpressionInfo rowUpper = isRowUpper ? ctx.rowUpper.info : null;
        ExpressionInfo colLower = isColLower ? ctx.colLower.info : null;
        ExpressionInfo colUpper = isColUpper ? ctx.colUpper.info : null;

		ctx.dataInfo.expr = new IndexedIdentifier(ctx.name.getText(), false, false);
		setFileLineColumn(ctx.dataInfo.expr, ctx);

		try {
			ArrayList< ArrayList<Expression> > exprList = new ArrayList< ArrayList<Expression> >();

			ArrayList<Expression> rowIndices = new ArrayList<Expression>();
			ArrayList<Expression> colIndices = new ArrayList<Expression>();


			if(!isRowLower && !isRowUpper) {
				// both not set
				rowIndices.add(null); rowIndices.add(null);
			}
			else if(isRowLower && isRowUpper) {
				// both set
				rowIndices.add(incrementByOne(rowLower.expr, ctx));
				rowIndices.add(rowUpper.expr);
			}
			else if(isRowLower && !isRowUpper) {
				// Add given lower bound
				rowIndices.add(incrementByOne(rowLower.expr, ctx));
				if(isRowSliceImplicit) {
					// Add expression for nrow(X) for implicit upper bound
					Expression.BuiltinFunctionOp bop = Expression.BuiltinFunctionOp.NROW;
					DataIdentifier x = new DataIdentifier(ctx.name.getText());
					int line = ctx.start.getLine();
					int col = ctx.start.getCharPositionInLine();
					Expression expr = new BuiltinFunctionExpression(bop, new Expression[]{x},
							currentFile, line, col, line, col);
					setFileLineColumn(expr, ctx);
					rowIndices.add(expr);
				}
			}
			else if(!isRowLower && isRowUpper && isRowSliceImplicit) {
				// Add expression for `1` for implicit lower bound
				// Note: We go ahead and increment by 1 to convert from 0-based to 1-based indexing
				int line = ctx.start.getLine();
				int col = ctx.start.getCharPositionInLine();
				IntIdentifier one = new IntIdentifier(1, currentFile, line, col, line, col);
				setFileLineColumn(one, ctx);
				rowIndices.add(one);

				// Add given upper bound
				rowIndices.add(rowUpper.expr);
			}
			else {
				notifyErrorListeners("incorrect index expression for row", ctx.start);
				return;
			}

			if(!isColLower && !isColUpper) {
				// both not set
				colIndices.add(null); colIndices.add(null);
			}
			else if(isColLower && isColUpper) {
				colIndices.add(incrementByOne(colLower.expr, ctx));
				colIndices.add(colUpper.expr);
			}
			else if(isColLower && !isColUpper) {
				// Add given lower bound
				colIndices.add(incrementByOne(colLower.expr, ctx));
				if(isColSliceImplicit) {
					// Add expression for ncol(X) for implicit upper bound
					Expression.BuiltinFunctionOp bop = Expression.BuiltinFunctionOp.NCOL;
					DataIdentifier x = new DataIdentifier(ctx.name.getText());
					int line = ctx.start.getLine();
					int col = ctx.start.getCharPositionInLine();
					Expression expr = new BuiltinFunctionExpression(bop, new Expression[]{x},
							currentFile, line, col, line, col);
					setFileLineColumn(expr, ctx);
					colIndices.add(expr);
				}
			}
			else if(!isColLower && isColUpper && isColSliceImplicit) {
				// Add expression for `1` for implicit lower bound
				// Note: We go ahead and increment by 1 to convert from 0-based to 1-based indexing
				int line = ctx.start.getLine();
				int col = ctx.start.getCharPositionInLine();
				IntIdentifier one = new IntIdentifier(1, currentFile, line, col, line, col);
				setFileLineColumn(one, ctx);
				colIndices.add(one);

				// Add given upper bound
				colIndices.add(colUpper.expr);
			}
			else {
				notifyErrorListeners("incorrect index expression for column", ctx.start);
				return;
			}
			exprList.add(rowIndices);
			exprList.add(colIndices);
			((IndexedIdentifier) ctx.dataInfo.expr).setIndices(exprList);
		}
		catch(Exception e) {
			notifyErrorListeners("cannot set the indices", ctx.start);
			return;
		}
	}

	/**
	 * Increment lower indices by 1 when translating from PyDML
	 * (0-based indexing) to DML (1-based indexing).
	 *
	 * @param expr expression 
	 * @param ctx antlr rule context
	 * @return expression
	 */
	private Expression incrementByOne(Expression expr, ParserRuleContext ctx) {
		// Addition and subtraction operator same as DML
		Expression.BinaryOp bop = Expression.getBinaryOp("+");
		Expression retVal = new BinaryExpression(bop);
		((BinaryExpression)retVal).setLeft(expr);
		int line = ctx.start.getLine();
		int col = ctx.start.getCharPositionInLine();
		((BinaryExpression)retVal).setRight(new DoubleIdentifier(1.0, currentFile, line, col, line, col));
		setFileLineColumn(retVal, ctx);
		return retVal;
	}

	// -----------------------------------------------------------------
	//          Command line parameters (begin with a '$')
	// -----------------------------------------------------------------

	@Override
	public void exitCommandlineParamExpression(CommandlineParamExpressionContext ctx) {
		handleCommandlineArgumentExpression(ctx);
	}

	@Override
	public void exitCommandlinePositionExpression(CommandlinePositionExpressionContext ctx) {
		handleCommandlineArgumentExpression(ctx);
	}


	private void handleCommandlineArgumentExpression(DataIdentifierContext ctx)
	{
		String varName = ctx.getText().trim();
		fillExpressionInfoCommandLineParameters(varName, ctx.dataInfo, ctx.start);

		if(ctx.dataInfo.expr == null) {
			if(!(ctx.parent instanceof IfdefAssignmentStatementContext)) {
				String msg = "The parameter " + varName + " either needs to be passed "
						+ "through commandline or initialized to default value.";
				if( ConfigurationManager.getCompilerConfigFlag(ConfigType.IGNORE_UNSPECIFIED_ARGS) ) {
					ctx.dataInfo.expr = getConstIdFromString(" ", ctx.start);
					if (!ConfigurationManager.getCompilerConfigFlag(ConfigType.MLCONTEXT)) {
						raiseWarning(msg, ctx.start);
					}
				}
				else {
					notifyErrorListeners(msg, ctx.start);
				}
			}
		}
	}

	// -----------------------------------------------------------------
	// 			"src" statment
	// -----------------------------------------------------------------

	@Override
	public void exitImportStatement(ImportStatementContext ctx)
	{
		//prepare import filepath
		String filePath = ctx.filePath.getText();
		String namespace = DMLProgram.DEFAULT_NAMESPACE;
		if(ctx.namespace != null && ctx.namespace.getText() != null && !ctx.namespace.getText().isEmpty()) {
			namespace = ctx.namespace.getText();
		}
		if((filePath.startsWith("\"") && filePath.endsWith("\"")) ||
				filePath.startsWith("'") && filePath.endsWith("'")) {
			filePath = filePath.substring(1, filePath.length()-1);
		}

		File file = new File(filePath);
		if (!file.isAbsolute()) {
			//concatenate working directory to filepath
			filePath = _workingDir + File.separator + filePath;
		}
		validateNamespace(namespace, filePath, ctx);
		String scriptID = DMLProgram.constructFunctionKey(namespace, filePath);

		DMLProgram prog = null;
		if (!_scripts.get().containsKey(scriptID))
		{
			_scripts.get().put(scriptID, namespace);
			try {
				prog = (new PyDMLParserWrapper()).doParse(filePath, null, getQualifiedNamespace(namespace), argVals);
			} catch (ParseException e) {
				notifyErrorListeners(e.getMessage(), ctx.start);
				return;
			}
	        // Custom logic whether to proceed ahead or not. Better than the current exception handling mechanism
			if(prog == null) {
				notifyErrorListeners("One or more errors found during importing a program from file " + filePath, ctx.start);
				return;
			}
			else {
				ctx.info.namespaces = new HashMap<String, DMLProgram>();
				ctx.info.namespaces.put(getQualifiedNamespace(namespace), prog);
				ctx.info.stmt = new ImportStatement();
				((ImportStatement) ctx.info.stmt).setCompletePath(filePath);
				((ImportStatement) ctx.info.stmt).setFilePath(ctx.filePath.getText());
				((ImportStatement) ctx.info.stmt).setNamespace(namespace);
			}
		}
		else
		{
			// Skip redundant parsing (to prevent potential infinite recursion) and
			// create empty program for this context to allow processing to continue.
			prog = new DMLProgram();
			ctx.info.namespaces = new HashMap<String, DMLProgram>();
			ctx.info.namespaces.put(getQualifiedNamespace(namespace), prog);
			ctx.info.stmt = new ImportStatement();
			((ImportStatement) ctx.info.stmt).setCompletePath(filePath);
			((ImportStatement) ctx.info.stmt).setFilePath(ctx.filePath.getText());
			((ImportStatement) ctx.info.stmt).setNamespace(namespace);
		}
	}

	// -----------------------------------------------------------------
	// 			Assignment Statement
	// -----------------------------------------------------------------

	@Override
	public void exitAssignmentStatement(AssignmentStatementContext ctx) {
		if(ctx.targetList == null) {
			notifyErrorListeners("incorrect parsing for assignment", ctx.start);
			return;
		}
		exitAssignmentStatementHelper(ctx, ctx.targetList.getText(), ctx.targetList.dataInfo, ctx.targetList.start, ctx.source.info, ctx.info);

	}


	// -----------------------------------------------------------------
	// 			Control Statements - Guards & Loops
	// -----------------------------------------------------------------

	/** Similar to the "axis" argument in numpy.
	 * @param ctx parameter expression
	 * @return 0 (along rows), 1 (along column) or -1 (for error)
	 */
	private int getAxis(ParameterExpression ctx) {
		if(ctx.getName() != null && ctx.getName() != null) {
			if(!ctx.getName().equals("axis")) {
				return -1;
			}
		}

		String val = ctx.getExpr().toString();
		if(val != null && val.equals("0")) {
			return 0;
		}
		else if(val != null && val.equals("1")) {
			return 1;
		}

		return -1;
	}

	// TODO : Clean up to use Map or some other structure
	private String getPythonAggFunctionNames(String functionName, int axis) {
		if(axis != 0 && axis != 1) {
			return functionName;
		}
		// axis=0 maps to column-wise computation and axis=1 maps to row-wise computation

		if(functionName.equals("sum")) {
			return axis == 0 ? "colSums" : "rowSums";
		}
		else if(functionName.equals("mean")) {
			return axis == 0 ? "colMeans" : "rowMeans";
		}
		else if(functionName.equals("var")) {
			return axis == 0 ? "colVars" : "rowVars";
		}
		else if(functionName.equals("sd")) {
			return axis == 0 ? "colSds" : "rowSds";
		}
		else if(functionName.equals("avg")) {
			return axis == 0 ? "colMeans" : "rowMeans";
		}
		else if(functionName.equals("max")) {
			return axis == 0 ? "colMaxs" : "rowMaxs";
		}
		else if(functionName.equals("min")) {
			return axis == 0 ? "colMins" : "rowMins";
		}
		else if(functionName.equals("argmin")) {
			return axis == 0 ? "Not Supported" : "rowIndexMin";
		}
		else if(functionName.equals("argmax")) {
			return axis == 0 ? "Not Supported" : "rowIndexMax";
		}
		else if(functionName.equals("cumsum")) {
			return axis == 0 ?  "cumsum" : "Not Supported";
		}
		else if(functionName.equals("transpose")) {
			return axis == 0 ?  "Not Supported" : "Not Supported";
		}
		else if(functionName.equals("trace")) {
			return axis == 0 ?  "Not Supported" : "Not Supported";
		}
		else {
			return functionName;
		}
	}

	@Override
	public ConvertedDMLSyntax convertToDMLSyntax(ParserRuleContext ctx, String namespace, String functionName, ArrayList<ParameterExpression> paramExpression, Token fnName) {
		return convertPythonBuiltinFunctionToDMLSyntax(ctx, namespace, functionName, paramExpression, fnName);
	}

	// TODO : Clean up to use Map or some other structure

	/**
	 * Check function name, namespace, parameters (#params & possible values) and produce useful messages/hints
	 * @param ctx antlr rule context
	 * @param namespace Namespace of the function
	 * @param functionName Name of the builtin function
	 * @param paramExpression Array of parameter names and values
	 * @param fnName Token of the builtin function identifier
	 * @return common syntax format for runtime
	 */
	private ConvertedDMLSyntax convertPythonBuiltinFunctionToDMLSyntax(ParserRuleContext ctx, String namespace, String functionName, ArrayList<ParameterExpression> paramExpression,
			Token fnName) {
		if (sources.containsValue(namespace) || functions.contains(functionName)) {
			return new ConvertedDMLSyntax(namespace, functionName, paramExpression);
		}

		String fileName = currentFile;
		int line = ctx.start.getLine();
		int col = ctx.start.getCharPositionInLine();

		if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("len")) {
			if(paramExpression.size() != 1) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts 1 arguments", fnName);
				return null;
			}
			functionName = "length";
		}
		else if(functionName.equals("sum") || functionName.equals("mean") || functionName.equals("avg") ||
				functionName.equals("min") || functionName.equals("max")  ||
				functionName.equals("argmax") || functionName.equals("argmin") ||
				functionName.equals("cumsum") || functionName.equals("transpose") || functionName.equals("trace") ||
				functionName.equals("var") || functionName.equals("sd")) {
			// 0 maps row-wise computation and 1 maps to column-wise computation

			// can mean sum of all cells or row-wise or columnwise sum
			if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && paramExpression.size() == 1) {
				// sum(x) => sum(x)
				// otherwise same function name
				if(functionName.equals("avg")) {
					functionName = "mean";
				}
				else if(functionName.equals("transpose")) {
					functionName = "t";
				}
				else if(functionName.equals("argmax") || functionName.equals("argmin") || functionName.equals("cumsum")) {
					notifyErrorListeners("The builtin function \'" + functionName + "\' for entire matrix is not supported", fnName);
					return null;
				}
			}
			else if(!(namespace.equals(DMLProgram.DEFAULT_NAMESPACE)) && paramExpression.size() == 0) {
				// x.sum() => sum(x)
				paramExpression = new ArrayList<ParameterExpression>();
				paramExpression.add(new ParameterExpression(null, new DataIdentifier(namespace)));
				// otherwise same function name
				if(functionName.equals("avg")) {
					functionName = "mean";
				}
				else if(functionName.equals("transpose")) {
					functionName = "t";
				}
				else if(functionName.equals("argmax") || functionName.equals("argmin") || functionName.equals("cumsum")) {
					notifyErrorListeners("The builtin function \'" + functionName + "\' for entire matrix is not supported", fnName);
					return null;
				}
			}
			else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && paramExpression.size() == 2) {
				// sum(x, axis=1) => rowSums(x)
				int axis = getAxis(paramExpression.get(1));
				if(axis == -1 && (functionName.equals("min") || functionName.equals("max") )) {
					// Do nothing
					// min(2, 3)
				}
				else if(axis == -1) {
					notifyErrorListeners("The builtin function \'" + functionName + "\' for given arguments is not supported", fnName);
					return null;
				}
				else {
					ArrayList<ParameterExpression> temp = new ArrayList<ParameterExpression>();
					temp.add(paramExpression.get(0));
					paramExpression = temp;
					functionName = getPythonAggFunctionNames(functionName, axis);
					if(functionName.equals("Not Supported")) {
						notifyErrorListeners("The builtin function \'" + functionName + "\' for given arguments is not supported", fnName);
						return null;
					}
				}
			}
			else if(!(namespace.equals(DMLProgram.DEFAULT_NAMESPACE)) && paramExpression.size() == 1) {
				// x.sum(axis=1) => rowSums(x)
				int axis = getAxis(paramExpression.get(0));
				 if(axis == -1) {
					 notifyErrorListeners("The builtin function \'" + functionName + "\' for given arguments is not supported", fnName);
					 return null;
				 }
				 else {
					 paramExpression = new ArrayList<ParameterExpression>();
					 paramExpression.add(new ParameterExpression(null, new DataIdentifier(namespace)));
					 functionName = getPythonAggFunctionNames(functionName, axis);
					 if(functionName.equals("Not Supported")) {
						 notifyErrorListeners("The builtin function \'" + functionName + "\' for given arguments is not supported", fnName);
						 return null;
					 }
				 }
			}
			else {
				notifyErrorListeners("Incorrect number of arguments for the builtin function \'" + functionName + "\'.", fnName);
				return null;
			}
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("concatenate")) {
			if(paramExpression.size() != 2) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts 2 arguments (Note: concatenate append columns of two matrices)", fnName);
				return null;
			}
			functionName = "append";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("minimum")) {
			if(paramExpression.size() != 2) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts 2 arguments", fnName);
				return null;
			}
			functionName = "min";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("maximum")) {
			if(paramExpression.size() != 2) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts 2 arguments", fnName);
				return null;
			}
			functionName = "max";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(!(namespace.equals(DMLProgram.DEFAULT_NAMESPACE)) && functionName.equals("shape")) {
			if(paramExpression.size() != 1) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts only 1 argument (0 or 1)", fnName);
				return null;
			}

			int axis = getAxis(paramExpression.get(0));
			if(axis == -1) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts only 1 argument (0 or 1)", fnName);
				return null;
			}
			paramExpression = new ArrayList<ParameterExpression>();
			paramExpression.add(new ParameterExpression(null, new DataIdentifier(namespace)));
			namespace = DMLProgram.DEFAULT_NAMESPACE;
			if(axis == 0) {
				functionName = "nrow";
			}
			else if(axis == 1) {
				functionName = "ncol";
			}
		}
		else if(namespace.equals("random") && functionName.equals("normal")) {
			if(paramExpression.size() != 3) {
				String qualifiedName = namespace + namespaceResolutionOp() + functionName;
				notifyErrorListeners("The builtin function \'" + qualifiedName + "\' accepts exactly 3 arguments (number of rows, number of columns, sparsity)", fnName);
				return null;
			}
			paramExpression.get(0).setName("rows");
			paramExpression.get(1).setName("cols");
			paramExpression.get(2).setName("sparsity");
			paramExpression.add(new ParameterExpression("pdf", new StringIdentifier("normal", fileName, line, col, line, col)));
			functionName = "rand";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals("random") && functionName.equals("poisson")) {
			if(paramExpression.size() != 4) {
				String qualifiedName = namespace + namespaceResolutionOp() + functionName;
				notifyErrorListeners("The builtin function \'" + qualifiedName + "\' accepts exactly 3 arguments (number of rows, number of columns, sparsity, lambda)", fnName);
				return null;
			}
			paramExpression.get(0).setName("rows");
			paramExpression.get(1).setName("cols");
			paramExpression.get(2).setName("sparsity");
			paramExpression.get(3).setName("lambda");
			paramExpression.add(new ParameterExpression("pdf", new StringIdentifier("poisson", fileName, line, col, line, col)));
			functionName = "rand";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals("random") && functionName.equals("uniform")) {
			if(paramExpression.size() != 5) {
				String qualifiedName = namespace + namespaceResolutionOp() + functionName;
				notifyErrorListeners("The builtin function \'" + qualifiedName + "\' accepts exactly 5 arguments (number of rows, number of columns, sparsity, min, max)", fnName);
				return null;
			}
			paramExpression.get(0).setName("rows");
			paramExpression.get(1).setName("cols");
			paramExpression.get(2).setName("sparsity");
			paramExpression.get(3).setName("min");
			paramExpression.get(4).setName("max");
			paramExpression.add(new ParameterExpression("pdf", new StringIdentifier("uniform", fileName, line, col, line, col)));
			functionName = "rand";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("full")) {
			if(paramExpression.size() != 3) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts exactly 3 arguments (constant float value, number of rows, number of columns)", fnName);
				return null;
			}
			paramExpression.get(1).setName("rows");
			paramExpression.get(2).setName("cols");
			functionName = "matrix";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("matrix")) {
			// This can either be string initializer or as.matrix function
			if(paramExpression.size() != 1) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts exactly 1 argument (either str or float value)", fnName);
				return null;
			}

			if(paramExpression.get(0).getExpr() instanceof StringIdentifier) {
				String initializerString = ((StringIdentifier)paramExpression.get(0).getExpr()).getValue().trim();
				if(!initializerString.startsWith("[") || !initializerString.endsWith("]")) {
					notifyErrorListeners("Incorrect initializer string for builtin function \'" + functionName + "\' (Eg: matrix(\"[1 2 3; 4 5 6]\"))", fnName);
					return null;
				}
				int rows = StringUtils.countMatches(initializerString, ";") + 1;

				// Make sure user doesnot have pretty string
				initializerString = initializerString.replaceAll("; ", ";");
				initializerString = initializerString.replaceAll(" ;", ";");
				initializerString = initializerString.replaceAll("\\[ ", "\\[");
				initializerString = initializerString.replaceAll(" \\]", "\\]");

				// Each row has ncol-1 spaces
				// #spaces = nrow * (ncol-1)
				// ncol = (#spaces / nrow) + 1
				int cols = (StringUtils.countMatches(initializerString, " ") / rows) + 1;

				initializerString = initializerString.replaceAll(";", " ");
				initializerString = initializerString.replaceAll("\\[", "");
				initializerString = initializerString.replaceAll("\\]", "");
				paramExpression = new ArrayList<ParameterExpression>();
				paramExpression.add(new ParameterExpression(null, new StringIdentifier(initializerString, fileName, line, col, line, col)));
				paramExpression.add(new ParameterExpression("rows", new IntIdentifier(rows, fileName, line, col, line, col)));
				paramExpression.add(new ParameterExpression("cols", new IntIdentifier(cols, fileName, line, col, line, col)));
			}
			else {
				functionName = "as.matrix";
			}
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("scalar")) {
			if(paramExpression.size() != 1) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts exactly 1 argument", fnName);
				return null;
			}
			functionName = "as.scalar";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("float")) {
			if(paramExpression.size() != 1) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts exactly 1 argument", fnName);
				return null;
			}
			functionName = "as.double";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("int")) {
			if(paramExpression.size() != 1) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts exactly 1 argument", fnName);
				return null;
			}
			functionName = "as.integer";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("bool")) {
			if(paramExpression.size() != 1) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts exactly 1 argument", fnName);
				return null;
			}
			functionName = "as.logical";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(!(namespace.equals(DMLProgram.DEFAULT_NAMESPACE)) && functionName.equals("reshape")) {
			if(paramExpression.size() != 2) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts exactly 2 arguments (number of rows, number of columns)", fnName);
				return null;
			}
			paramExpression.get(0).setName("rows");
			paramExpression.get(1).setName("cols");

			ArrayList<ParameterExpression> temp = new ArrayList<ParameterExpression>();
			temp.add(new ParameterExpression(null, new DataIdentifier(namespace)));
			temp.add(paramExpression.get(0));
			temp.add(paramExpression.get(1));
			paramExpression = temp;

			functionName = "matrix";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("removeEmpty")) {
			if(paramExpression.size() != 2) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts exactly 2 arguments (matrix, axis=0 or 1)", fnName);
				return null;
			}
			int axis = getAxis(paramExpression.get(1));
			if(axis == -1) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts exactly 2 arguments (matrix, axis=0 or 1)", fnName);
				return null;
			}
			StringIdentifier marginVal = null;
			if(axis == 0) {
				marginVal = new StringIdentifier("rows", fileName, line, col, line, col);
			}
			else {
				marginVal = new StringIdentifier("cols", fileName, line, col, line, col);
			}
			paramExpression.get(0).setName("target");
			paramExpression.get(1).setName("margin");
			paramExpression.get(1).setExpr(marginVal);
			functionName = "removeEmpty";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("replace")) {
			if(paramExpression.size() != 3) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts exactly 3 arguments (matrix, scalar value that should be replaced (pattern), scalar value (replacement))", fnName);
				return null;
			}
			paramExpression.get(0).setName("target");
			paramExpression.get(1).setName("pattern");
			paramExpression.get(2).setName("replacement");
			functionName = "replace";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("range")) {
			if (paramExpression.size() < 2) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts 3 arguments (from, to, increment), with the first 2 lacking default values", fnName);
				return null;
			} else if (paramExpression.size() > 3) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts 3 arguments (from, to, increment)", fnName);
			}
			functionName = "seq";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals("norm") && functionName.equals("cdf")) {
			if(paramExpression.size() != 3) {
				String qualifiedName = namespace + namespaceResolutionOp() + functionName;
				notifyErrorListeners("The builtin function \'" + qualifiedName + "\' accepts exactly 3 arguments (target, mean, sd)", fnName);
				return null;
			}
			functionName = "cdf";
			paramExpression.get(0).setName("target");
			paramExpression.get(1).setName("mean");
			paramExpression.get(2).setName("sd");
			paramExpression.add(new ParameterExpression("dist", new StringIdentifier("normal", fileName, line, col, line, col)));
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals("expon") && functionName.equals("cdf")) {
			if(paramExpression.size() != 2) {
				String qualifiedName = namespace + namespaceResolutionOp() + functionName;
				notifyErrorListeners("The builtin function \'" + qualifiedName + "\' accepts exactly 2 arguments (target, mean)", fnName);
				return null;
			}
			functionName = "cdf";
			paramExpression.get(0).setName("target");
			paramExpression.get(1).setName("mean");
			paramExpression.add(new ParameterExpression("dist", new StringIdentifier("exp", fileName, line, col, line, col)));
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals("chi") && functionName.equals("cdf")) {
			if(paramExpression.size() != 2) {
				String qualifiedName = namespace + namespaceResolutionOp() + functionName;
				notifyErrorListeners("The builtin function \'" + qualifiedName + "\' accepts exactly 2 arguments (target, df)", fnName);
				return null;
			}
			functionName = "cdf";
			paramExpression.get(0).setName("target");
			paramExpression.get(1).setName("df");
			paramExpression.add(new ParameterExpression("dist", new StringIdentifier("chisq", fileName, line, col, line, col)));
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals("f") && functionName.equals("cdf")) {
			if(paramExpression.size() != 3) {
				String qualifiedName = namespace + namespaceResolutionOp() + functionName;
				notifyErrorListeners("The builtin function \'" + qualifiedName + "\' accepts exactly 3 arguments (target, df1, df2)", fnName);
				return null;
			}
			functionName = "cdf";
			paramExpression.get(0).setName("target");
			paramExpression.get(1).setName("df1");
			paramExpression.get(2).setName("df2");
			paramExpression.add(new ParameterExpression("dist", new StringIdentifier("f", fileName, line, col, line, col)));
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals("t") && functionName.equals("cdf")) {
			if(paramExpression.size() != 2) {
				String qualifiedName = namespace + namespaceResolutionOp() + functionName;
				notifyErrorListeners("The builtin function \'" + qualifiedName + "\' accepts exactly 2 arguments (target, df)", fnName);
				return null;
			}
			functionName = "cdf";
			paramExpression.get(0).setName("target");
			paramExpression.get(1).setName("df");
			paramExpression.add(new ParameterExpression("dist", new StringIdentifier("t", fileName, line, col, line, col)));
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("percentile")) {
			if(paramExpression.size() != 2 && paramExpression.size() != 3) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts either 2 or 3 arguments", fnName);
				return null;
			}
			functionName = "quantile";
			namespace = DMLProgram.DEFAULT_NAMESPACE;
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("arcsin")) {
			functionName = "asin";
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("arccos")) {
			functionName = "acos";
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("arctan")) {
			functionName = "atan";
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("load")) {
			functionName = "read";
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("eigen")) {
			functionName = "eig";
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("power")) {
			if(paramExpression.size() != 2) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts exactly 2 arguments", fnName);
				return null;
			}
		}
		else if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE) && functionName.equals("dot")) {
			if(paramExpression.size() != 2) {
				notifyErrorListeners("The builtin function \'" + functionName + "\' accepts exactly 2 arguments", fnName);
				return null;
			}
		}

		return new ConvertedDMLSyntax(namespace, functionName, paramExpression);
	}

	/**
	 * For Pydml, matrix multiply is invoked using dot (A, B). This is taken from numpy.dot
	 * For Dml, it is invoked using "%*%". The dot function call in pydml is converted to a
	 * {@link BinaryExpression} equivalent to what is done in
	 * DmlSyntacticValidator's exitMatrixMulExpression(MatrixMulExpressionContext).
	 */
	@Override
	protected Expression handleLanguageSpecificFunction(ParserRuleContext ctx, String functionName, ArrayList<ParameterExpression> paramExpression){
		if(functionName.equals("dot") && paramExpression.size() == 2) {
			Expression.BinaryOp bop = Expression.getBinaryOp("%*%");
			Expression expr = new BinaryExpression(bop);
			((BinaryExpression)expr).setLeft(paramExpression.get(0).getExpr());
			((BinaryExpression)expr).setRight(paramExpression.get(1).getExpr());
			return expr;
		}
		return null;
	}

	@Override
	public void exitFunctionCallAssignmentStatement(FunctionCallAssignmentStatementContext ctx) {

		Set<String> printStatements = new  HashSet<String>();
		printStatements.add("print");
		printStatements.add("stop");

		Set<String> outputStatements = new HashSet<String>();
		outputStatements.add("save");

		String[] fnNames = getQualifiedNames(ctx.name.getText());
		if(fnNames == null) {
			String errorMsg = "incorrect function name (only namespace " + namespaceResolutionOp() + " functionName allowed. Hint: If you are trying to use builtin functions, you can skip the namespace)";
			notifyErrorListeners(errorMsg, ctx.name);
			return;
		}
		String namespace = fnNames[0];
		String functionName = fnNames[1];
		ArrayList<ParameterExpression> paramExpression = getParameterExpressionList(ctx.paramExprs);

		castAsScalarDeprecationCheck(functionName, ctx);

		boolean hasLHS = ctx.targetList != null;
		functionCallAssignmentStatementHelper(ctx, printStatements, outputStatements, hasLHS ? ctx.targetList.dataInfo.expr : null, ctx.info, ctx.name,
	 			hasLHS ? ctx.targetList.start : null, namespace, functionName, paramExpression, hasLHS);
	}

	// TODO: remove this when castAsScalar has been removed from DML/PYDML
	private void castAsScalarDeprecationCheck(String functionName, ParserRuleContext ctx) {
		if ("castAsScalar".equalsIgnoreCase(functionName)) {
			raiseWarning("castAsScalar() has been deprecated. Please use scalar().", ctx.start);
		}
	}

	@Override
	public void exitBuiltinFunctionExpression(BuiltinFunctionExpressionContext ctx) {
		// Double verification: verify passed function name is a (non-parameterized) built-in function.
		String[] names = getQualifiedNames(ctx.name.getText());
		if(names == null) {
			notifyErrorListeners("incorrect function name (only namespace " + namespaceResolutionOp() + " functionName allowed. Hint: If you are trying to use builtin functions, you can skip the namespace)", ctx.name);
			return;
		}
		String namespace = names[0];
		String functionName = names[1];

		ArrayList<ParameterExpression> paramExpression = getParameterExpressionList(ctx.paramExprs);

		castAsScalarDeprecationCheck(functionName, ctx);

		ConvertedDMLSyntax convertedSyntax = convertToDMLSyntax(ctx, namespace, functionName, paramExpression, ctx.name);
		if(convertedSyntax == null) {
			return;
		}
		else {
			functionName = convertedSyntax.functionName;
			paramExpression = convertedSyntax.paramExpression;
		}

		final ExpressionInfo info = ctx.info;
		Action f = new Action() {
			@Override public void execute(Expression e) { info.expr = e; }
		};
		boolean validBIF = buildForBuiltInFunction(ctx, functionName, paramExpression, f);
		if (validBIF)
			return;

		notifyErrorListeners("only builtin functions allowed as part of expression", ctx.start);
	}

	@Override
	public void exitFunctionCallMultiAssignmentStatement(
			FunctionCallMultiAssignmentStatementContext ctx) {
		String[] names = getQualifiedNames(ctx.name.getText());
		if(names == null) {
			notifyErrorListeners("incorrect function name (only namespace.functionName allowed. Hint: If you are trying to use builtin functions, you can skip the namespace)", ctx.name);
			return;
		}
		String namespace = names[0];
		String functionName = names[1];

		ArrayList<ParameterExpression> paramExpression = getParameterExpressionList(ctx.paramExprs);
		ConvertedDMLSyntax convertedSyntax = convertToDMLSyntax(ctx, namespace, functionName, paramExpression, ctx.name);
		if(convertedSyntax == null) {
			return;
		}
		else {
			namespace = convertedSyntax.namespace;
			functionName = convertedSyntax.functionName;
			paramExpression = convertedSyntax.paramExpression;
		}

		// No need to support dot() function since it will never return multi-assignment function

		FunctionCallIdentifier functCall = new FunctionCallIdentifier(paramExpression);
		functCall.setFunctionName(functionName);
		functCall.setFunctionNamespace(namespace);

		final ArrayList<DataIdentifier> targetList = new ArrayList<DataIdentifier>();
		for(DataIdentifierContext dataCtx : ctx.targetList) {
			if(dataCtx.dataInfo.expr instanceof DataIdentifier) {
				targetList.add((DataIdentifier) dataCtx.dataInfo.expr);
			}
			else {
				notifyErrorListeners("incorrect type for variable ", dataCtx.start);
				return;
			}
		}

		if(namespace.equals(DMLProgram.DEFAULT_NAMESPACE)) {
			final FunctionCallMultiAssignmentStatementContext fctx = ctx;
			Action f = new Action() {
				@Override public void execute(Expression e) { setMultiAssignmentStatement(targetList, e, fctx, fctx.info); }
			};
			boolean validBIF = buildForBuiltInFunction(ctx, functionName, paramExpression, f);
			if (validBIF)
				return;
		}

		// Override default namespace for imported non-built-in function
		String inferNamespace = (sourceNamespace != null && sourceNamespace.length() > 0 && DMLProgram.DEFAULT_NAMESPACE.equals(namespace)) ? sourceNamespace : namespace;
		functCall.setFunctionNamespace(inferNamespace);

		setMultiAssignmentStatement(targetList, functCall, ctx, ctx.info);
	}


	// -----------------------------------------------------------------
	// 			Control Statements - Guards & Loops
	// -----------------------------------------------------------------

	private StatementBlock getStatementBlock(Statement current) {
		return PyDMLParserWrapper.getStatementBlock(current);
	}

	@Override
	public void exitIfStatement(IfStatementContext ctx) {
		IfStatement ifStmt = new IfStatement();
		ConditionalPredicate predicate = new ConditionalPredicate(ctx.predicate.info.expr);
		ifStmt.setConditionalPredicate(predicate);
		String fileName = currentFile;
		int line = ctx.start.getLine();
		int col = ctx.start.getCharPositionInLine();
		ifStmt.setAllPositions(fileName, line, col, line, col);

		if(ctx.ifBody.size() > 0) {
			for(StatementContext stmtCtx : ctx.ifBody) {
				ifStmt.addStatementBlockIfBody(getStatementBlock(stmtCtx.info.stmt));
			}
			ifStmt.mergeStatementBlocksIfBody();
		}

		IfStatement tailIfStmt = ifStmt;

		if(ctx.elifBranches.size() > 0) {
			for(ElifBranchContext elifCtx : ctx.elifBranches) {
				tailIfStmt.addStatementBlockElseBody(getStatementBlock(elifCtx.info.stmt));
				tailIfStmt = (IfStatement) elifCtx.info.stmt;
			}
		}

		if(ctx.elseBody.size() > 0) {
			for(StatementContext stmtCtx : ctx.elseBody) {
				tailIfStmt.addStatementBlockElseBody(getStatementBlock(stmtCtx.info.stmt));
			}
			tailIfStmt.mergeStatementBlocksElseBody();
		}

		ctx.info.stmt = ifStmt;
		setFileLineColumn(ctx.info.stmt, ctx);
	}

	@Override
	public void exitElifBranch(ElifBranchContext ctx) {
		IfStatement elifStmt = new IfStatement();
		ConditionalPredicate predicate = new ConditionalPredicate(ctx.predicate.info.expr);
		elifStmt.setConditionalPredicate(predicate);
		String fileName = currentFile;
		int line = ctx.start.getLine();
		int col = ctx.start.getCharPositionInLine();
		elifStmt.setAllPositions(fileName, line, col, line, col);

		if(ctx.elifBody.size() > 0) {
			for (StatementContext stmtCtx : ctx.elifBody) {
				elifStmt.addStatementBlockIfBody(getStatementBlock(stmtCtx.info.stmt));
			}
			elifStmt.mergeStatementBlocksIfBody();
		}

		ctx.info.stmt = elifStmt;
		setFileLineColumn(ctx.info.stmt, ctx);
	}

	@Override
	public void exitWhileStatement(WhileStatementContext ctx) {
		WhileStatement whileStmt = new WhileStatement();
		ConditionalPredicate predicate = new ConditionalPredicate(ctx.predicate.info.expr);
		whileStmt.setPredicate(predicate);
		int line = ctx.start.getLine();
		int col = ctx.start.getCharPositionInLine();
		whileStmt.setAllPositions(currentFile, line, col, line, col);

		if(ctx.body.size() > 0) {
			for(StatementContext stmtCtx : ctx.body) {
				whileStmt.addStatementBlock(getStatementBlock(stmtCtx.info.stmt));
			}
			whileStmt.mergeStatementBlocks();
		}

		ctx.info.stmt = whileStmt;
		setFileLineColumn(ctx.info.stmt, ctx);
	}
	
	@Override
	public void exitIterableForStatement(IterableForStatementContext ctx) {
		ForStatement forStmt = new ForStatement();
		int line = ctx.start.getLine();
		int col = ctx.start.getCharPositionInLine();
		
		if(ctx.iterVars != null && ctx.iterVars.size() > 0) {
			if(ctx.iterPreds != null && ctx.iterPreds.size() > 0) {
				ArrayList<DataIdentifier> iterVars = new ArrayList<DataIdentifier>();
				for(Token iterVar : ctx.iterVars) {
					iterVars.add(new DataIdentifier(iterVar.getText()));
				}
				ArrayList<Expression> iterPreds = new ArrayList<Expression>();
				for(ExpressionContext iterPred : ctx.iterPreds) {
					iterPreds.add(iterPred.info.expr);
				}
				if(iterVars.size() != iterPreds.size()) {
					notifyErrorListeners("invalid syntax: the number of iter variables should match: " + iterVars + " != " + iterPreds, ctx.start);
				}
				else if(iterPreds.size() == 0) {
					notifyErrorListeners("invalid syntax: at minimum, we allow iterating over 1 variables", ctx.start);
				}
				else if(iterPreds.size() >= 2) {
					notifyErrorListeners("invalid syntax: in current version, we allow iterating over only 1 variables", ctx.start);
					// notifyErrorListeners("invalid syntax: at maximum, we allow iterating over two variables", ctx.start);
				}
				else {
					HashMap<String, String> parForParamValues = null;
					forStmt.setAllPositions(currentFile, line, col, line, col);
					ArrayList<ParameterExpression> paramExpression = getParameterExpressionList(ctx.paramExprs);
					if(paramExpression != null && paramExpression.size() > 0 && !paramExpression.get(0).getName().equals("nrow")) {
						notifyErrorListeners("invalid syntax: expected \'nrow\' instead of " + paramExpression.get(0).getName(), ctx.start);
						return;
					}
					
					Expression incrementExpr = null;
					if(paramExpression == null || paramExpression.size() == 0) {
						incrementExpr = new IntIdentifier(1, currentFile, line, col, line, col);
					}
					else if(paramExpression.size() >= 2) {
						notifyErrorListeners("invalid syntax: more than 2 parameters not allowed in for", ctx.start);
						return;
					}
					else {
						incrementExpr = paramExpression.get(0).getExpr();
					}
					
					IterablePredicate predicate = null;
					if(iterPreds.size() == 1)
						predicate = new IterablePredicate(iterVars, iterPreds.get(0), new IntIdentifier(1, currentFile, line, col, line, col), incrementExpr, parForParamValues, currentFile, line, col, line, col, true);
					else if(iterPreds.size() == 2)
						predicate = new IterablePredicate(iterVars, iterPreds.get(0), iterPreds.get(1), incrementExpr, parForParamValues, currentFile, line, col, line, col, true);
					else
						notifyErrorListeners("invalid syntax: at maximum, we allow iterating over two variables", ctx.start);
					
					forStmt.setPredicate(predicate);
					
					if(ctx.body.size() > 0) {
						for(StatementContext stmtCtx : ctx.body) {
							forStmt.addStatementBlock(getStatementBlock(stmtCtx.info.stmt));
						}
						forStmt.mergeStatementBlocks();
					}
					ctx.info.stmt = forStmt;
					setFileLineColumn(ctx.info.stmt, ctx);
				}
			}
			else {
				notifyErrorListeners("invalid syntax: the matrices to be iterated on cannot be empty", ctx.start);
			}
		}
		else {
			notifyErrorListeners("invalid syntax: atleast one iteration variable required", ctx.start);
		}
		
	}

	@Override
	public void exitForStatement(ForStatementContext ctx) {
		ForStatement forStmt = new ForStatement();
		int line = ctx.start.getLine();
		int col = ctx.start.getCharPositionInLine();

		DataIdentifier iterVar = new DataIdentifier(ctx.iterVar.getText());
		ArrayList<DataIdentifier> iterVars = new ArrayList<DataIdentifier>(); 
		iterVars.add(iterVar);
		
		HashMap<String, String> parForParamValues = null;
		Expression incrementExpr = null; //1/-1
		if(ctx.iterPred.info.increment != null) {
			incrementExpr = ctx.iterPred.info.increment;
		}
		IterablePredicate predicate = new IterablePredicate(iterVars, ctx.iterPred.info.from, ctx.iterPred.info.to, incrementExpr, parForParamValues, currentFile, line, col, line, col, false);
		forStmt.setPredicate(predicate);

		if(ctx.body.size() > 0) {
			for(StatementContext stmtCtx : ctx.body) {
				forStmt.addStatementBlock(getStatementBlock(stmtCtx.info.stmt));
			}
			forStmt.mergeStatementBlocks();
		}
		ctx.info.stmt = forStmt;
		setFileLineColumn(ctx.info.stmt, ctx);
	}

	@Override
	public void exitParForStatement(ParForStatementContext ctx) {
		ParForStatement parForStmt = new ParForStatement();
		int line = ctx.start.getLine();
		int col = ctx.start.getCharPositionInLine();

		DataIdentifier iterVar = new DataIdentifier(ctx.iterVar.getText());
		ArrayList<DataIdentifier> iterVars = new ArrayList<DataIdentifier>(); 
		iterVars.add(iterVar);
		
		HashMap<String, String> parForParamValues = new HashMap<String, String>();
		if(ctx.parForParams != null && ctx.parForParams.size() > 0) {
			for(StrictParameterizedExpressionContext parForParamCtx : ctx.parForParams) {
				String paramVal = parForParamCtx.paramVal.getText();
				if( argVals.containsKey(paramVal) )
					paramVal = argVals.get(paramVal);
				parForParamValues.put(parForParamCtx.paramName.getText(), paramVal);
			}
		}

		Expression incrementExpr = null; //1/-1
		if( ctx.iterPred.info.increment != null ) {
			incrementExpr = ctx.iterPred.info.increment;
		}
		IterablePredicate predicate = new IterablePredicate(iterVars, ctx.iterPred.info.from, ctx.iterPred.info.to, incrementExpr, parForParamValues, currentFile, line, col, line, col, false);
		parForStmt.setPredicate(predicate);
		if(ctx.body.size() > 0) {
			for(StatementContext stmtCtx : ctx.body) {
				parForStmt.addStatementBlock(getStatementBlock(stmtCtx.info.stmt));
			}
			parForStmt.mergeStatementBlocks();
		}
		ctx.info.stmt = parForStmt;
		setFileLineColumn(ctx.info.stmt, ctx);
	}


	@Override
	public void exitIterablePredicateColonExpression(IterablePredicateColonExpressionContext ctx) {
		ctx.info.from = ctx.from.info.expr;
		ctx.info.to = ctx.to.info.expr;
		ctx.info.increment = null;
	}

	@Override
	public void exitIterablePredicateSeqExpression(IterablePredicateSeqExpressionContext ctx) {
		if(!ctx.ID().getText().equals("range")) {
			notifyErrorListeners("incorrect function:\'" + ctx.ID().getText() + "\'. expected \'range\'", ctx.start);
			return;
		}
		ctx.info.from = ctx.from.info.expr;
		ctx.info.to = ctx.to.info.expr;
		if(ctx.increment != null && ctx.increment.info != null)
			ctx.info.increment = ctx.increment.info.expr;
	}


	// -----------------------------------------------------------------
	// 				Internal & External Functions Definitions
	// -----------------------------------------------------------------

	private ArrayList<DataIdentifier> getFunctionParameters(List<TypedArgNoAssignContext> ctx) {
		ArrayList<DataIdentifier> retVal = new ArrayList<DataIdentifier>();
		for(TypedArgNoAssignContext paramCtx : ctx) {
			DataIdentifier dataId = new DataIdentifier(paramCtx.paramName.getText());
			String dataType = null;
			String valueType = null;

			if(paramCtx.paramType == null || paramCtx.paramType.dataType() == null
					|| paramCtx.paramType.dataType().getText() == null || paramCtx.paramType.dataType().getText().isEmpty()) {
				dataType = "scalar";
			}
			else {
				dataType = paramCtx.paramType.dataType().getText();
			}

			//check and assign data type
			checkValidDataType(dataType, paramCtx.start);
			if( dataType.equals("matrix") )
				dataId.setDataType(DataType.MATRIX);
			else if( dataType.equals("frame") )
				dataId.setDataType(DataType.FRAME);
			else if( dataType.equals("scalar") )
				dataId.setDataType(DataType.SCALAR);

			valueType = paramCtx.paramType.valueType().getText();
			if(valueType.equals("int")) {
				dataId.setValueType(ValueType.INT);
			}
			else if(valueType.equals("str")) {
				dataId.setValueType(ValueType.STRING);
			}
			else if(valueType.equals("bool")) {
				dataId.setValueType(ValueType.BOOLEAN);
			}
			else if(valueType.equals("float")) {
				dataId.setValueType(ValueType.DOUBLE);
			}
			else {
				notifyErrorListeners("invalid valuetype " + valueType, paramCtx.start);
				return null;
			}
			retVal.add(dataId);
		}
		return retVal;
	}

	@Override
	public void exitInternalFunctionDefExpression(InternalFunctionDefExpressionContext ctx) {
		FunctionStatement functionStmt = new FunctionStatement();

		ArrayList<DataIdentifier> functionInputs  = getFunctionParameters(ctx.inputParams);
		functionStmt.setInputParams(functionInputs);

		// set function outputs
		ArrayList<DataIdentifier> functionOutputs = getFunctionParameters(ctx.outputParams);
		functionStmt.setOutputParams(functionOutputs);

		// set function name
		functionStmt.setName(ctx.name.getText());
		
		if(ctx.body.size() > 0) {
			// handle function body
			// Create arraylist of one statement block
			ArrayList<StatementBlock> body = new ArrayList<StatementBlock>();
			for(StatementContext stmtCtx : ctx.body) {
				body.add(getStatementBlock(stmtCtx.info.stmt));
			}
			functionStmt.setBody(body);
			functionStmt.mergeStatementBlocks();
		}
		else {
			notifyErrorListeners("functions with no statements are not allowed", ctx.start);
			return;
		}

		ctx.info.stmt = functionStmt;
		setFileLineColumn(ctx.info.stmt, ctx);
		ctx.info.functionName = ctx.name.getText();
	}

	@Override
	public void exitExternalFunctionDefExpression(ExternalFunctionDefExpressionContext ctx) {
		ExternalFunctionStatement functionStmt = new ExternalFunctionStatement();

		ArrayList<DataIdentifier> functionInputs  = getFunctionParameters(ctx.inputParams);
		functionStmt.setInputParams(functionInputs);

		// set function outputs
		ArrayList<DataIdentifier> functionOutputs = getFunctionParameters(ctx.outputParams);
		functionStmt.setOutputParams(functionOutputs);

		// set function name
		functionStmt.setName(ctx.name.getText());
		
		// set other parameters
		HashMap<String, String> otherParams = new HashMap<String,String>();
		boolean atleastOneClassName = false;
		for(StrictParameterizedKeyValueStringContext otherParamCtx : ctx.otherParams){
			String paramName = otherParamCtx.paramName.getText();
			String val = "";
			String text = otherParamCtx.paramVal.getText();
			// First unquote the string
			if(	(text.startsWith("\"") && text.endsWith("\"")) ||
				(text.startsWith("\'") && text.endsWith("\'"))) {
				if(text.length() > 2) {
					val = text.substring(1, text.length()-1);
				}
				// Empty value allowed
			}
			else {
				notifyErrorListeners("the value of user parameter for external function should be of type str", ctx.start);
				return;
			}
			otherParams.put(paramName, val);
			if(paramName.equals("classname")) {
				atleastOneClassName = true;
			}
		}
		functionStmt.setOtherParams(otherParams);
		if(!atleastOneClassName) {
			notifyErrorListeners("the parameter \'className\' needs to be passed for defExternal", ctx.start);
			return;
		}

		ctx.info.stmt = functionStmt;
		setFileLineColumn(ctx.info.stmt, ctx);
		ctx.info.functionName = ctx.name.getText();
	}


	@Override
	public void exitPathStatement(PathStatementContext ctx) {
		PathStatement stmt = new PathStatement(ctx.pathValue.getText());
		String filePath = ctx.pathValue.getText();
		if((filePath.startsWith("\"") && filePath.endsWith("\"")) ||
				filePath.startsWith("'") && filePath.endsWith("'")) {
			filePath = filePath.substring(1, filePath.length()-1);
		}

		_workingDir = filePath;
		ctx.info.stmt = stmt;
	}

	@Override
	public void exitIfdefAssignmentStatement(IfdefAssignmentStatementContext ctx) {
		if(!ctx.commandLineParam.getText().startsWith("$")) {
			notifyErrorListeners("the first argument of ifdef function should be a commandline argument parameter (which starts with $)", ctx.commandLineParam.start);
			return;
		}

		if(ctx.targetList == null) {
			notifyErrorListeners("incorrect lvalue in ifdef function ", ctx.start);
			return;
		}
		String targetListText = ctx.targetList.getText();
		if(targetListText.startsWith("$")) {
			notifyErrorListeners("lhs of ifdef function cannot be a commandline parameters. Use local variable instead", ctx.start);
			return;
		}

		DataIdentifier target = null;
		if(ctx.targetList.dataInfo.expr instanceof DataIdentifier) {
			target = (DataIdentifier) ctx.targetList.dataInfo.expr;
			Expression source = null;
			if(ctx.commandLineParam.dataInfo.expr != null) {
				// Since commandline parameter is set
				// The check of following is done in fillExpressionInfoCommandLineParameters:
				// Command line param cannot be empty string
				// If you want to pass space, please quote it
				source = ctx.commandLineParam.dataInfo.expr;
			}
			else {
				source = ctx.source.info.expr;
			}

			int line = ctx.start.getLine();
			int col = ctx.start.getCharPositionInLine();
			try {
				ctx.info.stmt = new AssignmentStatement(target, source, line, col, line, col);
				setFileLineColumn(ctx.info.stmt, ctx);
			} catch (LanguageException e) {
				notifyErrorListeners("invalid assignment for ifdef function", ctx.targetList.start);
				return;
			}

		}
		else {
			notifyErrorListeners("incorrect lvalue in ifdef function ", ctx.targetList.start);
			return;
		}
	}

	@Override
	public void exitMatrixDataTypeCheck(MatrixDataTypeCheckContext ctx) {
		checkValidDataType(ctx.ID().getText(), ctx.start);
		
		//additional error handling (pydml-specific)
		String datatype = ctx.ID().getText();
		if(datatype.equals("Matrix"))
			notifyErrorListeners("incorrect datatype (Hint: use matrix instead of Matrix)", ctx.start);
		else if(datatype.equals("Frame"))
			notifyErrorListeners("incorrect datatype (Hint: use frame instead of Frame)", ctx.start);
		else if(datatype.equals("Scalar"))
			notifyErrorListeners("incorrect datatype (Hint: use scalar instead of Scalar)", ctx.start);
		else if( datatype.equals("int") || datatype.equals("str")
			|| datatype.equals("bool") || datatype.equals("float") ) {
			notifyErrorListeners("expected datatype but found a valuetype "
					+ "(Hint: use matrix, frame or scalar instead of " + datatype + ")", ctx.start);
		}
	}

	// -----------------------------------------------------------------
	//        PyDML Specific
	// -----------------------------------------------------------------

	@Override
	public void exitIgnoreNewLine(IgnoreNewLineContext ctx) {
		// Introduce empty StatementInfo
		// This is later ignored by PyDMLParserWrapper
		try {
			ctx.info.stmt = new AssignmentStatement(null, null, 0, 0, 0, 0);
			ctx.info.stmt.setEmptyNewLineStatement(true);
		} catch (LanguageException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void exitValueDataTypeCheck(ValueDataTypeCheckContext ctx) {
		if(		ctx.ID().getText().equals("int")
				|| ctx.ID().getText().equals("str")
				|| ctx.ID().getText().equals("bool")
				|| ctx.ID().getText().equals("float")
				) {
			// Do nothing
		}
		else if(ctx.ID().getText().equals("integer"))
			notifyErrorListeners("incorrect valuetype (Hint: use int instead of integer)", ctx.start);
		else if(ctx.ID().getText().equals("double"))
			notifyErrorListeners("incorrect valuetype (Hint: use float instead of double)", ctx.start);
		else if(ctx.ID().getText().equals("boolean"))
			notifyErrorListeners("incorrect valuetype (Hint: use bool instead of boolean)", ctx.start);
		else if(ctx.ID().getText().equals("string"))
			notifyErrorListeners("incorrect valuetype (Hint: use str instead of string)", ctx.start);
		else {
			notifyErrorListeners("incorrect valuetype (expected int, str, bool or float)", ctx.start);
		}
	}


	// -----------------------------------------------------------------
	// 			Not overridden
	// -----------------------------------------------------------------


	@Override public void visitTerminal(TerminalNode node) {}

	@Override public void visitErrorNode(ErrorNode node) {}

	@Override public void exitEveryRule(ParserRuleContext ctx) {}

	@Override public void enterModIntDivExpression(ModIntDivExpressionContext ctx) {}

	@Override public void enterExternalFunctionDefExpression(ExternalFunctionDefExpressionContext ctx) {}

	@Override public void enterBooleanNotExpression(BooleanNotExpressionContext ctx) {}

	@Override public void enterPowerExpression(PowerExpressionContext ctx) {}

	@Override public void enterInternalFunctionDefExpression(InternalFunctionDefExpressionContext ctx) {}

	@Override public void enterBuiltinFunctionExpression(BuiltinFunctionExpressionContext ctx) {}

	@Override public void enterConstIntIdExpression(ConstIntIdExpressionContext ctx) {}

	@Override public void enterAtomicExpression(AtomicExpressionContext ctx) {}

	@Override public void enterIfdefAssignmentStatement(IfdefAssignmentStatementContext ctx) {}

	@Override public void enterConstStringIdExpression(ConstStringIdExpressionContext ctx) {}

	@Override public void enterConstTrueExpression(ConstTrueExpressionContext ctx) {}

	@Override public void enterValueDataTypeCheck(ValueDataTypeCheckContext ctx) {}

	@Override public void enterParForStatement(ParForStatementContext ctx) {}

	@Override public void enterUnaryExpression(UnaryExpressionContext ctx) {}

	@Override public void enterImportStatement(ImportStatementContext ctx) {}

	@Override public void enterPathStatement(PathStatementContext ctx) {}

	@Override public void enterWhileStatement(WhileStatementContext ctx) {}

	@Override public void enterCommandlineParamExpression(CommandlineParamExpressionContext ctx) {}

	@Override public void enterFunctionCallAssignmentStatement(FunctionCallAssignmentStatementContext ctx) {}

	@Override public void enterAddSubExpression(AddSubExpressionContext ctx) {}

	@Override public void enterIfStatement(IfStatementContext ctx) {}

	@Override public void enterElifBranch(ElifBranchContext ctx) {}

	@Override public void enterIgnoreNewLine(IgnoreNewLineContext ctx) {}

	@Override public void enterConstDoubleIdExpression(ConstDoubleIdExpressionContext ctx) {}

	@Override public void enterMatrixDataTypeCheck(MatrixDataTypeCheckContext ctx) {}

	@Override public void enterCommandlinePositionExpression(CommandlinePositionExpressionContext ctx) {}

	@Override public void enterIterablePredicateColonExpression(IterablePredicateColonExpressionContext ctx) {}

	@Override public void enterAssignmentStatement(AssignmentStatementContext ctx) {}

	@Override public void enterMl_type(Ml_typeContext ctx) {}

	@Override public void exitMl_type(Ml_typeContext ctx) {}

	@Override public void enterBooleanAndExpression(BooleanAndExpressionContext ctx) {}

	@Override public void enterForStatement(ForStatementContext ctx) {}

	@Override public void enterRelationalExpression(RelationalExpressionContext ctx) {}

	@Override public void enterTypedArgNoAssign(TypedArgNoAssignContext ctx) {}

	@Override public void exitTypedArgNoAssign(TypedArgNoAssignContext ctx) {}

	@Override public void enterStrictParameterizedExpression(StrictParameterizedExpressionContext ctx) {}

	@Override public void exitStrictParameterizedExpression(StrictParameterizedExpressionContext ctx) {}

	@Override public void enterMultDivExpression(MultDivExpressionContext ctx) {}

	@Override public void enterConstFalseExpression(ConstFalseExpressionContext ctx) {}

	@Override public void enterStrictParameterizedKeyValueString(StrictParameterizedKeyValueStringContext ctx) {}

	@Override public void exitStrictParameterizedKeyValueString(StrictParameterizedKeyValueStringContext ctx) {}

	@Override public void enterProgramroot(ProgramrootContext ctx) {}

	@Override public void exitProgramroot(ProgramrootContext ctx) {}

	@Override public void enterDataIdExpression(DataIdExpressionContext ctx) {}

	@Override public void enterIndexedExpression(IndexedExpressionContext ctx) {}

	@Override public void enterParameterizedExpression(ParameterizedExpressionContext ctx) {}

	@Override public void exitParameterizedExpression(ParameterizedExpressionContext ctx) {}

	@Override public void enterFunctionCallMultiAssignmentStatement(FunctionCallMultiAssignmentStatementContext ctx) {}

	@Override public void enterIterablePredicateSeqExpression(IterablePredicateSeqExpressionContext ctx) {}

	@Override public void enterSimpleDataIdentifierExpression(SimpleDataIdentifierExpressionContext ctx) {}

	@Override public void enterBooleanOrExpression(BooleanOrExpressionContext ctx) {}

	@Override
	public void enterIterableForStatement(IterableForStatementContext ctx) { }
	
}
