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

import java.util.HashSet;
import java.util.Set;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.sysml.parser.common.CustomErrorListener;
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
import org.apache.sysml.parser.pydml.PydmlParser.ElifBranchContext;
import org.apache.sysml.parser.pydml.PydmlParser.ExternalFunctionDefExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.ForStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.FunctionCallAssignmentStatementContext;
import org.apache.sysml.parser.pydml.PydmlParser.FunctionCallMultiAssignmentStatementContext;
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
import org.apache.sysml.parser.pydml.PydmlParser.StrictParameterizedExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.StrictParameterizedKeyValueStringContext;
import org.apache.sysml.parser.pydml.PydmlParser.TypedArgNoAssignContext;
import org.apache.sysml.parser.pydml.PydmlParser.UnaryExpressionContext;
import org.apache.sysml.parser.pydml.PydmlParser.ValueDataTypeCheckContext;
import org.apache.sysml.parser.pydml.PydmlParser.WhileStatementContext;

/**
 * Minimal pre-processing of user function definitions which take precedence over built-in 
 * functions in cases where names conflict.  This pre-processing takes place outside of 
 * PymlSyntacticValidator since the function definition can be located after the function
 * is used in a statement.
 */
public class PydmlPreprocessor implements PydmlListener {

	protected final CustomErrorListener errorListener;
	// Names of user internal and external functions definitions
	protected Set<String> functions;

	public PydmlPreprocessor(CustomErrorListener errorListener) {
		this.errorListener = errorListener;
		functions = new HashSet<String>();
	}

	public Set<String> getFunctionDefs() {
		return functions;
	}

	@Override
	public void enterExternalFunctionDefExpression(ExternalFunctionDefExpressionContext ctx) {
		validateFunctionName(ctx.name.getText(), ctx);
	}

	@Override
	public void exitExternalFunctionDefExpression(ExternalFunctionDefExpressionContext ctx) {}

	@Override
	public void enterInternalFunctionDefExpression(InternalFunctionDefExpressionContext ctx) {
		validateFunctionName(ctx.name.getText(), ctx);
	}

	@Override
	public void exitInternalFunctionDefExpression(InternalFunctionDefExpressionContext ctx) {}

	protected void validateFunctionName(String name, ParserRuleContext ctx) {
		if (!functions.contains(name)) {
			functions.add(name);
		}
		else {
			notifyErrorListeners("Function Name Conflict: '" + name + "' already defined in " + errorListener.getCurrentFileName(), ctx.start);
		}
	}

	protected void notifyErrorListeners(String message, Token op) {
		errorListener.validationError(op.getLine(), op.getCharPositionInLine(), message);
	}

	// -----------------------------------------------------------------
	// 			Not overridden
	// -----------------------------------------------------------------

	@Override
	public void visitTerminal(TerminalNode node) {}

	@Override
	public void visitErrorNode(ErrorNode node) {}

	@Override
	public void enterEveryRule(ParserRuleContext ctx) {}

	@Override
	public void exitEveryRule(ParserRuleContext ctx) {}

	@Override
	public void enterFunctionCallMultiAssignmentStatement(FunctionCallMultiAssignmentStatementContext ctx) {}

	@Override
	public void exitFunctionCallMultiAssignmentStatement(FunctionCallMultiAssignmentStatementContext ctx) {}

	@Override
	public void enterIgnoreNewLine(IgnoreNewLineContext ctx) {}

	@Override
	public void exitIgnoreNewLine(IgnoreNewLineContext ctx) {}

	@Override
	public void enterMatrixDataTypeCheck(MatrixDataTypeCheckContext ctx) {}

	@Override
	public void exitMatrixDataTypeCheck(MatrixDataTypeCheckContext ctx) {}

	@Override
	public void enterStrictParameterizedKeyValueString(StrictParameterizedKeyValueStringContext ctx) {}

	@Override
	public void exitStrictParameterizedKeyValueString(StrictParameterizedKeyValueStringContext ctx) {}

	@Override
	public void enterPathStatement(PathStatementContext ctx) {}

	@Override
	public void exitPathStatement(PathStatementContext ctx) {}

	@Override
	public void enterConstTrueExpression(ConstTrueExpressionContext ctx) {}

	@Override
	public void exitConstTrueExpression(ConstTrueExpressionContext ctx) {}

	@Override
	public void enterTypedArgNoAssign(TypedArgNoAssignContext ctx) {}

	@Override
	public void exitTypedArgNoAssign(TypedArgNoAssignContext ctx) {}

	@Override
	public void enterWhileStatement(WhileStatementContext ctx) {}

	@Override
	public void exitWhileStatement(WhileStatementContext ctx) {}

	@Override
	public void enterConstStringIdExpression(ConstStringIdExpressionContext ctx) {}

	@Override
	public void exitConstStringIdExpression(ConstStringIdExpressionContext ctx) {}

	@Override
	public void enterDataIdExpression(DataIdExpressionContext ctx) {}

	@Override
	public void exitDataIdExpression(DataIdExpressionContext ctx) {}

	@Override
	public void enterAtomicExpression(AtomicExpressionContext ctx) {}

	@Override
	public void exitAtomicExpression(AtomicExpressionContext ctx) {}

	@Override
	public void enterPowerExpression(PowerExpressionContext ctx) {}

	@Override
	public void exitPowerExpression(PowerExpressionContext ctx) {}

	@Override
	public void enterFunctionCallAssignmentStatement(FunctionCallAssignmentStatementContext ctx) {}

	@Override
	public void exitFunctionCallAssignmentStatement(FunctionCallAssignmentStatementContext ctx) {}

	@Override
	public void enterModIntDivExpression(ModIntDivExpressionContext ctx) {}

	@Override
	public void exitModIntDivExpression(ModIntDivExpressionContext ctx) {}

	@Override
	public void enterSimpleDataIdentifierExpression(SimpleDataIdentifierExpressionContext ctx) {}

	@Override
	public void exitSimpleDataIdentifierExpression(SimpleDataIdentifierExpressionContext ctx) {}

	@Override
	public void enterBuiltinFunctionExpression(BuiltinFunctionExpressionContext ctx) {}

	@Override
	public void exitBuiltinFunctionExpression(BuiltinFunctionExpressionContext ctx) {}

	@Override
	public void enterConstIntIdExpression(ConstIntIdExpressionContext ctx) {}

	@Override
	public void exitConstIntIdExpression(ConstIntIdExpressionContext ctx) {}

	@Override
	public void enterForStatement(ForStatementContext ctx) {}

	@Override
	public void exitForStatement(ForStatementContext ctx) {}

	@Override
	public void enterParameterizedExpression(ParameterizedExpressionContext ctx) {}

	@Override
	public void exitParameterizedExpression(ParameterizedExpressionContext ctx) {}

	@Override
	public void enterConstFalseExpression(ConstFalseExpressionContext ctx) {}

	@Override
	public void exitConstFalseExpression(ConstFalseExpressionContext ctx) {}

	@Override
	public void enterBooleanOrExpression(BooleanOrExpressionContext ctx) {}

	@Override
	public void exitBooleanOrExpression(BooleanOrExpressionContext ctx) {}

	@Override
	public void enterAssignmentStatement(AssignmentStatementContext ctx) {}

	@Override
	public void exitAssignmentStatement(AssignmentStatementContext ctx) {}

	@Override
	public void enterIterablePredicateColonExpression(IterablePredicateColonExpressionContext ctx) {}

	@Override
	public void exitIterablePredicateColonExpression(IterablePredicateColonExpressionContext ctx) {}

	@Override
	public void enterParForStatement(ParForStatementContext ctx) {}

	@Override
	public void exitParForStatement(ParForStatementContext ctx) {}

	@Override
	public void enterStrictParameterizedExpression(StrictParameterizedExpressionContext ctx) {}

	@Override
	public void exitStrictParameterizedExpression(StrictParameterizedExpressionContext ctx) {}

	@Override
	public void enterCommandlineParamExpression(CommandlineParamExpressionContext ctx) {}

	@Override
	public void exitCommandlineParamExpression(CommandlineParamExpressionContext ctx) {}

	@Override
	public void enterMultDivExpression(MultDivExpressionContext ctx) {}

	@Override
	public void exitMultDivExpression(MultDivExpressionContext ctx) {}

	@Override
	public void enterAddSubExpression(AddSubExpressionContext ctx) {}

	@Override
	public void exitAddSubExpression(AddSubExpressionContext ctx) {}

	@Override
	public void enterImportStatement(ImportStatementContext ctx) {}

	@Override
	public void exitImportStatement(ImportStatementContext ctx) {}

	@Override
	public void enterProgramroot(ProgramrootContext ctx) {}

	@Override
	public void exitProgramroot(ProgramrootContext ctx) {}

	@Override
	public void enterIterablePredicateSeqExpression(IterablePredicateSeqExpressionContext ctx) {}

	@Override
	public void exitIterablePredicateSeqExpression(IterablePredicateSeqExpressionContext ctx) {}

	@Override
	public void enterIfdefAssignmentStatement(IfdefAssignmentStatementContext ctx) {}

	@Override
	public void exitIfdefAssignmentStatement(IfdefAssignmentStatementContext ctx) {}

	@Override
	public void enterBooleanAndExpression(BooleanAndExpressionContext ctx) {}

	@Override
	public void exitBooleanAndExpression(BooleanAndExpressionContext ctx) {}

	@Override
	public void enterIndexedExpression(IndexedExpressionContext ctx) {}

	@Override
	public void exitIndexedExpression(IndexedExpressionContext ctx) {}

	@Override
	public void enterBooleanNotExpression(BooleanNotExpressionContext ctx) {}

	@Override
	public void exitBooleanNotExpression(BooleanNotExpressionContext ctx) {}

	@Override
	public void enterIfStatement(IfStatementContext ctx) {}

	@Override
	public void exitIfStatement(IfStatementContext ctx) {}

	@Override
	public void enterRelationalExpression(RelationalExpressionContext ctx) {}

	@Override
	public void exitRelationalExpression(RelationalExpressionContext ctx) {}

	@Override
	public void enterCommandlinePositionExpression(CommandlinePositionExpressionContext ctx) {}

	@Override
	public void exitCommandlinePositionExpression(CommandlinePositionExpressionContext ctx) {}

	@Override
	public void enterConstDoubleIdExpression(ConstDoubleIdExpressionContext ctx) {}

	@Override
	public void exitConstDoubleIdExpression(ConstDoubleIdExpressionContext ctx) {}

	@Override
	public void enterUnaryExpression(UnaryExpressionContext ctx) {}

	@Override
	public void exitUnaryExpression(UnaryExpressionContext ctx) {}

	@Override
	public void enterValueDataTypeCheck(ValueDataTypeCheckContext ctx) {}

	@Override
	public void exitValueDataTypeCheck(ValueDataTypeCheckContext ctx) {}

	@Override
	public void enterMl_type(Ml_typeContext ctx) {}

	@Override
	public void exitMl_type(Ml_typeContext ctx) {}

	@Override
	public void enterElifBranch(ElifBranchContext ctx) {}

	@Override
	public void exitElifBranch(ElifBranchContext ctx) {}

	@Override
	public void enterIterableForStatement(IterableForStatementContext ctx) {}

	@Override
	public void exitIterableForStatement(IterableForStatementContext ctx) {}

}
