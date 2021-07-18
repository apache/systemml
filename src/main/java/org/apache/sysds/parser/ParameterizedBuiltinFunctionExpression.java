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

package org.apache.sysds.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.wink.json4j.JSONObject;
import org.apache.sysds.common.Builtins;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.ParamBuiltinOp;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.parser.LanguageException.LanguageErrorCodes;
import org.apache.sysds.runtime.util.CollectionUtils;


public class ParameterizedBuiltinFunctionExpression extends DataIdentifier 
{
	//note: we use a linked hashmap to preserve the order of
	//parameters if needed, such as for named lists
	private Builtins _opcode;
	private LinkedHashMap<String,Expression> _varParams;
	
	public static final String TF_FN_PARAM_DATA = "target";
	public static final String TF_FN_PARAM_MTD2 = "meta";
	public static final String TF_FN_PARAM_SPEC = "spec";
	public static final String TF_FN_PARAM_MTD = "transformPath"; //NOTE MB: for backwards compatibility
	
	public static HashMap<Builtins, ParamBuiltinOp> pbHopMap;
	static {
		pbHopMap = new HashMap<>();
		
		pbHopMap.put(Builtins.GROUPEDAGG, ParamBuiltinOp.GROUPEDAGG);
		pbHopMap.put(Builtins.RMEMPTY, ParamBuiltinOp.RMEMPTY);
		pbHopMap.put(Builtins.REPLACE, ParamBuiltinOp.REPLACE);
		pbHopMap.put(Builtins.LOWER_TRI, ParamBuiltinOp.LOWER_TRI);
		pbHopMap.put(Builtins.UPPER_TRI, ParamBuiltinOp.UPPER_TRI);
		
		// For order, a ReorgOp is constructed with ReorgOp.SORT type
		pbHopMap.put(Builtins.ORDER, ParamBuiltinOp.INVALID);
		
		// Distribution Functions
		pbHopMap.put(Builtins.CDF, ParamBuiltinOp.CDF);
		pbHopMap.put(Builtins.PNORM, ParamBuiltinOp.CDF);
		pbHopMap.put(Builtins.PT, ParamBuiltinOp.CDF);
		pbHopMap.put(Builtins.PF, ParamBuiltinOp.CDF);
		pbHopMap.put(Builtins.PCHISQ, ParamBuiltinOp.CDF);
		pbHopMap.put(Builtins.PEXP, ParamBuiltinOp.CDF);
		
		pbHopMap.put(Builtins.INVCDF, ParamBuiltinOp.INVCDF);
		pbHopMap.put(Builtins.QNORM, ParamBuiltinOp.INVCDF);
		pbHopMap.put(Builtins.QT, ParamBuiltinOp.INVCDF);
		pbHopMap.put(Builtins.QF, ParamBuiltinOp.INVCDF);
		pbHopMap.put(Builtins.QCHISQ, ParamBuiltinOp.INVCDF);
		pbHopMap.put(Builtins.QEXP, ParamBuiltinOp.INVCDF);
		
		// toString
		pbHopMap.put(Builtins.TOSTRING, ParamBuiltinOp.TOSTRING);
	}
	
	public static ParameterizedBuiltinFunctionExpression getParamBuiltinFunctionExpression(ParserRuleContext ctx,
			String functionName, ArrayList<ParameterExpression> paramExprsPassed, String fileName) {
		if (functionName == null || paramExprsPassed == null)
			return null;
		
		Builtins pbifop = Builtins.get(functionName, true);
		
		if ( pbifop == null ) 
			return null;
		
		LinkedHashMap<String,Expression> varParams = new LinkedHashMap<>();
		for (ParameterExpression pexpr : paramExprsPassed)
			varParams.put(pexpr.getName(), pexpr.getExpr());
		
		ParameterizedBuiltinFunctionExpression retVal = 
			new ParameterizedBuiltinFunctionExpression(ctx, pbifop,varParams, fileName);
		return retVal;
	}
	
			
	public ParameterizedBuiltinFunctionExpression(ParserRuleContext ctx, Builtins op, LinkedHashMap<String,Expression> varParams,
			String filename) {
		_opcode = op;
		_varParams = varParams;
		setCtxValuesAndFilename(ctx, filename);
	}

	public ParameterizedBuiltinFunctionExpression(Builtins op,
			LinkedHashMap<String, Expression> varParams, ParseInfo parseInfo) {
		_opcode = op;
		_varParams = varParams;
		setParseInfo(parseInfo);
	}

	@Override
	public Expression rewriteExpression(String prefix) {
		LinkedHashMap<String,Expression> newVarParams = new LinkedHashMap<>();
		for (String key : _varParams.keySet()){
			Expression newExpr = _varParams.get(key).rewriteExpression(prefix);
			newVarParams.put(key, newExpr);
		}
		ParameterizedBuiltinFunctionExpression retVal = 
			new ParameterizedBuiltinFunctionExpression(_opcode, newVarParams, this);
		return retVal;
	}

	public void setOpcode(Builtins op) {
		_opcode = op;
	}
	
	public Builtins getOpCode() {
		return _opcode;
	}
	
	public HashMap<String,Expression> getVarParams() {
		return _varParams;
	}
	
	public Expression getVarParam(String name) {
		return _varParams.get(name);
	}

	public void addVarParam(String name, Expression value){
		_varParams.put(name, value);
	}

	/**
	 * Validate parse tree : Process BuiltinFunction Expression in an assignment
	 * statement
	 */
	@Override
	public void validateExpression(HashMap<String, DataIdentifier> ids, HashMap<String, ConstIdentifier> constVars, boolean conditional)
	{
		// validate all input parameters
		for ( String s : getVarParams().keySet() ) {
			Expression paramExpr = getVarParam(s);
			if (paramExpr instanceof FunctionCallIdentifier)
				raiseValidateError("UDF function call not supported as parameter to built-in function call", false);	
			paramExpr.validateExpression(ids, constVars, conditional);
		}
		
		String outputName = getTempName();
		DataIdentifier output = new DataIdentifier(outputName);
		//output.setProperties(this.getFirstExpr().getOutput());
		this.setOutput(output);

		// IMPORTANT: for each operation, one must handle unnamed parameters
		
		switch (this.getOpCode()) {
		
		case GROUPEDAGG:
			validateGroupedAgg(output, conditional);
			break; 
			
		case CDF:
		case INVCDF:
		case PNORM:
		case QNORM:
		case PT:
		case QT:
		case PF:
		case QF:
		case PCHISQ:
		case QCHISQ:
		case PEXP:
		case QEXP:
			validateDistributionFunctions(output, conditional);
			break;
			
		case RMEMPTY:
			validateRemoveEmpty(output, conditional);
			break;
		
		case REPLACE:
			validateReplace(output, conditional);
			break;
		
		case ORDER:
			validateOrder(output, conditional);
			break;

		case TOKENIZE:
			validateTokenize(output, conditional);
			break;
		
		case TRANSFORMAPPLY:
			validateTransformApply(output, conditional);
			break;
		
		case TRANSFORMDECODE:
			validateTransformDecode(output, conditional);
			break;
		
		case TRANSFORMCOLMAP:
			validateTransformColmap(output, conditional);
			break;
		
		case TRANSFORMMETA:
			validateTransformMeta(output, conditional);
			break;
			
		case LOWER_TRI:
		case UPPER_TRI:
			validateExtractTriangular(output, getOpCode(), conditional);
			break;
			
		case TOSTRING:
			validateCastAsString(output, conditional);
			break;
		
		case LISTNV:
			validateNamedList(output, conditional);
			break;

		case PARAMSERV:
			validateParamserv(output, conditional);
			break;

		default: //always unconditional (because unsupported operation)
			//handle common issue of transformencode
			if( getOpCode()==Builtins.TRANSFORMENCODE )
				raiseValidateError("Parameterized function "+ getOpCode() +" requires a multi-assignment statement "
						+ "for data and metadata.", false, LanguageErrorCodes.UNSUPPORTED_EXPRESSION);
			else
				raiseValidateError("Unsupported parameterized function "+ getOpCode(), 
						false, LanguageErrorCodes.UNSUPPORTED_EXPRESSION);
		}
	}

	@Override
	public void validateExpression(MultiAssignmentStatement stmt, HashMap<String, DataIdentifier> ids, HashMap<String, ConstIdentifier> constVars, boolean conditional)
	{
		// validate all input parameters
		for ( String s : getVarParams().keySet() ) {
			Expression paramExpr = getVarParam(s);			
			if (paramExpr instanceof FunctionCallIdentifier)
				raiseValidateError("UDF function call not supported as parameter to built-in function call", false);
			paramExpr.validateExpression(ids, constVars, conditional);
		}
		
		_outputs = new Identifier[stmt.getTargetList().size()];
		int count = 0;
		for (DataIdentifier outParam: stmt.getTargetList()){
			DataIdentifier tmp = new DataIdentifier(outParam);
			tmp.setParseInfo(this);
			_outputs[count++] = tmp;
		}
		
		switch (this.getOpCode()) {	
			case TRANSFORMENCODE:
				DataIdentifier out1 = (DataIdentifier) getOutputs()[0];
				DataIdentifier out2 = (DataIdentifier) getOutputs()[1];
				
				validateTransformEncode(out1, out2, conditional);
				break;	
			default: //always unconditional (because unsupported operation)
				raiseValidateError("Unsupported parameterized function "+ getOpCode(), false, LanguageErrorCodes.INVALID_PARAMETERS);
		}
	}


	private void validateParamserv(DataIdentifier output, boolean conditional) {
		String fname = getOpCode().name();
		// validate the first five arguments
		if (getVarParams().size() < 1) {
			raiseValidateError("Should provide more arguments for function " + fname, false, LanguageErrorCodes.INVALID_PARAMETERS);
		}
		//check for invalid parameters
		Set<String> valid = CollectionUtils.asSet(Statement.PS_MODEL, Statement.PS_FEATURES, Statement.PS_LABELS,
			Statement.PS_VAL_FEATURES, Statement.PS_VAL_LABELS, Statement.PS_UPDATE_FUN, Statement.PS_AGGREGATION_FUN,
			Statement.PS_VAL_FUN, Statement.PS_MODE, Statement.PS_UPDATE_TYPE, Statement.PS_FREQUENCY, Statement.PS_EPOCHS,
			Statement.PS_BATCH_SIZE, Statement.PS_PARALLELISM, Statement.PS_SCHEME, Statement.PS_FED_RUNTIME_BALANCING,
			Statement.PS_FED_WEIGHTING, Statement.PS_HYPER_PARAMS, Statement.PS_CHECKPOINTING, Statement.PS_SEED,Statement.PS_MODELAVG);
		checkInvalidParameters(getOpCode(), getVarParams(), valid);

		// check existence and correctness of parameters
		checkDataType(fname, Statement.PS_MODEL, DataType.LIST, conditional); // check the model which is the only non-parameterized argument
		checkDataType(fname, Statement.PS_FEATURES, DataType.MATRIX, conditional);
		checkDataType(fname, Statement.PS_LABELS, DataType.MATRIX, conditional);
		checkDataValueType(true, fname, Statement.PS_VAL_FEATURES, DataType.MATRIX, ValueType.FP64, conditional);
		checkDataValueType(true, fname, Statement.PS_VAL_LABELS, DataType.MATRIX, ValueType.FP64, conditional);
		checkDataValueType(false, fname, Statement.PS_UPDATE_FUN, DataType.SCALAR, ValueType.STRING, conditional);
		checkDataValueType(false, fname, Statement.PS_AGGREGATION_FUN, DataType.SCALAR, ValueType.STRING, conditional);
		checkDataValueType(true, fname, Statement.PS_VAL_FUN, DataType.SCALAR, ValueType.STRING, conditional);
		checkStringParam(true, fname, Statement.PS_MODE, conditional);
		checkStringParam(true, fname, Statement.PS_UPDATE_TYPE, conditional);
		checkStringParam(true, fname, Statement.PS_FREQUENCY, conditional);
		checkDataValueType(false, fname, Statement.PS_EPOCHS, DataType.SCALAR, ValueType.INT64, conditional);
		checkDataValueType(true, fname, Statement.PS_BATCH_SIZE, DataType.SCALAR, ValueType.INT64, conditional);
		checkDataValueType(true, fname, Statement.PS_PARALLELISM, DataType.SCALAR, ValueType.INT64, conditional);
		checkStringParam(true, fname, Statement.PS_SCHEME, conditional);
		checkStringParam(true, fname, Statement.PS_FED_RUNTIME_BALANCING, conditional);
		checkStringParam(true, fname, Statement.PS_FED_WEIGHTING, conditional);
		checkDataValueType(true, fname, Statement.PS_HYPER_PARAMS, DataType.LIST, ValueType.UNKNOWN, conditional);
		checkStringParam(true, fname, Statement.PS_CHECKPOINTING, conditional);
		checkDataValueType(true, fname, Statement.PS_SEED, DataType.SCALAR, ValueType.INT64, conditional);

		// set output characteristics
		output.setDataType(DataType.LIST);
		output.setValueType(ValueType.UNKNOWN);
		output.setDimensions(getVarParam(Statement.PS_MODEL).getOutput().getDim1(), 1);
		output.setBlocksize(-1);
	}

	private void checkStringParam(boolean optional, String fname, String pname, boolean conditional) {
		Expression param = getVarParam(pname);
		if (param == null) {
			if (optional) {
				return;
			}
			raiseValidateError(String.format("Function %s should provide parameter '%s'", fname, pname), conditional);
		}
		if (!(param.getOutput().getDataType().isScalar() && param.getOutput().getValueType().equals(ValueType.STRING))) {
			raiseValidateError(
					String.format("Function %s should provide a string value for %s parameter.", fname, pname),
					conditional);
		}
	}

	private void validateTokenize(DataIdentifier output, boolean conditional)
	{
		//validate data / metadata (recode maps)
		checkDataType("tokenize", TF_FN_PARAM_DATA, DataType.FRAME, conditional);

		//validate specification
		checkDataValueType(false, "tokenize", TF_FN_PARAM_SPEC, DataType.SCALAR, ValueType.STRING, conditional);
		validateTransformSpec(TF_FN_PARAM_SPEC, conditional);

		//set output dimensions
		output.setDataType(DataType.FRAME);
		output.setValueType(ValueType.STRING);
		output.setDimensions(-1, -1);
	}

	// example: A = transformapply(target=X, meta=M, spec=s)
	private void validateTransformApply(DataIdentifier output, boolean conditional) 
	{
		//validate data / metadata (recode maps)
		checkDataType("transformapply", TF_FN_PARAM_DATA, DataType.FRAME, conditional);
		checkDataType("transformapply", TF_FN_PARAM_MTD2, DataType.FRAME, conditional);
		
		//validate specification
		checkDataValueType(false, "transformapply", TF_FN_PARAM_SPEC, DataType.SCALAR, ValueType.STRING, conditional);
		validateTransformSpec(TF_FN_PARAM_SPEC, conditional);
		
		//set output dimensions
		output.setDataType(DataType.MATRIX);
		output.setValueType(ValueType.FP64);
		output.setDimensions(-1, -1);
	}
	
	private void validateTransformDecode(DataIdentifier output, boolean conditional) 
	{
		//validate data / metadata (recode maps) 
		checkDataType("transformdecode", TF_FN_PARAM_DATA, DataType.MATRIX, conditional);
		checkDataType("transformdecode", TF_FN_PARAM_MTD2, DataType.FRAME, conditional);
		
		//validate specification
		checkDataValueType(false, "transformdecode", TF_FN_PARAM_SPEC, DataType.SCALAR, ValueType.STRING, conditional);
		validateTransformSpec(TF_FN_PARAM_SPEC, conditional);
		
		//set output dimensions
		output.setDataType(DataType.FRAME);
		output.setValueType(ValueType.STRING);
		output.setDimensions(-1, -1);
	}
	
	private void validateTransformColmap(DataIdentifier output, boolean conditional) 
	{
		//validate data / metadata (recode maps) 
		Expression exprTarget = getVarParam(Statement.GAGG_TARGET);
		checkDataType("transformcolmap", TF_FN_PARAM_DATA, DataType.FRAME, conditional);
		
		//validate specification
		checkDataValueType(false,"transformcolmap", TF_FN_PARAM_SPEC, DataType.SCALAR, ValueType.STRING, conditional);
		validateTransformSpec(TF_FN_PARAM_SPEC, conditional);
		
		//set output dimensions
		output.setDataType(DataType.MATRIX);
		output.setValueType(ValueType.FP64);
		output.setDimensions(exprTarget.getOutput().getDim2(), 3);
	}
	
	private void validateTransformMeta(DataIdentifier output, boolean conditional) 
	{
		//validate specification
		checkDataValueType(false,"transformmeta", TF_FN_PARAM_SPEC, DataType.SCALAR, ValueType.STRING, conditional);
		validateTransformSpec(TF_FN_PARAM_SPEC, conditional);
		
		//validate meta data path 
		checkDataValueType(false,"transformmeta", TF_FN_PARAM_MTD, DataType.SCALAR, ValueType.STRING, conditional);
		
		//set output dimensions
		output.setDataType(DataType.FRAME);
		output.setValueType(ValueType.STRING);
		output.setDimensions(-1, -1);
	}
	
	private void validateTransformEncode(DataIdentifier output1, DataIdentifier output2, boolean conditional) 
	{
		//validate data / metadata (recode maps) 
		checkDataType("transformencode", TF_FN_PARAM_DATA, DataType.FRAME, conditional);
		
		//validate specification
		checkDataValueType(false, "transformencode", TF_FN_PARAM_SPEC, DataType.SCALAR, ValueType.STRING, conditional);
		validateTransformSpec(TF_FN_PARAM_SPEC, conditional);
		
		//set output dimensions 
		output1.setDataType(DataType.MATRIX);
		output1.setValueType(ValueType.FP64);
		output1.setDimensions(-1, -1);
		output2.setDataType(DataType.FRAME);
		output2.setValueType(ValueType.STRING);
		output2.setDimensions(-1, -1);
	}
	
	@SuppressWarnings("unused")
	private void validateTransformSpec(String pname, boolean conditional) {
		Expression data = getVarParam(pname);
		if( data instanceof StringIdentifier ) {
			try {
				StringIdentifier spec = (StringIdentifier)data;
				new JSONObject(spec.getValue()); //for validate
			}
			catch(Exception ex) {
				raiseValidateError("Transform specification parsing issue: ", 
					conditional, ex.getMessage());
			}
		}
	}
	
	private void validateExtractTriangular(DataIdentifier output,  Builtins op, boolean conditional) {
		
		//check for invalid parameters
		Set<String> valid = CollectionUtils.asSet("target", "diag", "values");
		checkInvalidParameters(op, getVarParams(), valid);
		
		//check existence and correctness of arguments
		checkTargetParam(getVarParam("target"), conditional);
		checkOptionalBooleanParam(getVarParam("diag"), "diag", conditional);
		checkOptionalBooleanParam(getVarParam("values"), "values", conditional);
		if( getVarParam("diag") == null ) //default handling
			_varParams.put("diag", new BooleanIdentifier(false));
		if( getVarParam("values") == null ) //default handling
			_varParams.put("values", new BooleanIdentifier(false));
		
		// Output is a matrix with unknown dims
		Identifier in = getVarParam("target").getOutput();
		output.setDataType(DataType.MATRIX);
		output.setValueType(ValueType.FP64);
		output.setDimensions(in.getDim1(), in.getDim2());
	}
	
	private void validateReplace(DataIdentifier output, boolean conditional) {
		//check existence and correctness of arguments
		Expression target = getVarParam("target");
		if( target.getOutput().getDataType() != DataType.FRAME ){
			checkTargetParam(target, conditional);
		}
		
		Expression pattern = getVarParam("pattern");
		if( pattern==null ) {
			raiseValidateError("Named parameter 'pattern' missing. Please specify the replacement pattern.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}
		else if( pattern.getOutput().getDataType() != DataType.SCALAR ){				
			raiseValidateError("Replacement pattern 'pattern' is of type '"+pattern.getOutput().getDataType()+"'. Please, specify a scalar replacement pattern.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}	
		
		Expression replacement = getVarParam("replacement");
		if( replacement==null ) {
			raiseValidateError("Named parameter 'replacement' missing. Please specify the replacement value.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}
		else if( replacement.getOutput().getDataType() != DataType.SCALAR ){	
			raiseValidateError("Replacement value 'replacement' is of type '"+replacement.getOutput().getDataType()+"'. Please, specify a scalar replacement value.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}	
		
		// Output is a matrix with same dims as input
		output.setDataType(target.getOutput().getDataType());
		if(target.getOutput().getDataType() == DataType.FRAME)
			output.setValueType(ValueType.STRING);
		else
			output.setValueType(ValueType.FP64);
		output.setDimensions(target.getOutput().getDim1(), target.getOutput().getDim2());
	}

	private void validateOrder(DataIdentifier output, boolean conditional) {
		//check existence and correctness of arguments
		Expression target = getVarParam("target");
		checkTargetParam(target, conditional);
		
		//check for unsupported parameters
		for(String param : getVarParams().keySet())
			if( !(param.equals("target") || param.equals("by") || param.equals("decreasing") || param.equals("index.return")) )
				raiseValidateError("Unsupported order parameter: '"+param+"'", false);
		
		Expression orderby = getVarParam("by"); //[OPTIONAL] BY
		if( orderby == null ) { //default first column, good fit for vectors
			orderby = new IntIdentifier(1);
			addVarParam("by", orderby);
		}
		else if( !(orderby.getOutput().getDataType().isScalar() 
			|| orderby.getOutput().getDataType().isMatrix()) ) {
			raiseValidateError("Orderby column 'by' is of type '"+orderby.getOutput().getDataType()+"'. Please, use a scalar or row vector to specify column indexes.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}
		
		Expression decreasing = getVarParam("decreasing"); //[OPTIONAL] DECREASING
		if( decreasing == null ) { //default: ascending
			addVarParam("decreasing", new BooleanIdentifier(false));
		}
		else if( decreasing.getOutput().getDataType() != DataType.SCALAR ){
			raiseValidateError("Ordering 'decreasing' is of type '"+decreasing.getOutput().getDataType()+"', '"+decreasing.getOutput().getValueType()+"'. Please, specify 'decreasing' as a scalar boolean.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}
		
		Expression indexreturn = getVarParam("index.return"); //[OPTIONAL] DECREASING
		if( indexreturn == null ) { //default: sorted data
			indexreturn = new BooleanIdentifier(false);
			addVarParam("index.return", indexreturn);
		}
		else if( indexreturn.getOutput().getDataType() != DataType.SCALAR ){
			raiseValidateError("Return type 'index.return' is of type '"+indexreturn.getOutput().getDataType()+"', '"+indexreturn.getOutput().getValueType()+"'. Please, specify 'indexreturn' as a scalar boolean.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}
		long dim2 = ( indexreturn instanceof BooleanIdentifier ) ? 
				((BooleanIdentifier)indexreturn).getValue() ? 1: target.getOutput().getDim2() : -1; 
		
		// Output is a matrix with same dims as input
		output.setDataType(DataType.MATRIX);
		output.setValueType(ValueType.FP64);
		output.setDimensions(target.getOutput().getDim1(), dim2 );
		
	}

	private void validateRemoveEmpty(DataIdentifier output, boolean conditional) {
		
		//check for invalid parameters
		Set<String> valid = CollectionUtils.asSet("target", "margin", "select", "empty.return");
		Set<String> invalid = _varParams.keySet().stream()
			.filter(k -> !valid.contains(k)).collect(Collectors.toSet());
		if( !invalid.isEmpty() )
			raiseValidateError("Invalid parameters for removeEmpty: "
				+ Arrays.toString(invalid.toArray(new String[0])), false);
		
		//check existence and correctness of arguments
		checkTargetParam(getVarParam("target"), conditional);
		
		Expression margin = getVarParam("margin");
		if( margin==null ){
			raiseValidateError("Named parameter 'margin' missing. Please specify 'rows' or 'cols'.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}
		else if( !(margin instanceof DataIdentifier) && !margin.toString().equals("rows") && !margin.toString().equals("cols") ){
			raiseValidateError("Named parameter 'margin' has an invalid value '"+margin.toString()+"'. Please specify 'rows' or 'cols'.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}
		
		Expression select = getVarParam("select");
		if( select!=null && select.getOutput().getDataType() != DataType.MATRIX ){
			raiseValidateError("Index matrix 'select' is of type '"+select.getOutput().getDataType()+"'. Please specify the select matrix.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}
		
		Expression empty = getVarParam("empty.return");
		if( empty!=null && (!empty.getOutput().getDataType().isScalar() || empty.getOutput().getValueType() != ValueType.BOOLEAN) ){
			raiseValidateError("Boolean parameter 'empty.return' is of type "+empty.getOutput().getDataType()
				+"["+empty.getOutput().getValueType()+"].", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}
		if( empty == null ) //default handling
			_varParams.put("empty.return", new BooleanIdentifier(true));
		
		// Output is a matrix with unknown dims
		output.setDataType(DataType.MATRIX);
		output.setValueType(ValueType.FP64);
		output.setDimensions(-1, -1);
	}
	
	private void validateGroupedAgg(DataIdentifier output, boolean conditional) 
	{
		//check existing target and groups
		if (getVarParam(Statement.GAGG_TARGET)  == null || getVarParam(Statement.GAGG_GROUPS) == null){
			raiseValidateError("Must define both target and groups.", conditional);
		}
		
		Expression exprTarget = getVarParam(Statement.GAGG_TARGET);
		Expression exprGroups = getVarParam(Statement.GAGG_GROUPS);
		Expression exprNGroups = getVarParam(Statement.GAGG_NUM_GROUPS);
		
		//check valid input dimensions
		boolean colwise = true;
		boolean matrix = false;
		if( exprGroups.getOutput().dimsKnown() && exprTarget.getOutput().dimsKnown() )
		{
			//check for valid matrix input
			if( exprGroups.getOutput().getDim2()==1 && exprTarget.getOutput().getDim2()>1 )
			{
				if( getVarParam(Statement.GAGG_WEIGHTS) != null ) {
					raiseValidateError("Matrix input not supported with weights.", conditional);
				}
				if( getVarParam(Statement.GAGG_NUM_GROUPS) == null ) {
					raiseValidateError("Matrix input not supported without specified numgroups.", conditional);
				}
				if( exprGroups.getOutput().getDim1() != exprTarget.getOutput().getDim1() ) {					
					raiseValidateError("Target and groups must have same dimensions -- " + " target dims: " + 
						exprTarget.getOutput().getDim1() +" x "+exprTarget.getOutput().getDim2()+", groups dims: " + exprGroups.getOutput().getDim1() + " x 1.", conditional);
				}
				matrix = true;
			}
			//check for valid col vector input
			else if( exprGroups.getOutput().getDim2()==1 && exprTarget.getOutput().getDim2()==1 )
			{
				if( exprGroups.getOutput().getDim1() != exprTarget.getOutput().getDim1() ) {					
					raiseValidateError("Target and groups must have same dimensions -- " + " target dims: " + 
						exprTarget.getOutput().getDim1() +" x 1, groups dims: " + exprGroups.getOutput().getDim1() + " x 1.", conditional);
				}
			}
			//check for valid row vector input
			else if( exprGroups.getOutput().getDim1()==1 && exprTarget.getOutput().getDim1()==1 )
			{
				if( exprGroups.getOutput().getDim2() != exprTarget.getOutput().getDim2() ) {					
					raiseValidateError("Target and groups must have same dimensions -- " + " target dims: " + 
						"1 x " + exprTarget.getOutput().getDim2() +", groups dims: 1 x " + exprGroups.getOutput().getDim2() + ".", conditional);
				}
				colwise = true;
			}
			else {
				raiseValidateError("Invalid target and groups inputs - dimension mismatch.", conditional);
			}
		}
		
		
		//check function parameter
		Expression functParam = getVarParam(Statement.GAGG_FN);
		if( functParam == null ) {
			raiseValidateError("must define function name (fn=<function name>) for aggregate()", conditional);
		}
		else if (functParam instanceof Identifier)
		{
			// standardize to lowercase and dequote fname
			String fnameStr = functParam.toString();
			
			// check that IF fname="centralmoment" THEN order=m is defined, where m=2,3,4 
			// check ELSE IF fname is allowed
			if(fnameStr.equals(Statement.GAGG_FN_CM)){
				String orderStr = getVarParam(Statement.GAGG_FN_CM_ORDER) == null ? null : getVarParam(Statement.GAGG_FN_CM_ORDER).toString();
				if (orderStr == null || !(orderStr.equals("2") || orderStr.equals("3") || orderStr.equals("4"))){
					raiseValidateError("for centralmoment, must define order.  Order must be equal to 2,3, or 4", conditional);
				}
			}
			else if (fnameStr.equals(Statement.GAGG_FN_COUNT) 
					|| fnameStr.equals(Statement.GAGG_FN_SUM) 
					|| fnameStr.equals(Statement.GAGG_FN_MEAN)
					|| fnameStr.equals(Statement.GAGG_FN_VARIANCE)){}
			else { 
				raiseValidateError("fname is " + fnameStr + " but must be either centeralmoment, count, sum, mean, variance", conditional);
			}
		}
		
		//determine output dimensions
		long outputDim1 = -1, outputDim2 = -1;
		if( exprNGroups != null && exprNGroups instanceof Identifier ) 
		{
			Identifier numGroups = (Identifier) exprNGroups;
			if ( numGroups instanceof ConstIdentifier) {
				long ngroups = ((ConstIdentifier)numGroups).getLongValue();
				if ( colwise ) {
					outputDim1 = ngroups;
					outputDim2 = matrix ? exprTarget.getOutput().getDim2() : 1;
				}
				else {
					outputDim1 = 1; //no support for matrix
					outputDim2 = ngroups;
				}
			}
		}
		
		//set output meta data
		output.setDataType(DataType.MATRIX);
		output.setValueType(ValueType.FP64);
		output.setDimensions(outputDim1, outputDim2);
	}
	
	private void checkTargetParam(Expression target, boolean conditional) {
		if( target==null )
			raiseValidateError("Named parameter 'target' missing. Please specify the input matrix.",
				conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		else if( target.getOutput().getDataType() != DataType.MATRIX )
			raiseValidateError("Input matrix 'target' is of type '"+target.getOutput().getDataType()
				+"'. Please specify the input matrix.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
	}
	
	private void checkOptionalBooleanParam(Expression param, String name, boolean conditional) {
		if( param!=null && (!param.getOutput().getDataType().isScalar() || param.getOutput().getValueType() != ValueType.BOOLEAN) ){
			raiseValidateError("Boolean parameter '"+name+"' is of type "+param.getOutput().getDataType()
				+"["+param.getOutput().getValueType()+"].", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}
	}

	private void checkInvalidParameters(Builtins op, HashMap<String, Expression> params,
			Set<String> valid) {
		Set<String> invalid = params.keySet().stream().filter(k -> !valid.contains(k)).collect(Collectors.toSet());
		if (!invalid.isEmpty()) {
			List<String> invalidMsg = invalid.stream().map(k -> {
				String val = params.get(k).getText();
				return k == null ? val : k + "=" + val;
			}).collect(Collectors.toList());
			raiseValidateError(String.format("Invalid parameters for %s: %s", op.name(), invalidMsg), false);
		}
	}

	private void validateDistributionFunctions(DataIdentifier output, boolean conditional) {
		// CDF and INVCDF expects one unnamed parameter, it must be renamed as "quantile" 
		// (i.e., we must compute P(X <= x) where x is called as "quantile" )
		
		Builtins op = this.getOpCode();
		
		// check if quantile is of type SCALAR
		if ( getVarParam("target") == null || getVarParam("target").getOutput().getDataType() != DataType.SCALAR ) {
			raiseValidateError("target must be provided for distribution functions, and it must be a scalar value.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}
		
		// Distribution specific checks
		switch(op) {
		case CDF:
		case INVCDF:
			if(getVarParam("dist") == null) {
				raiseValidateError("For cdf() and icdf(), a distribution function must be specified (as a string).", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
			}
			break;
			
		case QF:
		case PF:
			if(getVarParam("df1") == null || getVarParam("df2") == null ) {
				raiseValidateError("Two degrees of freedom df1 and df2 must be provided for F-distribution.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
			}
			break;
			
		case QT:
		case PT:
			if(getVarParam("df") == null ) {
				raiseValidateError("Degrees of freedom df must be provided for t-distribution.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
			}
			break;
			
		case QCHISQ:
		case PCHISQ:
			if(getVarParam("df") == null ) {
				raiseValidateError("Degrees of freedom df must be provided for chi-squared-distribution.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
			}
			break;
			
			default:
				break;
				
			// Not checking for QNORM, PNORM: distribution parameters mean and sd are optional with default values 0.0 and 1.0, respectively
			// Not checking for QEXP, PEXP: distribution parameter rate is optional with a default values 1.0
			
			// For all cdf functions, additional parameter lower.tail is optional with a default value TRUE
		}
		
		// CDF and INVCDF specific checks:
		switch(op) {
		case INVCDF:
		case QNORM:
		case QF:
		case QT:
		case QCHISQ:
		case QEXP:
			if(getVarParam("lower.tail") != null ) {
				raiseValidateError("Lower tail argument is invalid while computing inverse cumulative probabilities.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
			}
			break;
			
		case CDF:
		case PNORM:
		case PF:
		case PT:
		case PCHISQ:
		case PEXP:
			// no checks yet
			break;
			
			default:
				break;
		}
		
		// Output is a scalar
		output.setDataType(DataType.SCALAR);
		output.setValueType(ValueType.FP64);
		output.setDimensions(0, 0);
	}
	
	private void validateCastAsString(DataIdentifier output, boolean conditional) {
		HashMap<String, Expression> varParams = getVarParams();
		
		// replace parameter name for matrix argument
		if( varParams.containsKey(null) )
			varParams.put("target", varParams.remove(null));
		
		// check validate parameter names
		String[] validArgsArr = {"target", "rows", "cols", "decimal", "sparse", "sep", "linesep"};
		HashSet<String> validArgs = new HashSet<>(Arrays.asList(validArgsArr));
		for( String k : varParams.keySet() ) {
			if( !validArgs.contains(k) ) {
				raiseValidateError("Invalid parameter " + k + " for toString, valid parameters are " + 
						Arrays.toString(validArgsArr), conditional, LanguageErrorCodes.INVALID_PARAMETERS);
			}
		}
		
		// set output characteristics
		output.setDataType(DataType.SCALAR);
		output.setValueType(ValueType.STRING);
		output.setDimensions(0, 0);
	}
	
	private void validateNamedList(DataIdentifier output, boolean conditional) {
		HashMap<String, Expression> varParams = getVarParams();
		
		// set output characteristics
		output.setDataType(DataType.LIST);
		output.setValueType(ValueType.UNKNOWN);
		output.setDimensions(varParams.size(), 1);
		output.setBlocksize(-1);
	}

	private void checkDataType( String fname, String pname, DataType dt, boolean conditional ) {
		Expression data = getVarParam(pname);
		if( data==null )
			raiseValidateError("Named parameter '" + pname + "' missing. Please specify the input.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		else if( data.getOutput().getDataType() != dt )
			raiseValidateError("Input to "+fname+"::"+pname+" must be of type '"+dt.toString()+"'. It should not be of type '"+data.getOutput().getDataType()+"'.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
	}

	private void checkDataValueType(boolean optional, String fname, String pname, DataType dt, ValueType vt,
			boolean conditional) {
		Expression data = getVarParam(pname);
		if (data == null) {
			if (optional) {
				return;
			}
			raiseValidateError(String.format("Named parameter '%s' is missing. Please specify the input.", pname),
					conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		} else if (data.getOutput().getDataType() != dt || data.getOutput().getValueType() != vt)
			raiseValidateError(String.format("Input to %s::%s must be of type '%s', '%s'.It should not be of type '%s', '%s'.",
							fname, pname, dt.toString(), vt.toString(), data.getOutput().getDataType().toString(),
							data.getOutput().getValueType().toString()), conditional,
					LanguageErrorCodes.INVALID_PARAMETERS);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(_opcode.toString() + "(");

		 for (String key : _varParams.keySet()){
			 sb.append("," + key + "=" + _varParams.get(key));
		 }
		sb.append(" )");
		return sb.toString();
	}

	@Override
	public VariableSet variablesRead() {
		VariableSet result = new VariableSet();
		for (String s : _varParams.keySet()) {
			result.addVariables ( _varParams.get(s).variablesRead() );
		}
		return result;
	}

	@Override
	public VariableSet variablesUpdated() {
		VariableSet result = new VariableSet();
		for (String s : _varParams.keySet()) {
			result.addVariables ( _varParams.get(s).variablesUpdated() );
		}
		result.addVariable(((DataIdentifier)this.getOutput()).getName(), (DataIdentifier)this.getOutput());
		return result;
	}

	@Override
	public boolean multipleReturns() {
		return (_opcode == Builtins.TRANSFORMENCODE);
	}
}
