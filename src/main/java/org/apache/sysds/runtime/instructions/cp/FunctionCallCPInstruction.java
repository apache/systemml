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

package org.apache.sysds.runtime.instructions.cp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.lops.Lop;
import org.apache.sysds.parser.DMLProgram;
import org.apache.sysds.parser.DataIdentifier;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.DMLScriptException;
import org.apache.sysds.runtime.controlprogram.FunctionProgramBlock;
import org.apache.sysds.runtime.controlprogram.LocalVariableMap;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContextFactory;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.lineage.Lineage;
import org.apache.sysds.runtime.lineage.LineageCache;
import org.apache.sysds.runtime.lineage.LineageCacheConfig;
import org.apache.sysds.runtime.lineage.LineageCacheConfig.ReuseCacheType;
import org.apache.sysds.runtime.lineage.LineageCacheStatistics;
import org.apache.sysds.runtime.lineage.LineageItem;
import org.apache.sysds.runtime.lineage.LineageItemUtils;
import org.apache.sysds.utils.Statistics;

public class FunctionCallCPInstruction extends CPInstruction {
	private static final Log LOG = LogFactory.getLog(FunctionCallCPInstruction.class.getName());
	private final String _functionName;
	private final String _namespace;
	private final boolean _opt;
	private final CPOperand[] _boundInputs;
	private final LineageItem[] _lineageInputs;
	private final List<String> _boundInputNames;
	private final List<String> _funArgNames;
	private final List<String> _boundOutputNames;

	public FunctionCallCPInstruction(String namespace, String functName, boolean opt,
		CPOperand[] boundInputs, LineageItem[] lineageInputs, List<String> funArgNames, 
		List<String> boundOutputNames, String istr) {
		super(CPType.FCall, null, functName, istr);
		_functionName = functName;
		_namespace = namespace;
		_opt = opt;
		_boundInputs = boundInputs;
		_lineageInputs = lineageInputs;
		_boundInputNames = Arrays.stream(boundInputs).map(i -> i.getName())
			.collect(Collectors.toCollection(ArrayList::new));
		_funArgNames = funArgNames;
		_boundOutputNames = boundOutputNames;
	}

	public FunctionCallCPInstruction(String namespace, String functName, boolean opt,
		CPOperand[] boundInputs, List<String> funArgNames, List<String> boundOutputNames, String istr) {
		this(namespace, functName, opt, boundInputs, null, funArgNames, boundOutputNames, istr);
	}

	public String getFunctionName() {
		return _functionName;
	}

	public String getNamespace() {
		return _namespace;
	}
	
	public static FunctionCallCPInstruction parseInstruction(String str) {
		//schema: fcall, fnamespace, fname, opt, num inputs, num outputs, inputs (name-value pairs), outputs
		String[] parts = InstructionUtils.getInstructionPartsWithValueType (str);
		String namespace = parts[1];
		String functionName = parts[2];
		boolean opt = Boolean.parseBoolean(parts[3]);
		int numInputs = Integer.valueOf(parts[4]);
		int numOutputs = Integer.valueOf(parts[5]);
		CPOperand[] boundInputs = new CPOperand[numInputs];
		List<String> funArgNames = new ArrayList<>();
		List<String> boundOutputNames = new ArrayList<>();
		for (int i = 0; i < numInputs; i++) {
			String[] nameValue = IOUtilFunctions.splitByFirst(parts[6 + i], "=");
			boundInputs[i] = new CPOperand(nameValue[1]);
			funArgNames.add(nameValue[0]);
		}
		for (int i = 0; i < numOutputs; i++)
			boundOutputNames.add(parts[6 + numInputs + i]);
		return new FunctionCallCPInstruction ( namespace, functionName,
			opt, boundInputs, funArgNames, boundOutputNames, str );
	}
	
	@Override
	public Instruction preprocessInstruction(ExecutionContext ec) {
		//default pre-process behavior
		return super.preprocessInstruction(ec);
	}

	@Override
	public void

	processInstruction(ExecutionContext ec) {
		if( LOG.isTraceEnabled() ){
			LOG.trace("Executing instruction : " + toString());
		}
		// get the function program block (stored in the Program object)
		FunctionProgramBlock fpb = ec.getProgram().getFunctionProgramBlock(_namespace, _functionName, _opt);
		// sanity check number of function parameters
		if( _boundInputs.length < fpb.getInputParams().size() ) {

			throw new DMLRuntimeException("fcall "+_functionName+": "
				+ "Number of bound input parameters does not match the function signature "
				+ "("+_boundInputs.length+", but "+fpb.getInputParams().size()+" expected)");
		}

		// check if function outputs can be reused from cache
		LineageItem[] liInputs = _lineageInputs;
		if (_lineageInputs == null)
			liInputs = (LineageCacheConfig.isMultiLevelReuse() || DMLScript.LINEAGE_ESTIMATE) 
				? LineageItemUtils.getLineage(ec, _boundInputs) : null;
		if (!fpb.isNondeterministic() && reuseFunctionOutputs(liInputs, fpb, ec))
			return; //only if all the outputs are found in cache


		// create bindings to formal parameters for given function call
		// These are the bindings passed to the FunctionProgramBlock for function execution 
		LocalVariableMap functionVariables = new LocalVariableMap();
		Lineage lineage = DMLScript.LINEAGE ? new Lineage() : null;
		for( int i=0; i<_boundInputs.length; i++) {
			//error handling non-existing variables
			CPOperand input = _boundInputs[i];
			if( !input.isLiteral() && !ec.containsVariable(input.getName()) ) {
				throw new DMLRuntimeException("Input variable '"+input.getName()+"' not existing on call of " + 
					DMLProgram.constructFunctionKey(_namespace, _functionName) + " (line "+getLineNum()+").");
			}
			//get input matrix/frame/scalar
			String argName = _funArgNames.get(i);
			DataIdentifier currFormalParam = fpb.getInputParam(argName);
			if( currFormalParam == null ) {
				throw new DMLRuntimeException("fcall "+_functionName+": Non-existing named "
					+ "function argument: '"+argName+"' (line "+getLineNum()+").");
			}
			
			Data value = ec.getVariable(input);
			
			//graceful value type conversion for scalar inputs with wrong type
			if( value.getDataType() == DataType.SCALAR
				&& value.getValueType() != currFormalParam.getValueType() ) 
			{
				value = ScalarObjectFactory.createScalarObject(
					currFormalParam.getValueType(), (ScalarObject)value);
			}
			
			//set input parameter
			functionVariables.put(currFormalParam.getName(), value);

			//map lineage to function arguments
			if( lineage != null ) {
				LineageItem litem = _lineageInputs == null ? ec.getLineageItem(input) : _lineageInputs[i];
				lineage.set(currFormalParam.getName(), (litem!=null) ? 
					litem : ec.getLineage().getOrCreate(input));
			}
		}
		
		// Pin the input variables so that they do not get deleted 
		// from pb's symbol table at the end of execution of function
		boolean[] pinStatus = ec.pinVariables(_boundInputNames);
		// Create a symbol table under a new execution context for the function invocation,
		// and copy the function arguments into the created table. 
		ExecutionContext fn_ec = ExecutionContextFactory.createContext(false, false, ec.getProgram());
		if (DMLScript.USE_ACCELERATOR) {
			fn_ec.setGPUContexts(ec.getGPUContexts());
			fn_ec.getGPUContext(0).initializeThread();
		}

		fn_ec.setVariables(functionVariables);
		fn_ec.setLineage(lineage);
		// execute the function block
		long t0 = !ReuseCacheType.isNone()||DMLScript.LINEAGE_ESTIMATE ? System.nanoTime() : 0;
		try {
			fpb._functionName = this._functionName;
			fpb._namespace = this._namespace;
			fpb.execute(fn_ec);
		}
		catch (DMLScriptException e) {
			throw e;
		}
		catch (Exception e){
			String fname = DMLProgram.constructFunctionKey(_namespace, _functionName);
			throw new DMLRuntimeException("error executing function " + fname, e);
		}
		long t1 = !ReuseCacheType.isNone()||DMLScript.LINEAGE_ESTIMATE ? System.nanoTime() : 0;
		
		// cleanup all returned variables w/o binding 
		HashSet<String> expectRetVars = new HashSet<>();
		for(DataIdentifier di : fpb.getOutputParams())
			expectRetVars.add(di.getName());
		
		LocalVariableMap retVars = fn_ec.getVariables();
		for( String varName : new ArrayList<>(retVars.keySet()) ) {
			if( expectRetVars.contains(varName) )
				continue;
			//cleanup unexpected return values to avoid leaks
			fn_ec.cleanupDataObject(fn_ec.removeVariable(varName));
		}
		
		// Unpin the pinned variables
		ec.unpinVariables(_boundInputNames, pinStatus);

		// add the updated binding for each return variable to the variables in original symbol table
		// (with robustness for unbound outputs, i.e., function calls without assignment)
		int numOutputs = Math.min(_boundOutputNames.size(), fpb.getOutputParams().size());
		for (int i=0; i< numOutputs; i++) {
			String boundVarName = _boundOutputNames.get(i);
			String retVarName = fpb.getOutputParams().get(i).getName();
			Data boundValue = retVars.get(retVarName);
			if (boundValue == null)
				throw new DMLRuntimeException("fcall "+_functionName+": "
					+boundVarName + " was not assigned a return value");

			//cleanup existing data bound to output variable name
			Data exdata = ec.removeVariable(boundVarName);
			if( exdata != boundValue )
				ec.cleanupDataObject(exdata);
			
			//add/replace data in symbol table
			ec.setVariable(boundVarName, boundValue);

			//map lineage of function returns back to calling site
			if( lineage != null ) //unchanged ref
				ec.getLineage().set(boundVarName, lineage.get(retVarName));
		}

		//update lineage cache with the functions outputs
		if ((DMLScript.LINEAGE && LineageCacheConfig.isMultiLevelReuse() && !fpb.isNondeterministic())
			|| (LineageCacheConfig.isEstimator() && !fpb.isNondeterministic())) {
			LineageCache.putValue(fpb.getOutputParams(), liInputs, 
					getCacheFunctionName(_functionName, fpb), fn_ec, t1-t0);
			//FIXME: send _boundOutputNames instead of fpb.getOutputParams as 
			//those are already replaced by boundoutput names in the lineage map.
		}
	}

	@Override
	public void postprocessInstruction(ExecutionContext ec) {
		//default post-process behavior
		super.postprocessInstruction(ec);
	}

	@Override
	public void printMe() {
		LOG.debug("ExternalBuiltInFunction: " + this.toString());
	}
	
	public List<String> getBoundOutputParamNames() {
		return _boundOutputNames;
	}

	public String updateInstStringFunctionName(String pattern, String replace)
	{
		//split current instruction
		String[] parts = instString.split(Lop.OPERAND_DELIMITOR);
		if( parts[3].equals(pattern) )
			parts[3] = replace;
		
		//construct and set modified instruction
		StringBuilder sb = new StringBuilder();
		for( String part : parts ) {
			sb.append(part);
			sb.append(Lop.OPERAND_DELIMITOR);
		}

		return sb.substring( 0, sb.length()-Lop.OPERAND_DELIMITOR.length() );
	}

	public CPOperand[] getInputs(){
		return _boundInputs;
	}
	
	private boolean reuseFunctionOutputs(LineageItem[] liInputs, FunctionProgramBlock fpb, ExecutionContext ec) {
		//prepare lineage cache probing
		String funcName = getCacheFunctionName(_functionName, fpb);
		int numOutputs = Math.min(_boundOutputNames.size(), fpb.getOutputParams().size());
		
		//reuse of function outputs
		boolean reuse = LineageCache.reuse(
			_boundOutputNames, fpb.getOutputParams(), numOutputs, liInputs, funcName, ec);

		//statistics maintenance
		if (reuse && DMLScript.STATISTICS) {
			//decrement the call count for this function
			Statistics.maintainCPFuncCallStats(getExtendedOpcode());
			LineageCacheStatistics.incrementFuncHits();
		}
		return reuse;
	}
	
	private static String getCacheFunctionName(String fname, FunctionProgramBlock fpb) {
		return !fpb.hasThreadID() ? fname :
			fname.substring(0, fname.lastIndexOf(Lop.CP_CHILD_THREAD+fpb.getThreadID()));
	}
}
