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


public class FunctionStatement extends Statement
{
		
	private ArrayList<StatementBlock> _body;
	protected String _name;
	protected ArrayList <DataIdentifier> _inputParams;
	protected ArrayList <DataIdentifier> _outputParams;
	protected ArrayList <DataIdentifier> _scopeVariables; // the copied variables of this function scope
	
	@Override
	public Statement rewriteStatement(String prefix) {
		LOG.error(this.printErrorLocation() + "should not call rewriteStatement for FunctionStatement");
		throw new LanguageException(this.printErrorLocation() + "should not call rewriteStatement for FunctionStatement");
	}
	
	public FunctionStatement(){
		 _body = new ArrayList<>();
		 _name = null;
		 _inputParams = new ArrayList<>();
		 _outputParams = new ArrayList<>();
		 _scopeVariables = new ArrayList<>();
	}
	
	public ArrayList<DataIdentifier> getInputParams(){
		return _inputParams;
	}
	
	public ArrayList<DataIdentifier> getOutputParams(){
		return _outputParams;
	}
	
	public ArrayList<DataIdentifier> getScopeVariables() {
		return _scopeVariables;
	}
	
	public void setInputParams(ArrayList<DataIdentifier> inputParams){
		_inputParams = inputParams;
	}
	
	public void setOutputParams(ArrayList<DataIdentifier> outputParams){
		_outputParams = outputParams;
	}
	
	public void setName(String fname){
		_name = fname;
	}
	
	public String getName(){
		return _name;
	}

	public ArrayList<StatementBlock> getBody(){
		return _body;
	}
	
	public void setBody(ArrayList<StatementBlock> body){
		_body = body;
	}
	
	
	@Override
	public boolean controlStatement() {
		return true;
	}

	public void mergeStatementBlocks(){
		_body = StatementBlock.mergeStatementBlocks(_body);
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append(_name + " = ");
		
		sb.append("function ( ");
		
		for (int i=0; i<_inputParams.size(); i++){
			DataIdentifier curr = _inputParams.get(i);
			sb.append(curr.getName());
			if (i < _inputParams.size()-1) sb.append(", ");
		}
		sb.append(") return (");
		
		for (int i=0; i<_outputParams.size(); i++){
			sb.append(_outputParams.get(i).getName());
			if (i < _outputParams.size()-1) sb.append(", ");
		}
		sb.append(") { \n");
		
		for (StatementBlock block : _body){
			sb.append(block.toString());
		}
		sb.append("} \n");
		return sb.toString();
	}

	@Override
	public void initializeforwardLV(VariableSet activeIn) {
		LOG.error(this.printErrorLocation() + "should never call initializeforwardLV for FunctionStatement");
		throw new LanguageException(this.printErrorLocation() + "should never call initializeforwardLV for FunctionStatement");
	}
	
	@Override
	public VariableSet initializebackwardLV(VariableSet lo) {
		LOG.error(this.printErrorLocation() + "should never call initializeforwardLV for FunctionStatement");
		throw new LanguageException(this.printErrorLocation() + "should never call initializeforwardLV for FunctionStatement");
	}
	
	@Override
	public VariableSet variablesRead() {
		LOG.warn(this.printWarningLocation() + " -- should not call variablesRead from FunctionStatement ");
		return new VariableSet();
	}

	@Override
	public VariableSet variablesUpdated() {
		LOG.warn(this.printWarningLocation() + " -- should not call variablesRead from FunctionStatement ");
		return new VariableSet();
	}
}
