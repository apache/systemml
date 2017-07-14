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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.sysml.api.DMLScript;
import org.apache.sysml.conf.DMLConfig;
import org.apache.sysml.lops.LopProperties;
import org.apache.sysml.lops.Lop;
import org.apache.sysml.lops.LopsException;
import org.apache.sysml.lops.compile.Dag;
import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.ExternalFunctionProgramBlock;
import org.apache.sysml.runtime.controlprogram.ExternalFunctionProgramBlockCP;
import org.apache.sysml.runtime.controlprogram.ForProgramBlock;
import org.apache.sysml.runtime.controlprogram.FunctionProgramBlock;
import org.apache.sysml.runtime.controlprogram.IfProgramBlock;
import org.apache.sysml.runtime.controlprogram.ParForProgramBlock;
import org.apache.sysml.runtime.controlprogram.Program;
import org.apache.sysml.runtime.controlprogram.ProgramBlock;
import org.apache.sysml.runtime.controlprogram.WhileProgramBlock;
import org.apache.sysml.runtime.controlprogram.parfor.ProgramConverter;
import org.apache.sysml.runtime.instructions.CPInstructionParser;
import org.apache.sysml.runtime.instructions.Instruction;
import org.apache.sysml.runtime.instructions.cp.VariableCPInstruction;


public class DMLProgram 
{
	
	private ArrayList<StatementBlock> _blocks;
	private HashMap<String, FunctionStatementBlock> _functionBlocks;
	private HashMap<String,DMLProgram> _namespaces;
	public static String DEFAULT_NAMESPACE = ".defaultNS";
	public static String INTERNAL_NAMESPACE = "_internal"; // used for multi-return builtin functions
	private static final Log LOG = LogFactory.getLog(DMLProgram.class.getName());
	
	public DMLProgram(){
		_blocks = new ArrayList<StatementBlock>();
		_functionBlocks = new HashMap<String,FunctionStatementBlock>();
		_namespaces = new HashMap<String,DMLProgram>();
	}
	
	public HashMap<String,DMLProgram> getNamespaces(){
		return _namespaces;
	}

	public void addStatementBlock(StatementBlock b){
		_blocks.add(b);
	}
	
	public int getNumStatementBlocks(){
		return _blocks.size();
	}

	/**
	 * 
	 * @param fkey   function key as concatenation of namespace and function name 
	 *               (see DMLProgram.constructFunctionKey)
	 * @return function statement block
	 */
	public FunctionStatementBlock getFunctionStatementBlock(String fkey) {
		String[] tmp = splitFunctionKey(fkey);
		return getFunctionStatementBlock(tmp[0], tmp[1]);
	}
	
	public FunctionStatementBlock getFunctionStatementBlock(String namespaceKey, String functionName) {
		DMLProgram namespaceProgram = this.getNamespaces().get(namespaceKey);
		if (namespaceProgram == null)
			return null;
	
		// for the namespace DMLProgram, get the specified function (if exists) in its current namespace
		FunctionStatementBlock retVal = namespaceProgram._functionBlocks.get(functionName);
		return retVal;
	}
	
	public HashMap<String, FunctionStatementBlock> getFunctionStatementBlocks(String namespaceKey) throws LanguageException{
		DMLProgram namespaceProgram = this.getNamespaces().get(namespaceKey);
		if (namespaceProgram == null){
			LOG.error("ERROR: namespace " + namespaceKey + " is undefined");
			throw new LanguageException("ERROR: namespace " + namespaceKey + " is undefined");
		}
		// for the namespace DMLProgram, get the functions in its current namespace
		return namespaceProgram._functionBlocks;
	}
	
	public boolean hasFunctionStatementBlocks() {
		boolean ret = false;
		for( DMLProgram nsProg : _namespaces.values() )
			ret |= !nsProg._functionBlocks.isEmpty();
		
		return ret;
	}
	
	public ArrayList<FunctionStatementBlock> getFunctionStatementBlocks() 
		throws LanguageException
	{
		ArrayList<FunctionStatementBlock> ret = new ArrayList<FunctionStatementBlock>();
		
		for( DMLProgram nsProg : _namespaces.values() )
			ret.addAll(nsProg._functionBlocks.values());
		
		return ret;
	}

	public void addFunctionStatementBlock( String namespace, String fname, FunctionStatementBlock fsb ) 
		throws LanguageException
	{
		DMLProgram namespaceProgram = this.getNamespaces().get(namespace);
		if (namespaceProgram == null)
			throw new LanguageException( "Namespace does not exist." );
		
		namespaceProgram._functionBlocks.put(fname, fsb);
	}
	
	public ArrayList<StatementBlock> getStatementBlocks(){
		return _blocks;
	}
	
	public void setStatementBlocks(ArrayList<StatementBlock> passed){
		_blocks = passed;
	}
	
	public StatementBlock getStatementBlock(int i){
		return _blocks.get(i);
	}

	public void mergeStatementBlocks(){
		_blocks = StatementBlock.mergeStatementBlocks(_blocks);
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		
		// for each namespace, display all functions
		for (String namespaceKey : this.getNamespaces().keySet()){
			
			sb.append("NAMESPACE = " + namespaceKey + "\n");
			DMLProgram namespaceProg = this.getNamespaces().get(namespaceKey);
			
			
			sb.append("FUNCTIONS = ");
			
			for (FunctionStatementBlock fsb : namespaceProg._functionBlocks.values()){
				sb.append(fsb);
				sb.append(", ");
			}
			sb.append("\n");
			sb.append("********************************** \n");
		
		}
		
		sb.append("******** MAIN SCRIPT BODY ******** \n");
		for (StatementBlock b : _blocks){
			sb.append(b);
			sb.append("\n");
		}
		sb.append("********************************** \n");
		return sb.toString();
	}
	
	
	public Program getRuntimeProgram(DMLConfig config) throws IOException, LanguageException, DMLRuntimeException, LopsException {
		
		// constructor resets the set of registered functions
		Program rtprog = new Program();
		
		// for all namespaces, translate function statement blocks into function program blocks
		for (String namespace : _namespaces.keySet()){
		
			for (String fname : getFunctionStatementBlocks(namespace).keySet()){
				// add program block to program
				FunctionStatementBlock fsb = getFunctionStatementBlocks(namespace).get(fname);
				FunctionProgramBlock rtpb = (FunctionProgramBlock)createRuntimeProgramBlock(rtprog, fsb, config);
				rtprog.addFunctionProgramBlock(namespace, fname, rtpb);
				rtpb.setRecompileOnce( fsb.isRecompileOnce() );
			}
		}
		
		// for each top-level block
		for (StatementBlock sb : _blocks) {
		
			// add program block to program
			ProgramBlock rtpb = createRuntimeProgramBlock(rtprog, sb, config);
			rtprog.addProgramBlock(rtpb);
		}
		
		
		return rtprog ;
	}
	
	public ProgramBlock createRuntimeProgramBlock(Program prog, StatementBlock sb, DMLConfig config) 
		throws IOException, LopsException, DMLRuntimeException 
	{
		Dag<Lop> dag = null; 
		Dag<Lop> pred_dag = null;

		ArrayList<Instruction> instruct;
		ArrayList<Instruction> pred_instruct = null;
		
		ProgramBlock retPB = null;
		
		// process While Statement - add runtime program blocks to program
		if (sb instanceof WhileStatementBlock){
		
			// create DAG for loop predicates
			pred_dag = new Dag<Lop>();
			((WhileStatementBlock) sb).get_predicateLops().addToDag(pred_dag);
			
			// create instructions for loop predicates
			pred_instruct = new ArrayList<Instruction>();
			ArrayList<Instruction> pInst = pred_dag.getJobs(null, config);
			for (Instruction i : pInst ) {
				pred_instruct.add(i);
			}
			
			// create while program block
			WhileProgramBlock rtpb = new WhileProgramBlock(prog, pred_instruct);
			
			if (rtpb.getPredicateResultVar() == null) {
				// e.g case : WHILE(continue)
				if ( ((WhileStatementBlock) sb).get_predicateLops().getExecLocation() == LopProperties.ExecLocation.Data ) {
					String resultVar = ((WhileStatementBlock) sb).get_predicateLops().getOutputParameters().getLabel();
					rtpb.setPredicateResultVar( resultVar );
				}
				else {
					LOG.error(sb.printBlockErrorLocation() + "Error in translating the WHILE predicate."); 
					throw new LopsException(sb.printBlockErrorLocation() + "Error in translating the WHILE predicate."); 
			
				}
			}			
			//// process the body of the while statement block ////
			
			WhileStatementBlock wsb = (WhileStatementBlock)sb;
			if (wsb.getNumStatements() > 1){
				LOG.error(wsb.printBlockErrorLocation() + "WhileStatementBlock should only have 1 statement");
				throw new LopsException(wsb.printBlockErrorLocation() + "WhileStatementBlock should only have 1 statement");
			}
			WhileStatement wstmt = (WhileStatement)wsb.getStatement(0);
			for (StatementBlock sblock : wstmt.getBody()){
				
				// process the body
				ProgramBlock childBlock = createRuntimeProgramBlock(prog, sblock, config);
				rtpb.addProgramBlock(childBlock);
			}
			
			// check there are actually Lops in to process (loop stmt body will not have any)
			if (wsb.getLops() != null && !wsb.getLops().isEmpty() ){
				LOG.error(wsb.printBlockErrorLocation() + "WhileStatementBlock should have no Lops");
				throw new LopsException(wsb.printBlockErrorLocation() + "WhileStatementBlock should have no Lops");
			}
			
			
			retPB = rtpb;
			
			//post processing for generating missing instructions
			//retPB = verifyAndCorrectProgramBlock(sb.liveIn(), sb.liveOut(), sb._kill, retPB);
			
			// add statement block
			retPB.setStatementBlock(sb);
			
			// add location information
			retPB.setAllPositions(sb.getBeginLine(), sb.getBeginColumn(), sb.getEndLine(), sb.getEndColumn());
		}
		
		// process If Statement - add runtime program blocks to program
		else if (sb instanceof IfStatementBlock){
		
			// create DAG for loop predicates
			pred_dag = new Dag<Lop>();
			((IfStatementBlock) sb).get_predicateLops().addToDag(pred_dag);
			
			// create instructions for loop predicates
			pred_instruct = new ArrayList<Instruction>();
			ArrayList<Instruction> pInst = pred_dag.getJobs(null, config);
			for (Instruction i : pInst ) {
				pred_instruct.add(i);
			}
			
			// create if program block
			IfProgramBlock rtpb = new IfProgramBlock(prog, pred_instruct);
			
			if (rtpb.getPredicateResultVar() == null ) {
				// e.g case : If(continue)
				if ( ((IfStatementBlock) sb).get_predicateLops().getExecLocation() == LopProperties.ExecLocation.Data ) {
					String resultVar = ((IfStatementBlock) sb).get_predicateLops().getOutputParameters().getLabel();
					rtpb.setPredicateResultVar( resultVar );
				}
				else {
					LOG.error(sb.printBlockErrorLocation() + "Error in translating the IF predicate."); 
					throw new LopsException(sb.printBlockErrorLocation() + "Error in translating the IF predicate."); 
				}
			}
			
			// process the body of the if statement block
			IfStatementBlock isb = (IfStatementBlock)sb;
			if (isb.getNumStatements() > 1){
				LOG.error(isb.printBlockErrorLocation() + "IfStatementBlock should have only 1 statement");
				throw new LopsException(isb.printBlockErrorLocation() + "IfStatementBlock should have only 1 statement");
			}
			IfStatement istmt = (IfStatement)isb.getStatement(0);
			
			// process the if body
			for (StatementBlock sblock : istmt.getIfBody()){
				ProgramBlock childBlock = createRuntimeProgramBlock(prog, sblock, config);
				rtpb.addProgramBlockIfBody(childBlock);
			}
			
			// process the else body
			for (StatementBlock sblock : istmt.getElseBody()){
				ProgramBlock childBlock = createRuntimeProgramBlock(prog, sblock, config);
				rtpb.addProgramBlockElseBody(childBlock); 
			}
			
			// check there are actually Lops in to process (loop stmt body will not have any)
			if (isb.getLops() != null && !isb.getLops().isEmpty() ){
				LOG.error(isb.printBlockErrorLocation() + "IfStatementBlock should have no Lops");
				throw new LopsException(isb.printBlockErrorLocation() + "IfStatementBlock should have no Lops");
			}
			
			retPB = rtpb;
			
			//post processing for generating missing instructions
			//retPB = verifyAndCorrectProgramBlock(sb.liveIn(), sb.liveOut(), sb._kill, retPB);
			
			// add statement block
			retPB.setStatementBlock(sb);
			
			// add location information
			retPB.setAllPositions(sb.getBeginLine(), sb.getBeginColumn(), sb.getEndLine(), sb.getEndColumn());
		}
		
		// process For Statement - add runtime program blocks to program
		// NOTE: applies to ForStatementBlock and ParForStatementBlock
		else if (sb instanceof ForStatementBlock) 
		{ 
			ForStatementBlock fsb = (ForStatementBlock) sb;
			
			// create DAGs for loop predicates 
			Dag<Lop> fromDag = new Dag<Lop>();
			Dag<Lop> toDag = new Dag<Lop>();
			Dag<Lop> incrementDag = new Dag<Lop>();
			if( fsb.getFromHops()!=null )
				fsb.getFromLops().addToDag(fromDag);
			if( fsb.getToHops()!=null )
				fsb.getToLops().addToDag(toDag);		
			if( fsb.getIncrementHops()!=null )
				fsb.getIncrementLops().addToDag(incrementDag);		
				
			// create instructions for loop predicates			
			ArrayList<Instruction> fromInstructions = fromDag.getJobs(null, config);
			ArrayList<Instruction> toInstructions = toDag.getJobs(null, config);
			ArrayList<Instruction> incrementInstructions = incrementDag.getJobs(null, config);		

			// create for program block
			String sbName = null;
			ForProgramBlock rtpb = null;
			IterablePredicate iterPred = fsb.getIterPredicate();
			String [] iterPredData= IterablePredicate.createIterablePredicateVariables(iterPred.getIterVar(),
					                                                                   fsb.getFromLops(), fsb.getToLops(), fsb.getIncrementLops()); 
			
			if( sb instanceof ParForStatementBlock )
			{
				sbName = "ParForStatementBlock";
				rtpb = new ParForProgramBlock(prog, iterPredData,iterPred.getParForParams());
				ParForProgramBlock pfrtpb = (ParForProgramBlock)rtpb;
				pfrtpb.setResultVariables( ((ParForStatementBlock)sb).getResultVariables() );
				pfrtpb.setStatementBlock((ParForStatementBlock)sb); //used for optimization and creating unscoped variables
			}
			else //ForStatementBlock
			{
				sbName = "ForStatementBlock";
				rtpb = new ForProgramBlock(prog, iterPredData);
			}
			 
			rtpb.setFromInstructions(      fromInstructions      );
			rtpb.setToInstructions(        toInstructions        );
			rtpb.setIncrementInstructions( incrementInstructions );
			
			rtpb.setIterablePredicateVars( iterPredData );
			
			// process the body of the for statement block
			if (fsb.getNumStatements() > 1){
				LOG.error(fsb.printBlockErrorLocation() + " "  + sbName + " should have 1 statement" );
				throw new LopsException(fsb.printBlockErrorLocation() + " "  + sbName + " should have 1 statement" );
			}
			ForStatement fs = (ForStatement)fsb.getStatement(0);
			for (StatementBlock sblock : fs.getBody()){
				ProgramBlock childBlock = createRuntimeProgramBlock(prog, sblock, config);
				rtpb.addProgramBlock(childBlock); 
			}
		
			// check there are actually Lops in to process (loop stmt body will not have any)
			if (fsb.getLops() != null && !fsb.getLops().isEmpty()){
				LOG.error(fsb.printBlockErrorLocation() + sbName + " should have no Lops" );
				throw new LopsException(fsb.printBlockErrorLocation() + sbName + " should have no Lops" );
			}
			
			retPB = rtpb;
			
			//post processing for generating missing instructions
			//retPB = verifyAndCorrectProgramBlock(sb.liveIn(), sb.liveOut(), sb._kill, retPB);
			
			// add statement block
			retPB.setStatementBlock(sb);
			
			// add location information
			retPB.setAllPositions(sb.getBeginLine(), sb.getBeginColumn(), sb.getEndLine(), sb.getEndColumn());
		}
		
		// process function statement block - add runtime program blocks to program
		else if (sb instanceof FunctionStatementBlock){
			
			FunctionStatementBlock fsb = (FunctionStatementBlock)sb;
			if (fsb.getNumStatements() > 1){
				LOG.error(fsb.printBlockErrorLocation() + "FunctionStatementBlock should only have 1 statement");
				throw new LopsException(fsb.printBlockErrorLocation() + "FunctionStatementBlock should only have 1 statement");
			}
			FunctionStatement fstmt = (FunctionStatement)fsb.getStatement(0);
			FunctionProgramBlock rtpb = null;
			
			if (fstmt instanceof ExternalFunctionStatement) {
				 // create external function program block
				
				String execType = ((ExternalFunctionStatement) fstmt)
                				    .getOtherParams().get(ExternalFunctionStatement.EXEC_TYPE);
				boolean isCP = (execType.equals(ExternalFunctionStatement.IN_MEMORY)) ? true : false;
				
				String scratchSpaceLoc = null;
				try {
					scratchSpaceLoc = config.getTextValue(DMLConfig.SCRATCH_SPACE);
				} catch (Exception e){
					LOG.error(fsb.printBlockErrorLocation() + "could not retrieve parameter " + DMLConfig.SCRATCH_SPACE + " from DMLConfig");
				}				
				StringBuilder buff = new StringBuilder();
				buff.append(scratchSpaceLoc);
				buff.append(Lop.FILE_SEPARATOR);
				buff.append(Lop.PROCESS_PREFIX);
				buff.append(DMLScript.getUUID());
				buff.append(Lop.FILE_SEPARATOR);
				buff.append(ProgramConverter.CP_ROOT_THREAD_ID);
				buff.append(Lop.FILE_SEPARATOR);
				buff.append("PackageSupport");
				buff.append(Lop.FILE_SEPARATOR);
				String basedir =  buff.toString();
				
				if( isCP )
				{
					
					rtpb = new ExternalFunctionProgramBlockCP(prog, 
									fstmt.getInputParams(), fstmt.getOutputParams(), 
									((ExternalFunctionStatement) fstmt).getOtherParams(),
									basedir );					
				}
				else
				{
					rtpb = new ExternalFunctionProgramBlock(prog, 
									fstmt.getInputParams(), fstmt.getOutputParams(), 
									((ExternalFunctionStatement) fstmt).getOtherParams(),
									basedir);
				}
				
				if (!fstmt.getBody().isEmpty()){
					LOG.error(fstmt.printErrorLocation() + "ExternalFunctionStatementBlock should have no statement blocks in body");
					throw new LopsException(fstmt.printErrorLocation() + "ExternalFunctionStatementBlock should have no statement blocks in body");
				}
			}
			else 
			{
				// create function program block
				rtpb = new FunctionProgramBlock(prog, fstmt.getInputParams(), fstmt.getOutputParams());
				
				// process the function statement body
				for (StatementBlock sblock : fstmt.getBody()){	
					// process the body
					ProgramBlock childBlock = createRuntimeProgramBlock(prog, sblock, config);
					rtpb.addProgramBlock(childBlock);
				}
			}
			
			// check there are actually Lops in to process (loop stmt body will not have any)
			if (fsb.getLops() != null && !fsb.getLops().isEmpty()){
				LOG.error(fsb.printBlockErrorLocation() + "FunctionStatementBlock should have no Lops");
				throw new LopsException(fsb.printBlockErrorLocation() + "FunctionStatementBlock should have no Lops");
			}
			
			retPB = rtpb;
			
			// add location information
			retPB.setAllPositions(sb.getBeginLine(), sb.getBeginColumn(), sb.getEndLine(), sb.getEndColumn());
		}
		else {
	
			// handle general case
			ProgramBlock rtpb = new ProgramBlock(prog);
		
			// DAGs for Lops
			dag = new Dag<Lop>();

			// check there are actually Lops in to process (loop stmt body will not have any)
			if (sb.getLops() != null && !sb.getLops().isEmpty()){
			
				for (Lop l : sb.getLops()) {
					l.addToDag(dag);
				}
				
				// Instructions for Lobs DAGs
				instruct = dag.getJobs(sb, config);
				rtpb.addInstructions(instruct);
			}
			
			/*// TODO: check with Doug
			// add instruction for a function call
			if (sb.getFunctionCallInst() != null){
				rtpb.addInstruction(sb.getFunctionCallInst());
			}*/
			
			retPB = rtpb;
			
			//post processing for generating missing instructions
			//retPB = verifyAndCorrectProgramBlock(sb.liveIn(), sb.liveOut(), sb._kill, retPB);
			
			// add statement block
			retPB.setStatementBlock(sb);
			
			// add location information
			retPB.setAllPositions(sb.getBeginLine(), sb.getBeginColumn(), sb.getEndLine(), sb.getEndColumn());
		}
		
		return retPB;
	}	
	
	/**
	 * Post processing of each created program block in order to adhere to livein/liveout
	 * (currently needed for cleanup (especially for caching) of intermediate results if the last datasink 
	 * is an external function because instructions of external functions are created outside hops/lops,
	 * e.g., X=..., Y=fun(X) and X is not used afterwards )
	 * 
	 * NOTES: 
	 * (1) Rule1: checking livein and liveout is sufficient because the last external function is in its own
	 * programblock anyway.
	 * (2) as we cannot efficiently distinguish if the problematic var is created by an external function
	 * or some other instruction, we generate RMVAR instructions although for vars created by non-CP
	 * external functions RMFILEVAR instructions are required. However, all remaining files in scratch_space
	 * are cleaned after execution anyway.
	 * (3) As an alternative to doing rule 2, we could also check for existing objects in createvar and function invocation
	 * (or generic at program block level) and remove objects of previous iterations accordingly (but objects of last iteration
	 * would still require separate cleanup).
	 * 
	 * TODO: MB: external function invocations should become hops/lops as well (see instruction gen in DMLTranslator), 
	 * (currently not possible at Hops/Lops level due the requirement of multiple outputs for functions) 
	 * TODO: MB: we should in general always leverage livein/liveout during hops/lops generation.
	 * TODO: MB: verify and correct can be removed once everything is integrated in hops/lops generation
	 * 
	 * @param in
	 * @param out
	 * @param pb
	 * @return
	 * @throws DMLRuntimeException 
	 */
	@SuppressWarnings("unused")
	private ProgramBlock verifyAndCorrectProgramBlock(VariableSet in, VariableSet out, VariableSet kill, ProgramBlock pb) 
		throws DMLRuntimeException
	{	
		//RULE 1: if in IN and not in OUT, then there should be an rmvar or rmfilevar inst
		//(currently required for specific cases of external functions)
		for( String varName : in.getVariableNames() )
			if( !out.containsVariable(varName) ) 
			{
				DataType dt = in.getVariable(varName).getDataType();
				if( !(dt==DataType.MATRIX || dt==DataType.UNKNOWN) )
					continue; //skip rm instructions for non-matrix objects
				
				boolean foundRMInst = rContainsRMInstruction(pb, varName);
				
				if( !foundRMInst )
				{
					//create RMVAR instruction and put it into the programblock
					Instruction inst = createCleanupInstruction(varName);
					
					inst.setLocation(in.getVariable(varName));
					
					addCleanupInstruction(pb, inst);

					LOG.trace("Adding instruction (r1) "+inst.toString());
				}		
			}

		//RULE 2: if in KILL and not in IN and not in OUT, then there should be an rmvar or rmfilevar inst
		//(currently required for specific cases of nested loops)
		for( String varName : kill.getVariableNames() )
			if( (!in.containsVariable(varName)) && (!out.containsVariable(varName)) ) 
			{
				DataType dt = kill.getVariable(varName).getDataType();
				if( !(dt==DataType.MATRIX || dt==DataType.UNKNOWN) )
					continue; //skip rm instructions for non-matrix objects
				
				boolean foundRMInst = rContainsRMInstruction(pb, varName);
				
				if( !foundRMInst )
				{
					//create RMVAR instruction and put it into the programblock
					Instruction inst = createCleanupInstruction(varName);
					
					inst.setLocation(kill.getVariable(varName));
					
					//System.out.println("add rvar rule2 "+inst.toString());
					addCleanupInstruction(pb, inst);
					
					LOG.trace("Adding instruction (r2) "+inst.toString());
				}		
			}
		
		return pb;
	}
	
	private Instruction createCleanupInstruction(String varName) 
		throws DMLRuntimeException
	{
		//(example "CP+Lops.OPERAND_DELIMITOR+rmvar+Lops.OPERAND_DELIMITOR+Var7")
		StringBuilder sb = new StringBuilder();
		sb.append("CP");
		sb.append(Lop.OPERAND_DELIMITOR);
		sb.append("rmvar");
		sb.append(Lop.OPERAND_DELIMITOR);
		sb.append(varName);
		String str = sb.toString();
		Instruction inst = CPInstructionParser.parseSingleInstruction( str );
		
		return inst;
	}
	
	/**
	 * Determines if the given program block includes a RMVAR or RMFILEVAR
	 * instruction for the given varName.
	 * 
	 * @param pb program block
	 * @param varName variable name
	 * @return true if program block contains remove instruction for variable
	 */
	private boolean rContainsRMInstruction(ProgramBlock pb, String varName)
	{	
		if (pb instanceof WhileProgramBlock)
		{
			WhileProgramBlock tmp = (WhileProgramBlock)pb;	
			for( ProgramBlock c : tmp.getChildBlocks() )
				if( rContainsRMInstruction(c, varName) )
					return true;
		}
		else if (pb instanceof IfProgramBlock)
		{
			IfProgramBlock tmp = (IfProgramBlock)pb;	
			for( ProgramBlock c : tmp.getChildBlocksIfBody() )
				if( rContainsRMInstruction(c, varName) )
					return true;
			for( ProgramBlock c : tmp.getChildBlocksElseBody() )
				if( rContainsRMInstruction(c, varName) )
					return true;
		}
		else if (pb instanceof ForProgramBlock) //includes ParFORProgramBlock
		{ 
			ForProgramBlock tmp = (ForProgramBlock)pb;	
			for( ProgramBlock c : tmp.getChildBlocks() )
				if( rContainsRMInstruction(c, varName) )
					return true;
		}		
		else if (  pb instanceof FunctionProgramBlock ) //includes ExternalFunctionProgramBlock and ExternalFunctionProgramBlockCP)
		{
			//do nothing
		}
		else 
		{
			for( Instruction inst : pb.getInstructions() )
				if( inst instanceof VariableCPInstruction 
					&& ((VariableCPInstruction) inst).isRemoveVariable(varName) )
					return true;
		}
		
		return false;
	}
	
	/**
	 * Adds the generated cleanup RMVAR instruction to the given program block.
	 * In case of generic (last-level) programblocks it is added to the end of 
	 * the list of instructions, while for complex program blocks it is added to
	 * the end of the list of exit instructions.
	 * 
	 * @param pb program block
	 * @param inst instruction
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	private void addCleanupInstruction( ProgramBlock pb, Instruction inst ) 
		throws DMLRuntimeException
	{
		//System.out.println("Adding rm var instructions: "+inst.toString());
		
		if (pb instanceof WhileProgramBlock)
		{
			WhileProgramBlock wpb = (WhileProgramBlock)pb;
			ArrayList<ProgramBlock> childs = wpb.getChildBlocks();
			if( !childs.get(childs.size()-1).getInstructions().isEmpty() ) //generic last level pb
				childs.get(childs.size()-1).addInstruction(inst);
			else{
				ProgramBlock pbNew = new ProgramBlock(pb.getProgram());
				pbNew.addInstruction(inst);
				childs.add(pbNew); 
			}
		}
		else if (pb instanceof ForProgramBlock) //includes ParFORProgramBlock
		{
			ForProgramBlock wpb = (ForProgramBlock)pb;
			ArrayList<ProgramBlock> childs = wpb.getChildBlocks();
			if( !childs.get(childs.size()-1).getInstructions().isEmpty() ) //generic last level pb
				childs.get(childs.size()-1).addInstruction(inst);
			else{
				ProgramBlock pbNew = new ProgramBlock(pb.getProgram());
				pbNew.addInstruction(inst);
				childs.add(pbNew); 
			}
		}
		else if (pb instanceof IfProgramBlock)
			((IfProgramBlock)pb).addExitInstruction(inst);
		else if (   pb instanceof FunctionProgramBlock )  //includes ExternalFunctionProgramBlock and ExternalFunctionProgramBlockCP)
			; //do nothing
		else 
		{
			pb.addInstruction(inst); //add inst at end of pb	
		}
	}
	
	public static String constructFunctionKey(String fnamespace, String fname)
	{
		return fnamespace + Program.KEY_DELIM + fname;
	}
	
	public static String[] splitFunctionKey(String fkey)
	{
		return fkey.split(Program.KEY_DELIM);
	}
}

