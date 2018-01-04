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


package org.apache.sysml.runtime.matrix.operators;

import org.apache.sysml.runtime.functionobjects.Builtin;
import org.apache.sysml.runtime.functionobjects.Not;
import org.apache.sysml.runtime.functionobjects.ValueFunction;
import org.apache.sysml.runtime.functionobjects.Xor;

public class UnaryOperator extends Operator 
{
	private static final long serialVersionUID = 2441990876648978637L;

	public final ValueFunction fn;
	private final int k; //num threads

	public UnaryOperator(ValueFunction p) {
		this(p, 1); //default single-threaded
	}
	
	public UnaryOperator(ValueFunction p, int numThreads) {
		super(p instanceof Not || (p instanceof Builtin &&
			((Builtin)p).bFunc==Builtin.BuiltinCode.SIN || ((Builtin)p).bFunc==Builtin.BuiltinCode.TAN 
			// sinh and tanh are zero only at zero, else they are nnz
			|| ((Builtin)p).bFunc==Builtin.BuiltinCode.SINH || ((Builtin)p).bFunc==Builtin.BuiltinCode.TANH
			|| ((Builtin)p).bFunc==Builtin.BuiltinCode.ROUND || ((Builtin)p).bFunc==Builtin.BuiltinCode.ABS
			|| ((Builtin)p).bFunc==Builtin.BuiltinCode.SQRT || ((Builtin)p).bFunc==Builtin.BuiltinCode.SPROP
			|| ((Builtin)p).bFunc==Builtin.BuiltinCode.SELP || ((Builtin)p).bFunc==Builtin.BuiltinCode.LOG_NZ
			|| ((Builtin)p).bFunc==Builtin.BuiltinCode.SIGN ));
		fn = p;
		k = numThreads;
	}

	public int getNumThreads() {
		return k;
	}
}
