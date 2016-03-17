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

package org.apache.sysml.runtime.io;

import java.io.IOException;
import java.util.List;

import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.matrix.data.FrameBlock;
import org.apache.sysml.runtime.matrix.data.InputInfo;

public class FrameReaderTextCell extends FrameReader
{

	private boolean _isMMFile = false;
	
	public FrameReaderTextCell(InputInfo info)
	{
		_isMMFile = (info == InputInfo.MatrixMarketInputInfo);
	}
	
	/**
	 * 
	 * @param fname
	 * @param schema
	 * @param names
	 * @return
	 * @throws DMLRuntimeException 
	 * @throws IOException 
	 */
	@Override
	public FrameBlock readFrameFromHDFS(String fname, List<ValueType> schema, List<String> names)
			throws IOException, DMLRuntimeException
	{
		//allocate output frame block
		FrameBlock ret = null;
		
		return ret;
	}

}
