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

package org.apache.sysds.runtime.controlprogram.federated;

import java.io.Serializable;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.sysds.runtime.DMLRuntimeException;

public class FederatedResponse implements Serializable {
	private static final long serialVersionUID = 3142180026498695091L;
	
	public enum Type {
		SUCCESS,
		SUCCESS_EMPTY,
		ERROR,
	}
	
	private FederatedResponse.Type _status;
	private Object[] _data;
	
	public FederatedResponse(FederatedResponse.Type status) {
		this(status, null);
	}
	
	public FederatedResponse(FederatedResponse.Type status, Object[] data) {
		_status = status;
		_data = data;
		if( _status == FederatedResponse.Type.SUCCESS && data == null )
			_status = FederatedResponse.Type.SUCCESS_EMPTY;
	}
	
	public FederatedResponse(FederatedResponse.Type status, Object data) {
		_status = status;
		_data = new Object[] {data};
		if(_status == FederatedResponse.Type.SUCCESS && data == null)
			_status = FederatedResponse.Type.SUCCESS_EMPTY;
	}
	
	public boolean isSuccessful() {
		return _status != FederatedResponse.Type.ERROR;
	}
	
	public String getErrorMessage() {
		return ExceptionUtils.getFullStackTrace( (Exception) _data[0] );
	}
	
	public Object[] getData() throws Exception {
		if ( !isSuccessful() )
			throwExceptionFromResponse(); 
		return _data;
	}

	/**
	 * Checks the data object array for exceptions that occurred in the federated worker
	 * during handling of request. 
	 * @throws Exception the exception retrieved from the data object array 
	 *  or DMLRuntimeException if no exception is provided by the federated worker.
	 */
	public void throwExceptionFromResponse() throws Exception {
		for ( Object potentialException : _data){
			if (potentialException != null && (potentialException instanceof Exception) ){
				throw (Exception) potentialException;
			}
		}
		throw new DMLRuntimeException("Unknown runtime exception in handling of federated request by federated worker.");
	}
}
