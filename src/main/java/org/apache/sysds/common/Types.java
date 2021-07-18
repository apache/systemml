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

package org.apache.sysds.common;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.sysds.runtime.DMLRuntimeException;

public class Types
{
	/**
	 * Execution mode for entire script. 
	 */
	public enum ExecMode { 
		SINGLE_NODE, // execute all matrix operations in CP
		HYBRID,      // execute matrix operations in CP or MR
		SPARK        // execute matrix operations in Spark
	}
	
	/**
	 * Execution type of individual operations.
	 */
	public enum ExecType { CP, CP_FILE, SPARK, GPU, FED, INVALID }

	/**
	 * Data types (tensor, matrix, scalar, frame, object, unknown).
	 */
	public enum DataType {
		TENSOR, MATRIX, SCALAR, FRAME, LIST, UNKNOWN;
		
		public boolean isMatrix() {
			return this == MATRIX;
		}
		public boolean isTensor() {
			return this == TENSOR;
		}
		public boolean isFrame() {
			return this == FRAME;
		}
		public boolean isMatrixOrFrame() {
			return isMatrix() | isFrame();
		}
		public boolean isScalar() {
			return this == SCALAR;
		}
		public boolean isList() {
			return this == LIST;
		}
		public boolean isUnknown() {
			return this == UNKNOWN;
		}
	}

	/**
	 * Value types (int, double, string, boolean, unknown).
	 */
	public enum ValueType {
		UINT8, // Used for parsing in UINT values from numpy.
		FP32, FP64, INT32, INT64, BOOLEAN, STRING, UNKNOWN;
		public boolean isNumeric() {
			return this == UINT8 || this == INT32 || this == INT64 || this == FP32 || this == FP64;
		}
		public boolean isUnknown() {
			return this == UNKNOWN;
		}
		public boolean isPseudoNumeric() {
			return isNumeric() || this == BOOLEAN;
		}
		public String toExternalString() {
			switch(this) {
				case FP32:
				case FP64:    return "DOUBLE";
				case UINT8:
				case INT32:
				case INT64:   return "INT";
				case BOOLEAN: return "BOOLEAN";
				default:      return toString();
			}
		}
		public static ValueType fromExternalString(String value) {
			//for now we support both internal and external strings
			//until we have completely changed the external types
			String lvalue = (value != null) ? value.toUpperCase() : null;
			switch(lvalue) {
				case "FP32":     return FP32;
				case "FP64":
				case "DOUBLE":   return FP64;
				case "UINT8":    return UINT8;
				case "INT32":    return INT32;
				case "INT64":
				case "INT":      return INT64;
				case "BOOLEAN":  return BOOLEAN;
				case "STRING":   return STRING;
				case "UNKNOWN":  return UNKNOWN;
				default:
					throw new DMLRuntimeException("Unknown value type: "+value);
			}
		}
	}
	
	/**
	 * Serialization block types (empty, dense, sparse, ultra-sparse)
	 */
	public enum BlockType{
		EMPTY_BLOCK,
		ULTRA_SPARSE_BLOCK,
		SPARSE_BLOCK,
		DENSE_BLOCK,
	}
	
	/**
	 * Type of builtin or user-defined function with regard to its
	 * number of return variables.
	 */
	public enum ReturnType {
		NO_RETURN,
		SINGLE_RETURN,
		MULTI_RETURN
	}
	
	
	/**
	 * Type of aggregation direction
	 */
	public enum Direction {
		RowCol, // full aggregate
		Row,    // row aggregate (e.g., rowSums)
		Col;    // column aggregate (e.g., colSums)
		
		@Override
		public String toString() {
			switch(this) {
				case RowCol: return "RC";
				case Row:    return "R";
				case Col:    return "C";
				default:
					throw new RuntimeException("Invalid direction type: " + this);
			}
		}
	}

	public enum CorrectionLocationType { 
		NONE, 
		LASTROW, 
		LASTCOLUMN, 
		LASTTWOROWS, 
		LASTTWOCOLUMNS,
		LASTFOURROWS,
		LASTFOURCOLUMNS,
		INVALID;
		
		public int getNumRemovedRowsColumns() {
			return (this==LASTROW || this==LASTCOLUMN) ? 1 :
				(this==LASTTWOROWS || this==LASTTWOCOLUMNS) ? 2 :
				(this==LASTFOURROWS || this==LASTFOURCOLUMNS) ? 4 : 0;
		}
		
		public boolean isRows() {
			return this == LASTROW || this == LASTTWOROWS || this == LASTFOURROWS;
		}
	}
	
	// these values need to match with their native counterparts (spoof cuda ops)
	public enum AggOp {
		SUM(0), SUM_SQ(1), MIN(2), MAX(3),
		PROD(4), SUM_PROD(5),
		TRACE(6), MEAN(7), VAR(8),
		MAXINDEX(9), MININDEX(10),
		COUNT_DISTINCT(11),
		COUNT_DISTINCT_APPROX(12);
		
		@Override
		public String toString() {
			switch(this) {
				case SUM:    return "+";
				case SUM_SQ: return "sq+";
				case PROD:   return "*";
				default:     return name().toLowerCase();
			}
		}
		
		private final int value;
		private final static HashMap<Integer, AggOp> map = new HashMap<>();
		
		AggOp(int value) {
			this.value = value;
		}
		
		static {
			for (AggOp aggOp : AggOp.values()) {
				map.put(aggOp.value, aggOp);
			}
		}
		
		public static AggOp valueOf(int aggOp) {
			return map.get(aggOp);
		}
		
		public int getValue() {
			return value;
		}
	}
	
	// Operations that require 1 operand
	public enum OpOp1 {
		ABS, ACOS, ASIN, ASSERT, ATAN, CAST_AS_SCALAR, CAST_AS_MATRIX,
		CAST_AS_FRAME, CAST_AS_DOUBLE, CAST_AS_INT, CAST_AS_BOOLEAN,
		CEIL, CHOLESKY, COS, COSH, CUMMAX, CUMMIN, CUMPROD, CUMSUM,
		CUMSUMPROD, DETECTSCHEMA, COLNAMES, EIGEN, EXISTS, EXP, FLOOR, INVERSE,
		IQM, ISNA, ISNAN, ISINF, LENGTH, LINEAGE, LOG, NCOL, NOT, NROW,
		MEDIAN, PRINT, ROUND, SIN, SINH, SIGN, SOFTMAX, SQRT, STOP, SVD,
		TAN, TANH, TYPEOF,
		//fused ML-specific operators for performance 
		SPROP, //sample proportion: P * (1 - P)
		SIGMOID, //sigmoid function: 1 / (1 + exp(-X))
		LOG_NZ, //sparse-safe log; ppred(X,0,"!=")*log(X)
		
		COMPRESS, DECOMPRESS, 

		//low-level operators //TODO used?
		MULT2, MINUS1_MULT, MINUS_RIGHT, 
		POW2, SUBTRACT_NZ;
		

		public boolean isScalarOutput() {
			return this == CAST_AS_SCALAR
				|| this == NROW || this == NCOL
				|| this == LENGTH || this == EXISTS
				|| this == IQM || this == LINEAGE
				|| this == MEDIAN;
		}
		
		@Override
		public String toString() {
			switch(this) {
				case CAST_AS_SCALAR:  return "castdts";
				case CAST_AS_MATRIX:  return "castdtm";
				case CAST_AS_FRAME:   return "castdtf";
				case CAST_AS_DOUBLE:  return "castvtd";
				case CAST_AS_INT:     return "castvti";
				case CAST_AS_BOOLEAN: return "castvtb";
				case CUMMAX:          return "ucummax";
				case CUMMIN:          return "ucummin";
				case CUMPROD:         return "ucum*";
				case CUMSUM:          return "ucumk+";
				case CUMSUMPROD:      return "ucumk+*";
				case DETECTSCHEMA:    return "detectSchema";
				case MULT2:           return "*2";
				case NOT:             return "!";
				case POW2:            return "^2";
				case TYPEOF:          return "typeOf";
				default:              return name().toLowerCase();
			}
		}

		//need to be kept consistent with toString
		public static OpOp1 valueOfByOpcode(String opcode) {
			switch(opcode) {
				case "castdts": return CAST_AS_SCALAR;
				case "castdtm": return CAST_AS_MATRIX;
				case "castdtf": return CAST_AS_FRAME;
				case "castvtd": return CAST_AS_DOUBLE;
				case "castvti": return CAST_AS_INT;
				case "castvtb": return CAST_AS_BOOLEAN;
				case "ucummax": return CUMMAX;
				case "ucummin": return CUMMIN;
				case "ucum*":   return CUMPROD;
				case "ucumk+":  return CUMSUM;
				case "ucumk+*": return CUMSUMPROD;
				case "detectSchema":    return DETECTSCHEMA;
				case "*2":      return MULT2;
				case "!":       return NOT;
				case "^2":      return POW2;
				case "typeOf":          return TYPEOF;
				default:        return valueOf(opcode.toUpperCase());
			}
		}
	}

	// Operations that require 2 operands
	public enum OpOp2 {
		AND(true), BITWAND(true), BITWOR(true), BITWSHIFTL(true), BITWSHIFTR(true),
		BITWXOR(true), CBIND(false), CONCAT(false), COV(false), DIV(true),
		DROP_INVALID_TYPE(false), DROP_INVALID_LENGTH(false), EQUAL(true), GREATER(true),
		GREATEREQUAL(true), INTDIV(true), INTERQUANTILE(false), IQM(false), LESS(true),
		LESSEQUAL(true), LOG(true), MAP(false), MAX(true), MEDIAN(false), MIN(true), 
		MINUS(true), MODULUS(true), MOMENT(false), MULT(true), NOTEQUAL(true), OR(true),
		PLUS(true), POW(true), PRINT(false), QUANTILE(false), SOLVE(false), RBIND(false),
		XOR(true),
		//fused ML-specific operators for performance
		MINUS_NZ(false), //sparse-safe minus: X-(mean*ppred(X,0,!=))
		LOG_NZ(false), //sparse-safe log; ppred(X,0,"!=")*log(X,0.5)
		MINUS1_MULT(false); //1-X*Y
		
		private final boolean _validOuter;
		
		private OpOp2(boolean outer) {
			_validOuter = outer;
		}
		
		public boolean isValidOuter() {
			return _validOuter;
		}
		
		@Override
		public String toString() {
			switch(this) {
				case PLUS:         return "+";
				case MINUS:        return "-";
				case MINUS_NZ:     return "-nz";
				case MINUS1_MULT:  return "1-*";
				case MULT:         return "*";
				case DIV:          return "/";
				case MODULUS:      return "%%";
				case INTDIV:       return "%/%";
				case LESSEQUAL:    return "<=";
				case LESS:         return "<";
				case GREATEREQUAL: return ">=";
				case GREATER:      return ">";
				case EQUAL:        return "==";
				case NOTEQUAL:     return "!=";
				case OR:           return "||";
				case AND:          return "&&";
				case POW:          return "^";
				case IQM:          return "IQM";
				case MOMENT:       return "cm";
				case BITWAND:      return "bitwAnd";
				case BITWOR:       return "bitwOr";
				case BITWXOR:      return "bitwXor";
				case BITWSHIFTL:   return "bitwShiftL";
				case BITWSHIFTR:   return "bitwShiftR";
				case DROP_INVALID_TYPE: return "dropInvalidType";
				case DROP_INVALID_LENGTH: return "dropInvalidLength";
				case MAP:          return "_map";
				default:           return name().toLowerCase();
			}
		}
		
		//need to be kept consistent with toString
		public static OpOp2 valueOfByOpcode(String opcode) {
			switch(opcode) {
				case "+":           return PLUS;
				case "-":           return MINUS;
				case "-nz":         return MINUS_NZ;
				case "1-*":         return MINUS1_MULT;
				case "*":           return MULT;
				case "/":           return DIV;
				case "%%":          return MODULUS;
				case "%/%":         return INTDIV;
				case "<=":          return LESSEQUAL;
				case "<":           return LESS;
				case ">=":          return GREATEREQUAL;
				case ">":           return GREATER;
				case "==":          return EQUAL;
				case "!=":          return NOTEQUAL;
				case "||":          return OR;
				case "&&":          return AND;
				case "^":           return POW;
				case "IQM":         return IQM;
				case "cm":          return MOMENT;
				case "bitwAnd":     return BITWAND;
				case "bitwOr":      return BITWOR;
				case "bitwXor":     return BITWXOR;
				case "bitwShiftL":  return BITWSHIFTL;
				case "bitwShiftR":  return BITWSHIFTR;
				case "dropInvalidType": return DROP_INVALID_TYPE;
				case "dropInvalidLength": return DROP_INVALID_LENGTH;
				case "map":         return MAP;
				default:            return valueOf(opcode.toUpperCase());
			}
		}
	}
	
	// Operations that require 3 operands
	public enum OpOp3 {
		QUANTILE, INTERQUANTILE, CTABLE, MOMENT, COV, PLUS_MULT, MINUS_MULT, IFELSE;
		
		@Override
		public String toString() {
			switch(this) {
				case MOMENT:     return "cm";
				case PLUS_MULT:  return "+*";
				case MINUS_MULT: return "-*";
				default:         return name().toLowerCase();
			}
		}
		
		public static OpOp3 valueOfByOpcode(String opcode) {
			switch(opcode) {
				case "cm": return MOMENT;
				case "+*": return PLUS_MULT;
				case "-*": return MINUS_MULT;
				default:   return valueOf(opcode.toUpperCase());
			}
		}
	}
	
	// Operations that require 4 operands
	public enum OpOp4 {
		WSLOSS, //weighted sloss mm
		WSIGMOID, //weighted sigmoid mm
		WDIVMM, //weighted divide mm
		WCEMM, //weighted cross entropy mm
		WUMM; //weighted unary mm
		
		@Override
		public String toString() {
			return name().toLowerCase();
		}
	}
	
	// Operations that require a variable number of operands
	public enum OpOpN {
		PRINTF, CBIND, RBIND, MIN, MAX, PLUS, EVAL, LIST;
		
		public boolean isCellOp() {
			return this == MIN || this == MAX || this == PLUS;
		}
	}
	
	public enum ReOrgOp {
		DIAG, //DIAG_V2M and DIAG_M2V could not be distinguished if sizes unknown
		RESHAPE, REV, SORT, TRANS;
		
		@Override
		public String toString() {
			switch(this) {
				case DIAG:    return "rdiag";
				case TRANS:   return "r'";
				case RESHAPE: return "rshape";
				default:      return name().toLowerCase();
			}
		}
		
		public static ReOrgOp valueOfByOpcode(String opcode) {
			switch(opcode) {
				case "rdiag":  return DIAG;
				case "r'":     return TRANS;
				case "rshape": return RESHAPE;
				default:       return valueOf(opcode.toUpperCase());
			}
		}
	}
	
	public enum ParamBuiltinOp {
		INVALID, CDF, INVCDF, GROUPEDAGG, RMEMPTY, REPLACE, REXPAND,
		LOWER_TRI, UPPER_TRI,
		TRANSFORMAPPLY, TRANSFORMDECODE, TRANSFORMCOLMAP, TRANSFORMMETA,
		TOKENIZE, TOSTRING, LIST, PARAMSERV
	}
	
	public enum OpOpDnn {
		MAX_POOL, MAX_POOL_BACKWARD, AVG_POOL, AVG_POOL_BACKWARD,
		CONV2D, CONV2D_BACKWARD_FILTER, CONV2D_BACKWARD_DATA,
		BIASADD, BIASMULT, BATCH_NORM2D_TEST, CHANNEL_SUMS,
		UPDATE_NESTEROV_X,
		//fused operators
		CONV2D_BIAS_ADD, RELU_MAX_POOL, RELU_MAX_POOL_BACKWARD, RELU_BACKWARD
	}
	
	public enum OpOpDG {
		RAND, SEQ, FRAMEINIT, SINIT, SAMPLE, TIME
	}
	
	public enum OpOpData {
		PERSISTENTREAD, PERSISTENTWRITE, 
		TRANSIENTREAD, TRANSIENTWRITE,
		FUNCTIONOUTPUT, 
		SQLREAD, FEDERATED;
		
		public boolean isTransient() {
			return this == TRANSIENTREAD || this == TRANSIENTWRITE;
		}
		public boolean isPersistent() {
			return this == PERSISTENTREAD || this == PERSISTENTWRITE;
		}
		public boolean isWrite() {
			return this == TRANSIENTWRITE || this == PERSISTENTWRITE;
		}
		public boolean isRead() {
			return this == TRANSIENTREAD || this == PERSISTENTREAD;
		}
		
		@Override
		public String toString() {
			switch(this) {
				case PERSISTENTREAD:  return "PRead";
				case PERSISTENTWRITE: return "PWrite";
				case TRANSIENTREAD:   return "TRead";
				case TRANSIENTWRITE:  return "TWrite";
				case FUNCTIONOUTPUT:  return "FunOut";
				case SQLREAD:         return "Sql";
				case FEDERATED:       return "Fed";
				default:              return "Invalid";
			}
		}
	}

	public enum ReplicationType {
		FULL, // full replication to all workers, broadcast
		NONE,      // one slice per worker, broadcast sliced
		PARTIAL        // one or more slices per worker, one or more copy of each slice
	}

	public enum FileFormat {
		TEXT,   // text cell IJV representation (mm w/o header)
		MM,     // text matrix market IJV representation
		CSV,    // text dense representation
		LIBSVM, // text libsvm sparse row representation
		JSONL,  // text nested JSON (Line) representation
		BINARY, // binary block representation (dense/sparse/ultra-sparse)
		FEDERATED, // A federated matrix
		PROTO,  // protocol buffer representation
		HDF5; // Hierarchical Data Format (HDF)
		
		public boolean isIJV() {
			return this == TEXT || this == MM;
		}
		
		public boolean isTextFormat() {
			return this != BINARY;
		}
		
		public static boolean isTextFormat(String fmt) {
			try {
				return valueOf(fmt.toUpperCase()).isTextFormat();
			}
			catch(Exception ex) {
				return false;
			}
		}
		
		public boolean isDelimitedFormat() {
			return this == CSV || this == LIBSVM;
		}
		
		public static boolean isDelimitedFormat(String fmt) {
			try {
				return valueOf(fmt.toUpperCase()).isDelimitedFormat();
			}
			catch(Exception ex) {
				return false;
			}
		}
		
		@Override
		public String toString() {
			return name().toLowerCase();
		}

		public static FileFormat defaultFormat() {
			return TEXT;
		}
		
		public static String defaultFormatString() {
			return defaultFormat().toString();
		}
		
		public static FileFormat safeValueOf(String fmt) {
			try {
				return valueOf(fmt.toUpperCase());
			}
			catch(Exception ex) {
				throw new DMLRuntimeException("Unknown file format: "+fmt
					+ " (valid values: " + Arrays.toString(FileFormat.values())+")");
			}
		}
	}
	
	/** Common type for both function statement blocks and function program blocks **/
	public static interface FunctionBlock {
		public FunctionBlock cloneFunctionBlock();
	} 
}
