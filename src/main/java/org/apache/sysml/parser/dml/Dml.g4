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

grammar Dml;

@header
{
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
}

// DML Program is a list of expression
// For now, we only allow global function definitions (not nested or inside a while block)
programroot: (blocks+=statement | functionBlocks+=functionStatement)* EOF;

statement returns [ org.apache.sysml.parser.common.StatementInfo info ]
@init {
       // This actions occurs regardless of how many alternatives in this rule
       $info = new org.apache.sysml.parser.common.StatementInfo();
} :
    // ------------------------------------------
    // ImportStatement
    'source' '(' filePath = STRING ')'  'as' namespace=ID ';'*       # ImportStatement
    | 'setwd'  '(' pathValue = STRING ')' ';'*                          # PathStatement
    // ------------------------------------------
    // Treat function call as AssignmentStatement or MultiAssignmentStatement
    // For backward compatibility and also since the behavior of foo() * A + foo() ... where foo returns A
    // Convert FunctionCallIdentifier(paramExprs, ..) -> source
    | // TODO: Throw an informative error if user doesnot provide the optional assignment
    ( targetList=dataIdentifier ('='|'<-') )? name=ID '(' (paramExprs+=parameterizedExpression (',' paramExprs+=parameterizedExpression)* )? ')' ';'*  # FunctionCallAssignmentStatement
    | '[' targetList+=dataIdentifier (',' targetList+=dataIdentifier)* ']' ('='|'<-') name=ID '(' (paramExprs+=parameterizedExpression (',' paramExprs+=parameterizedExpression)* )? ')' ';'*  # FunctionCallMultiAssignmentStatement
    // {notifyErrorListeners("Too many parentheses");}
    // ------------------------------------------
    // AssignmentStatement
    | targetList=dataIdentifier op=('<-'|'=') 'ifdef' '(' commandLineParam=dataIdentifier ','  source=expression ')' ';'*   # IfdefAssignmentStatement
    | targetList=dataIdentifier op=('<-'|'=') source=expression ';'*   # AssignmentStatement
    // ------------------------------------------
    // We don't support block statement
    // | '{' body+=expression ';'* ( body+=expression ';'* )*  '}' # BlockStatement
    // ------------------------------------------
    // IfStatement
    | 'if' '(' predicate=expression ')' (ifBody+=statement ';'* | '{' (ifBody+=statement ';'*)*  '}')  ('else' (elseBody+=statement ';'* | '{' (elseBody+=statement ';'*)*  '}'))?  # IfStatement
    // ------------------------------------------
    // ForStatement & ParForStatement
    | 'for' '(' iterVar=ID 'in' iterPred=iterablePredicate (',' parForParams+=strictParameterizedExpression)* ')' (body+=statement ';'* | '{' (body+=statement ';'* )*  '}')  # ForStatement
    | 'for' '(' iterVars+=ID  ( ',' iterVars+=ID )* 'in'  iterPreds+=expression  ( ',' iterPreds+=expression )*  (',' paramExprs+=parameterizedExpression)*  ')' (body+=statement ';'* | '{' (body+=statement ';'* )*  '}')  # IterableForStatement
    // Convert strictParameterizedExpression to HashMap<String, String> for parForParams
    | 'parfor' '(' iterVar=ID 'in' iterPred=iterablePredicate (',' parForParams+=strictParameterizedExpression)* ')' (body+=statement ';'* | '{' (body+=statement ';'*)*  '}')  # ParForStatement
    | 'while' '(' predicate=expression ')' (body+=statement ';'* | '{' (body+=statement ';'*)* '}')  # WhileStatement
    // ------------------------------------------
;

iterablePredicate returns [ org.apache.sysml.parser.common.ExpressionInfo info ]
  @init {
         // This actions occurs regardless of how many alternatives in this rule
         $info = new org.apache.sysml.parser.common.ExpressionInfo();
  } :
    from=expression ':' to=expression #IterablePredicateColonExpression
    | ID '(' from=expression ',' to=expression (',' increment=expression)? ')' #IterablePredicateSeqExpression
    ;

functionStatement returns [ org.apache.sysml.parser.common.StatementInfo info ]
@init {
       // This actions occurs regardless of how many alternatives in this rule
       $info = new org.apache.sysml.parser.common.StatementInfo();
} :
    // ------------------------------------------
    // FunctionStatement & ExternalFunctionStatement
    // small change: only allow typed arguments here ... instead of data identifier
    name=ID ('<-'|'=') 'function' '(' ( inputParams+=typedArgNoAssign (',' inputParams+=typedArgNoAssign)* )? ')'  ( 'return' '(' ( outputParams+=typedArgNoAssign (',' outputParams+=typedArgNoAssign)* )? ')' )? '{' (body+=statement ';'*)* '}' ';'* # InternalFunctionDefExpression
    | name=ID ('<-'|'=') 'externalFunction' '(' ( inputParams+=typedArgNoAssign (',' inputParams+=typedArgNoAssign)* )? ')'  ( 'return' '(' ( outputParams+=typedArgNoAssign (',' outputParams+=typedArgNoAssign)* )? ')' )?   'implemented' 'in' '(' ( otherParams+=strictParameterizedKeyValueString (',' otherParams+=strictParameterizedKeyValueString)* )? ')' ';'*    # ExternalFunctionDefExpression
    // ------------------------------------------
;


// Other data identifiers are typedArgNoAssign, parameterizedExpression and strictParameterizedExpression
dataIdentifier returns [ org.apache.sysml.parser.common.ExpressionInfo dataInfo ]
@init {
       // This actions occurs regardless of how many alternatives in this rule
       $dataInfo = new org.apache.sysml.parser.common.ExpressionInfo();
       // $dataInfo.expr = new org.apache.sysml.parser.DataIdentifier();
} :
    // ------------------------------------------
    // IndexedIdentifier
    name=ID '[' (rowLower=expression (':' rowUpper=expression)?)? ',' (colLower=expression (':' colUpper=expression)?)? ']' # IndexedExpression
    // ------------------------------------------
    | ID                                            # SimpleDataIdentifierExpression
    | COMMANDLINE_NAMED_ID                          # CommandlineParamExpression
    | COMMANDLINE_POSITION_ID                       # CommandlinePositionExpression
;
expression returns [ org.apache.sysml.parser.common.ExpressionInfo info ]
@init {
       // This actions occurs regardless of how many alternatives in this rule
       $info = new org.apache.sysml.parser.common.ExpressionInfo();
       // $info.expr = new org.apache.sysml.parser.BinaryExpression(org.apache.sysml.parser.Expression.BinaryOp.INVALID);
} :
    // ------------------------------------------
    // BinaryExpression
    // power
    <assoc=right> left=expression op='^' right=expression  # PowerExpression
    // unary plus and minus
    | op=('-'|'+') left=expression                        # UnaryExpression
    // sequence - since we are only using this into for
    //| left=expression op=':' right=expression             # SequenceExpression
    // matrix multiply
    | left=expression op='%*%' right=expression           # MatrixMulExpression
    // modulus and integer division
    | left=expression op=('%/%' | '%%' ) right=expression # ModIntDivExpression
    // arithmetic multiply and divide
    | left=expression op=('*'|'/') right=expression       # MultDivExpression
    // arithmetic addition and subtraction
    | left=expression op=('+'|'-') right=expression       # AddSubExpression
    // ------------------------------------------
    // RelationalExpression
    | left=expression op=('>'|'>='|'<'|'<='|'=='|'!=') right=expression # RelationalExpression
    // ------------------------------------------
    // BooleanExpression
    // boolean not
    | op='!' left=expression # BooleanNotExpression
    // boolean and
    | left=expression op=('&'|'&&') right=expression # BooleanAndExpression
    // boolean or
    | left=expression op=('|'|'||') right=expression # BooleanOrExpression

    // ---------------------------------
    // only applicable for builtin function expressions
    | name=ID '(' (paramExprs+=parameterizedExpression (',' paramExprs+=parameterizedExpression)* )? ')' ';'*  # BuiltinFunctionExpression

    // 4. Atomic
    | '(' left=expression ')'                       # AtomicExpression

    // Should you allow indexed expression here ?
    | '[' targetList+=expression (',' targetList+=expression)* ']'  # MultiIdExpression

    // | BOOLEAN                                       # ConstBooleanIdExpression
    | 'TRUE'                                        # ConstTrueExpression
    | 'FALSE'                                       # ConstFalseExpression
    | INT                                           # ConstIntIdExpression
    | DOUBLE                                        # ConstDoubleIdExpression
    | STRING                                        # ConstStringIdExpression
    | dataIdentifier                                # DataIdExpression
    // Special
    // | 'NULL' | 'NA' | 'Inf' | 'NaN'
;

typedArgNoAssign : paramType=ml_type paramName=ID;
parameterizedExpression : (paramName=ID '=')? paramVal=expression;
strictParameterizedExpression : paramName=ID '=' paramVal=expression ;
strictParameterizedKeyValueString : paramName=ID '=' paramVal=STRING ;
ID : (ALPHABET (ALPHABET|DIGIT|'_')*  '::')? ALPHABET (ALPHABET|DIGIT|'_')*
    // Special ID cases:
   // | 'matrix' // --> This is a special case which causes lot of headache
   | 'as.scalar' | 'as.matrix' | 'as.frame' | 'as.double' | 'as.integer' | 'as.logical' | 'index.return' | 'lower.tail'
;
// Unfortunately, we have datatype name clashing with builtin function name: matrix :(
// Therefore, ugly work around for checking datatype
ml_type :  valueType | dataType '[' valueType ']';
// Note to reduce number of keywords, these are case-sensitive,
// To allow case-insenstive,  'int' becomes: ('i' | 'I') ('n' | 'N') ('t' | 'T')
valueType: 'int' | 'integer' | 'string' | 'boolean' | 'double'
            | 'Int' | 'Integer' | 'String' | 'Boolean' | 'Double';
dataType:
        // 'scalar' # ScalarDataTypeDummyCheck
        // |
        ID # MatrixDataTypeCheck //{ if($ID.text.compareTo("matrix") != 0) { notifyErrorListeners("incorrect datatype"); } }
        //|  'matrix' //---> See ID, this causes lot of headache
        ;
INT : DIGIT+  [Ll]?;
// BOOLEAN : 'TRUE' | 'FALSE';
DOUBLE: DIGIT+ '.' DIGIT* EXP? [Ll]?
| DIGIT+ EXP? [Ll]?
| '.' DIGIT+ EXP? [Ll]?
;
DIGIT: '0'..'9';
ALPHABET : [a-zA-Z] ;
fragment EXP : ('E' | 'e') ('+' | '-')? INT ;
COMMANDLINE_NAMED_ID: '$' ALPHABET (ALPHABET|DIGIT|'_')*;
COMMANDLINE_POSITION_ID: '$' DIGIT+;

// supports single and double quoted string with escape characters
STRING: '"' ( ESC | ~[\\"] )*? '"' | '\'' ( ESC | ~[\\'] )*? '\'';
fragment ESC : '\\' [btnfr"'\\] ;
// Comments, whitespaces and new line
LINE_COMMENT : '#' .*? '\r'? '\n' -> skip ;
MULTILINE_BLOCK_COMMENT : '/*' .*? '*/' -> skip ;
WHITESPACE : (' ' | '\t' | '\r' | '\n')+ -> skip ;
