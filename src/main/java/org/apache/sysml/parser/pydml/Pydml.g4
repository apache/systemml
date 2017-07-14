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

grammar Pydml;

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

// This antlr grammar is based on Python 3.3 language reference: https://docs.python.org/3.3/reference/grammar.html

tokens { INDENT, DEDENT }

@lexer::members {
    private boolean debugIndentRules = false;

    // Indentation level stack
    private java.util.Stack<Integer> indents = new java.util.Stack<Integer>();

    // Extra tokens queue (see the NEWLINE rule).
    private java.util.Queue<Token> tokens = new java.util.LinkedList<Token>();

    // Number of opened braces, brackets and parenthesis.
    private int opened = 0;

    // This is only used to set the line number for dedent
    private Token lastToken = null;


    @Override
    public void emit(Token t) {
        if(debugIndentRules)
            System.out.println("Emitted token:" + t);

        super.setToken(t);
        tokens.offer(t);
    }


    @Override
    public Token nextToken() {
        if (_input.LA(1) == EOF && !this.indents.isEmpty()) {
            if(debugIndentRules)
                System.out.println("EOF reached and expecting some DEDENTS, so emitting them");

            tokens.poll();
            this.emit(commonToken(PydmlParser.NEWLINE, "\n"));

            // Now emit as much DEDENT tokens as needed.
            while (!indents.isEmpty()) {
                if(debugIndentRules)
                    System.out.println("Emitting (inserted) DEDENTS");

                this.emit(createDedent());
                indents.pop();
            }
            // Put the EOF back on the token stream.
            this.emit(commonToken(PydmlParser.EOF, "<EOF>"));
        }
        Token next = super.nextToken();
        if (next.getChannel() == Token.DEFAULT_CHANNEL) {
            // Keep track of the last token on the default channel.
            this.lastToken = next;
        }
        Token retVal = tokens.isEmpty() ? next : tokens.poll();

        if(debugIndentRules)
            System.out.println("Returning nextToken: [" + retVal + "]<<" + tokens.isEmpty());

        return retVal;
    }

    private Token createDedent() {
        CommonToken dedent = commonToken(PydmlParser.DEDENT, "");
        dedent.setLine(this.lastToken.getLine());
        return dedent;
    }

    private CommonToken commonToken(int type, String text) {
        // Nike: Main change: This logic was screwed up and was emitting additional 3 characters, so commenting it for now.
        // int start = this.getCharIndex();
        // int stop = start + text.length();
        // return new CommonToken(this._tokenFactorySourcePair, type, DEFAULT_TOKEN_CHANNEL, start, stop);
        return new CommonToken(type, text); // Main change
    }

    // Calculates the indentation level from the spaces:
    // "Tabs are replaced (from left to right) by one to eight spaces
    // such that the total number of characters up to and including
    // the replacement is a multiple of eight [...]"
    // https://docs.python.org/3.1/reference/lexical_analysis.html#indentation
    static int getIndentationCount(String spaces) {
        int count = 0;
        for (char ch : spaces.toCharArray()) {
            switch (ch) {
                case '\t':
                    count += 8 - (count % 8);
                    break;
                default:
                    // A normal space char.
                    count++;
            }
        }
        return count;
    }
}


// 2. Modify this g4 by comparing it with Java:
// - https://pythonconquerstheuniverse.wordpress.com/2009/10/03/python-java-a-side-by-side-comparison/
// - http://www.cs.gordon.edu/courses/cps122/handouts-2014/From%20Python%20to%20Java%20Lecture/A%20Comparison%20of%20the%20Syntax%20of%20Python%20and%20Java.pdf
// - http://cs.joensuu.fi/~pviktor/python/slides/cheatsheet.pdf
// - http://www.interfaceware.com/manual/chameleon/scripts/quickreference.pdf

// DML Program is a list of expression
// For now, we only allow global function definitions (not nested or inside a while block)
programroot: (blocks+=statement | functionBlocks+=functionStatement)*  NEWLINE* EOF;



statement returns [ org.apache.sysml.parser.common.StatementInfo info ]
@init {
       // This actions occurs regardless of how many alternatives in this rule
       $info = new org.apache.sysml.parser.common.StatementInfo();
} :
    // ------------------------------------------
    // ImportStatement
    'source' OPEN_PAREN filePath = STRING CLOSE_PAREN  'as' namespace=ID  NEWLINE      # ImportStatement
    | 'setwd'  OPEN_PAREN pathValue = STRING CLOSE_PAREN NEWLINE                     # PathStatement
    // ------------------------------------------
    // AssignmentStatement
    | targetList=dataIdentifier '=' 'ifdef' OPEN_PAREN commandLineParam=dataIdentifier ','  source=expression CLOSE_PAREN NEWLINE   # IfdefAssignmentStatement
    // ------------------------------------------
    // Treat function call as AssignmentStatement or MultiAssignmentStatement
    // For backward compatibility and also since the behavior of foo() * A + foo() ... where foo returns A
    // Convert FunctionCallIdentifier(paramExprs, ..) -> source
    | // TODO: Throw an informative error if user doesnot provide the optional assignment
    ( targetList=dataIdentifier '=' )? name=ID OPEN_PAREN (paramExprs+=parameterizedExpression (',' paramExprs+=parameterizedExpression)* )? CLOSE_PAREN NEWLINE  # FunctionCallAssignmentStatement
    | OPEN_BRACK targetList+=dataIdentifier (',' targetList+=dataIdentifier)* CLOSE_BRACK '=' name=ID OPEN_PAREN (paramExprs+=parameterizedExpression (',' paramExprs+=parameterizedExpression)* )? CLOSE_PAREN  NEWLINE  # FunctionCallMultiAssignmentStatement
    // {notifyErrorListeners("Too many parentheses");}
    // We don't support block statement
    // | '{' body+=expression ';'* ( body+=expression ';'* )*  '}' # BlockStatement
    // ------------------------------------------
    | targetList=dataIdentifier '=' source=expression NEWLINE   # AssignmentStatement
    // IfStatement
    // | 'if' OPEN_PAREN predicate=expression CLOSE_PAREN (ifBody+=statement ';'* |  NEWLINE INDENT (ifBody+=statement)+  DEDENT )  ('else' (elseBody+=statement ';'* | '{' (elseBody+=statement ';'*)*  '}'))?  # IfStatement
    | 'if' (OPEN_PAREN predicate=expression CLOSE_PAREN | predicate=expression) ':'  NEWLINE INDENT (ifBody+=statement)+  DEDENT (elifBranches += elifBranch)* ('else'  ':'  NEWLINE INDENT (elseBody+=statement)+  DEDENT )?  # IfStatement
    // ------------------------------------------
    // ForStatement & ParForStatement
    | 'for' (OPEN_PAREN iterVar=ID 'in' iterPred=iterablePredicate (',' parForParams+=strictParameterizedExpression)* CLOSE_PAREN |  iterVar=ID 'in' iterPred=iterablePredicate (',' parForParams+=strictParameterizedExpression)* ) ':'  NEWLINE INDENT (body+=statement)+  DEDENT  # ForStatement
    | 'for' (OPEN_PAREN iterVars+=ID  ( ',' iterVars+=ID )* 'in' iterPreds+=expression  ( ',' iterPreds+=expression )* (',' paramExprs+=parameterizedExpression)* CLOSE_PAREN |  iterVars+=ID  ( ',' iterVars+=ID )* 'in' iterPreds+=expression  ( ',' iterPreds+=expression )* (',' paramExprs+=parameterizedExpression)* ) ':'  NEWLINE INDENT (body+=statement)+  DEDENT  # IterableForStatement
    // Convert strictParameterizedExpression to HashMap<String, String> for parForParams
    | 'parfor' (OPEN_PAREN iterVar=ID 'in' iterPred=iterablePredicate (',' parForParams+=strictParameterizedExpression)* CLOSE_PAREN | iterVar=ID 'in' iterPred=iterablePredicate (',' parForParams+=strictParameterizedExpression)* ) ':' NEWLINE INDENT (body+=statement)+  DEDENT  # ParForStatement
    | 'while' ( OPEN_PAREN predicate=expression CLOSE_PAREN | predicate=expression ) ':' NEWLINE INDENT (body+=statement)+  DEDENT  # WhileStatement
    // ------------------------------------------
    | NEWLINE #IgnoreNewLine
;

elifBranch returns [ org.apache.sysml.parser.common.StatementInfo info ]
  @init {
        // This actions occurs regardless of how many alternatives in this rule
        $info = new org.apache.sysml.parser.common.StatementInfo();
  } :
     'elif' (OPEN_PAREN predicate=expression CLOSE_PAREN | predicate=expression) ':'  NEWLINE INDENT (elifBody+=statement)+  DEDENT
;

iterablePredicate returns [ org.apache.sysml.parser.common.ExpressionInfo info ]
  @init {
         // This actions occurs regardless of how many alternatives in this rule
         $info = new org.apache.sysml.parser.common.ExpressionInfo();
  } :
    from=expression ':' to=expression #IterablePredicateColonExpression
    | ID OPEN_PAREN from=expression ',' to=expression (',' increment=expression)? CLOSE_PAREN #IterablePredicateSeqExpression
    ;

functionStatement returns [ org.apache.sysml.parser.common.StatementInfo info ]
@init {
       // This actions occurs regardless of how many alternatives in this rule
       $info = new org.apache.sysml.parser.common.StatementInfo();
} :
    // ------------------------------------------
    // FunctionStatement & ExternalFunctionStatement
    // small change: only allow typed arguments here ... instead of data identifier
    'def' name=ID  OPEN_PAREN ( inputParams+=typedArgNoAssign (',' inputParams+=typedArgNoAssign)* )? CLOSE_PAREN  ( '->' OPEN_PAREN ( outputParams+=typedArgNoAssign (',' outputParams+=typedArgNoAssign)* )? CLOSE_PAREN )? ':' NEWLINE INDENT (body+=statement)+ DEDENT # InternalFunctionDefExpression
    | 'defExternal' name=ID  OPEN_PAREN ( inputParams+=typedArgNoAssign (',' inputParams+=typedArgNoAssign)* )? CLOSE_PAREN  ( '->' OPEN_PAREN ( outputParams+=typedArgNoAssign (',' outputParams+=typedArgNoAssign)* )? CLOSE_PAREN )?   'implemented' 'in' OPEN_PAREN ( otherParams+=strictParameterizedKeyValueString (',' otherParams+=strictParameterizedKeyValueString)* )? CLOSE_PAREN NEWLINE    # ExternalFunctionDefExpression
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
    // IndexedIdentifier -- allows implicit lower and upper bounds
    name=ID OPEN_BRACK (rowLower=expression)? (rowImplicitSlice=':' (rowUpper=expression)?)?  (',' (colLower=expression)? (colImplicitSlice=':' (colUpper=expression)?)?)? CLOSE_BRACK # IndexedExpression
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
    <assoc=right> left=expression op='**' right=expression  # PowerExpression
    // unary plus and minus
    | op=('-'|'+') left=expression                        # UnaryExpression
    // sequence - since we are only using this into for loop => Array not supported
    //| left=expression op=':' right=expression             # SequenceExpression
    // matrix multiply
    // | left=expression op='*' right=expression           # MatrixMulExpression
    // modulus and integer division
    | left=expression op=('//' | '%' ) right=expression # ModIntDivExpression
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
    | left=expression op=('&'|'and') right=expression # BooleanAndExpression
    // boolean or
    | left=expression op=('|'|'or') right=expression # BooleanOrExpression

    // ---------------------------------
    // only applicable for builtin function expressions
    // Add following additional functions and check number of parameters:
    // power, full, matrix, reshape, dot
    // Also take care whether there is y.transpose() => which sometinamespace
    | name=ID OPEN_PAREN (paramExprs+=parameterizedExpression (',' paramExprs+=parameterizedExpression)* )? CLOSE_PAREN ';'*  # BuiltinFunctionExpression

    // 4. Atomic
    | OPEN_PAREN left=expression CLOSE_PAREN                       # AtomicExpression

    // Should you allow indexed expression here ?
    // | OPEN_BRACK targetList+=expression (',' targetList+=expression)* CLOSE_BRACK  # MultiIdExpression

    // | BOOLEAN                                       # ConstBooleanIdExpression
    | 'True'                                        # ConstTrueExpression
    | 'False'                                       # ConstFalseExpression
    | INT                                           # ConstIntIdExpression
    | DOUBLE                                        # ConstDoubleIdExpression
    | STRING                                        # ConstStringIdExpression
    | dataIdentifier                                # DataIdExpression
    // Special
    // | 'NULL' | 'NA' | 'Inf' | 'NaN'
;

typedArgNoAssign : paramName=ID ':' paramType=ml_type ;
parameterizedExpression : (paramName=ID '=')? paramVal=expression;
strictParameterizedExpression : paramName=ID '=' paramVal=expression ;
strictParameterizedKeyValueString : paramName=ID '=' paramVal=STRING ;
// sometimes this is matrix object and sometimes its namespace
ID : (ALPHABET (ALPHABET|DIGIT|'_')*  '.')? ALPHABET (ALPHABET|DIGIT|'_')*
    // Special ID cases:
   // | 'matrix' // --> This is a special case which causes lot of headache
   // | 'scalar' |  'float' | 'int' | 'bool' // corresponds to as.scalar, as.double, as.integer and as.logical
   | 'index.return'
;
// Unfortunately, we have datatype name clashing with builtin function name: matrix :(
// Therefore, ugly work around for checking datatype
ml_type :  valueType | dataType OPEN_BRACK valueType CLOSE_BRACK;
// Note to reduce number of keywords, these are case-sensitive,
// To allow case-insenstive,  'int' becomes: ('i' | 'I') ('n' | 'N') ('t' | 'T')
valueType:
    ID # ValueDataTypeCheck
   //  'int' | 'str' | 'bool' | 'float'
;
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
// LINE_COMMENT : '#' .*? '\r'? '\n' -> skip ;
// MULTILINE_BLOCK_COMMENT : '/*' .*? '*/' -> skip ;
// WHITESPACE : (' ' | '\r' | '\n')+ -> skip ;

OPEN_BRACK : '[' {opened++;};
CLOSE_BRACK : ']' {opened--;};
OPEN_PAREN : '(' {opened++;};
CLOSE_PAREN : ')' {opened--;};
// OPEN_BRACE : '{' {opened++;};
// CLOSE_BRACE : '}' {opened--;};

fragment SPACES : [ \t]+ ;
fragment COMMENT : '#' ~[\r\n]* ;
fragment LINE_JOINING : '\\' SPACES? ( '\r'? '\n' | '\r' ) ;

NEWLINE : ( '\r'? '\n' | '\r' ) SPACES?
{
    String newLine = getText().replaceAll("[^\r\n]+", "");
    String spaces = getText().replaceAll("[\r\n]+", "");
    int next = _input.LA(1);
    if (opened > 0 || next == '\r' || next == '\n' || next == '#') {
        // If we're inside a list or on a blank line, ignore all indents,
        // dedents and line breaks.
        skip();
        if(debugIndentRules) {
            if(next == '\r' || next == '\n') {
                    System.out.println("4.1 Skipping (blank lines)");
            }
            else if(next == '#') {
                System.out.println("4.2 Skipping (comment)");
            }
            else {
                System.out.println("4.2 Skipping something else");
            }
        }
    }
    else {
        emit(commonToken(NEWLINE, newLine));

        int indent = getIndentationCount(spaces);
        int previous = indents.isEmpty() ? 0 : indents.peek();
        if (indent == previous) {
            if(debugIndentRules)
                System.out.println("3. Skipping identation as of same size:" + next);

            // skip indents of the same size as the present indent-size
            skip();
        }
        else if (indent > previous) {
            if(debugIndentRules)
                System.out.println("1. Indent:" + next);

            indents.push(indent);
            emit(commonToken(PydmlParser.INDENT, spaces));
        }
        else {
            // Possibly emit more than 1 DEDENT token.
            while(!indents.isEmpty() && indents.peek() > indent) {
                if(debugIndentRules)
                    System.out.println("2. Dedent:" + next);

                this.emit(createDedent());
                indents.pop();
            }
        }
    }
}
;

SKIP_WS : ( SPACES | COMMENT | LINE_JOINING ) -> skip ;
