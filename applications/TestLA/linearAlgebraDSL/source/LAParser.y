/*****************************************************************************
 *                                                                           *
 *  Copyright 2018 Rice University                                           *
 *                                                                           *
 *  Licensed under the Apache License, Version 2.0 (the "License");          *
 *  you may not use this file except in compliance with the License.         *
 *  You may obtain a copy of the License at                                  *
 *                                                                           *
 *      http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                           *
 *  Unless required by applicable law or agreed to in writing, software      *
 *  distributed under the License is distributed on an "AS IS" BASIS,        *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *  See the License for the specific language governing permissions and      *
 *  limitations under the License.                                           *
 *                                                                           *
 *****************************************************************************/
%{
  #include "LAParserHelperFunctions.h"
  #include <stdio.h>  // For fileno()
  #include <stdlib.h> // For malloc()
  #include <string.h> // For strdup()

  #define LAPARSEPRINTFLAG 0

  #if YYBISON
    union YYSTYPE;
    int yylex(union YYSTYPE *, void *);
  #endif

  #define YYDEBUG 1

  typedef void* yyscan_t;

  void yyerror(yyscan_t scanner, struct LAStatementsList **myStatements, const char *s) { printf( "Error: %s\n", s); exit(1); }
%}

%define api.prefix {LA}

%union {
  int intVal;
  double doubleVal;
  char * stringVal;
  struct LAIdentifierNode * myIdentifer;
  struct LAInitializerNode * myInitializer;
  struct LAPrimaryExpressionNode * myPrimaryExp;
  struct LAPostfixExpressionNode * myPostExp;
  struct LAMultiplicativeExpressionNode * myMultiExp;
  struct LAAdditiveExpressionNode * myAddExp;
  struct LAExpressionNode * myExp;
  struct LAStatementNode * myStatement;
  struct LAStatementsList * myList; 
}


%pure-parser
%lex-param {void *scanner}
%parse-param {void *scanner}
%parse-param {struct LAStatementsList **myStatements}



%token END                      0     "end of file"

%token TOKEN_ZEROS              701
%token TOKEN_ONES               702
%token TOKEN_IDENTITY           703
%token TOKEN_LOAD               704
%token TOKEN_TRANSPOSE          711
%token TOKEN_INV                712
%token TOKEN_MULTIPLY           721
%token TOKEN_TRANSPOSEMULTIPLY  722
%token TOKEN_SCALEMULTIPLY      723
%token TOKEN_ADD                731
%token TOKEN_MINUS              732
%token TOKEN_LEFT_BRACKET       741
%token TOKEN_RIGHT_BRACKET      742
%token TOKEN_COMMA              743
%token TOKEN_ASSIGN             744
%token TOKEN_MAX                751
%token TOKEN_MIN                752
%token TOKEN_ROWMAX             753
%token TOKEN_ROWMIN             754
%token TOKEN_ROWSUM             755
%token TOKEN_COLMAX             756
%token TOKEN_COLMIN             757
%token TOKEN_COLSUM             758 
%token TOKEN_DUPLICATEROW       759
%token TOKEN_DUPLICATECOL       760

%token <doubleVal>        DOUBLE                790
%token <intVal>           INTEGER               791
%token <stringVal>        IDENTIFIERLITERAL     792
%token <stringVal>        STRINGLITERAL         793

%type <myIdentifer>       identifier
%type <myInitializer>     initializer
%type <myExp>             expression
%type <myPrimaryExp>      primaryExpression
%type <myPostExp>         postfixExpression
%type <myMultiExp>        multiplicativeExpression
%type <myAddExp>          additiveExpression
%type <myStatement>       statement
%type <myList>            statements
%type <myList>            program 


%start program

%%

program  
  : statements 
  {
    $$ = $1;
    *myStatements = $$;
  }
  ;



statements 
  : statement
  {
    if(LAPARSEPRINTFLAG){
      printf("single statement!\n");
    }
    $$ = makeStatementList($1);
  }
  | statements statement
  {
    if(LAPARSEPRINTFLAG){
      printf("statements\n!");
    }
    $$ = appendStatementList($1,$2);
  }
  ;



statement
  : identifier TOKEN_ASSIGN expression 
  {
    if(LAPARSEPRINTFLAG){
      printf("Assign statement!\n");
    }
    $$ = makeStatement($1,$3);
  }
  ;



expression                 
  : additiveExpression
  {
    $$ = (struct LAExpressionNode *) ($1);
  }
  ;



additiveExpression
  : multiplicativeExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("multiplicativeExpression as additiveExpression\n");
    }
    $$ = makeAdditiveExpressionFromMultiplicativeExpressionSingle($1);
  }

  | additiveExpression TOKEN_ADD multiplicativeExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("add as additiveExpression\n");
    }
    $$ = makeAdditiveExpressionFromMultiplicativeExpressionDouble("add",$1,$3);
  }

  | additiveExpression TOKEN_MINUS multiplicativeExpression
  { 
    if(LAPARSEPRINTFLAG){
      printf("minus as additiveExpression\n");
    }
    $$ = makeAdditiveExpressionFromMultiplicativeExpressionDouble("substract",$1,$3);
  }
  ;



multiplicativeExpression
  : postfixExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("postfixExpression as multiplicativeExpression\n");
    }
    $$ = makeMultiplicativeExpressionFromPostfixExpressionSingle($1);
  }

  | multiplicativeExpression TOKEN_MULTIPLY postfixExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("multiply as multiplicativeExpression\n");
    }
    $$ = makeMultiplicativeExpressionFromPostfixExpressionDouble("multiply", $1, $3);
  }

  | multiplicativeExpression TOKEN_SCALEMULTIPLY postfixExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("scale multiply as multiplicativeExpression\n");
    }
    $$ = makeMultiplicativeExpressionFromPostfixExpressionDouble("scale_multiply", $1, $3);
  }

  | multiplicativeExpression TOKEN_TRANSPOSEMULTIPLY postfixExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("transpose multiply as multiplicativeExpression\n");
    }
    $$ = makeMultiplicativeExpressionFromPostfixExpressionDouble("transpose_multiply", $1, $3);
  }
  ;



postfixExpression
  : primaryExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("primaryExpression as postfixExpression\n");
    }
    $$ = makePostfixExpressionFromPrimaryExpression("none",$1);
  }

  | primaryExpression TOKEN_TRANSPOSE
  {
    if(LAPARSEPRINTFLAG){
      printf("transpose postfixExpression\n");
    }
    $$ = makePostfixExpressionFromPrimaryExpression("transpose",$1);
  }

  | primaryExpression TOKEN_INV
  {
    if(LAPARSEPRINTFLAG){
      printf("inverse postfixExpression\n");
    }
    $$ = makePostfixExpressionFromPrimaryExpression("inverse",$1);
  }
  ;



primaryExpression
  : identifier
  {
    if(LAPARSEPRINTFLAG){
      printf("identifier as primaryExpression\n");
    }
    $$ = makePrimaryExpressionFromIdentifier($1);
  }

  /*
  | constant
  {
    if(LAPARSEPRINTFLAG){
      printf("constant as primaryExpression\n");
    }
  }
  */

  | initializer
  {
    if(LAPARSEPRINTFLAG){
      printf("initializer as primaryExpression\n");
    }
    $$ = makePrimaryExpressionFromInitializer($1);
  }

  | TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("parenthesized Expression\n");
    }
    $$ = makePrimaryExpressionFromExpression("recursive",$2);
  }

  | TOKEN_MAX TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("max function Expression\n");
    }
    $$ = makePrimaryExpressionFromExpression("max",$3);
  }

  | TOKEN_MIN TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("min function Expression\n");
    }
    $$ = makePrimaryExpressionFromExpression("min",$3);
  }

  | TOKEN_ROWMAX TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("rowMax function Expression\n");
    }
    $$ = makePrimaryExpressionFromExpression("rowMax",$3);
  }

  | TOKEN_ROWMIN TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("rowMin function Expression\n");
    }
    $$ = makePrimaryExpressionFromExpression("rowMin",$3);
  }

  | TOKEN_ROWSUM TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("rowSum function Expression\n");
    }
    $$ = makePrimaryExpressionFromExpression("rowSum",$3);
  }

  | TOKEN_COLMAX TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("colMax function Expression\n");
    }
    $$ = makePrimaryExpressionFromExpression("colMax",$3);
  }

  | TOKEN_COLMIN TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("colMin function Expression\n");
    }
    $$ = makePrimaryExpressionFromExpression("colMin",$3);
  }

  | TOKEN_COLSUM TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("colSum function Expression\n");
    }
    $$ = makePrimaryExpressionFromExpression("colSum",$3);
  }
  // suppose expression is a row vector, duplicate to blockColSize, blockColNum 
  | TOKEN_DUPLICATEROW TOKEN_LEFT_BRACKET expression TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("duplicateRow function Expression\n");
    }
    $$ = makePrimaryExpressionFromExpressionDuplicate("duplicateRow",$3,$5,$7);
  }
  // suppose experssion is a col vector, duplicate to blockRowSize, blockRowNum
  | TOKEN_DUPLICATECOL TOKEN_LEFT_BRACKET expression TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("duplicateCol function Expression\n");
    }
    $$ = makePrimaryExpressionFromExpressionDuplicate("duplicateCol",$3,$5,$7);
  }
  ;


// integer order: (blockRowSize, blockColSize, blockRowNum, blockColNum)
initializer
  : TOKEN_ZEROS TOKEN_LEFT_BRACKET INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_RIGHT_BRACKET 
  {
    if(LAPARSEPRINTFLAG){
      printf("zeros(%d, %d, %d, %d)\n",$3,$5,$7,$9);
    }
    $$ = makeZerosInitializer($3,$5,$7,$9);
  }

  | TOKEN_ONES TOKEN_LEFT_BRACKET INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_RIGHT_BRACKET   
  { 
    if(LAPARSEPRINTFLAG){
      printf("ones(%d, %d, %d, %d)\n",$3,$5,$7,$9);
    }
    $$ = makeOnesInitializer($3,$5,$7,$9);
  }

  | TOKEN_IDENTITY TOKEN_LEFT_BRACKET INTEGER TOKEN_COMMA INTEGER TOKEN_RIGHT_BRACKET   
  { 
    if(LAPARSEPRINTFLAG){
      printf("identity(%d, %d)\n",$3,$5);
    }
    $$ = makeIdentityInitializer($3,$5);
  }

  | TOKEN_LOAD TOKEN_LEFT_BRACKET INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA STRINGLITERAL TOKEN_RIGHT_BRACKET 
  {
    if(LAPARSEPRINTFLAG){
      printf("load(%d, %d, %d, %d, %s)\n",$3,$5,$7,$9,$11);
    }
    $$ = makeLoadInitializer($3,$5,$7,$9,$11);
  }
  ;



identifier
  :IDENTIFIERLITERAL
  {
    if(LAPARSEPRINTFLAG){
      printf("Create identifer <%s>\n",$1);
    }
    $$ = makeIdentifier($1);
  }
  ;


/*
constant
  : INTEGER
  | DOUBLE
  ;
*/

%%

int yylex(YYSTYPE *, void *);
