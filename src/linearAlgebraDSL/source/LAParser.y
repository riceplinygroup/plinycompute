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
  #include "ASTNode.h"
  #include <stdio.h>  // For fileno()
  #include <stdlib.h> // For malloc()
  #include <string.h> // For strdup()

  #define LAPARSEPRINTFLAG 1

  #if YYBISON
    union YYSTYPE;
    int yylex(union YYSTYPE *, void *);
  #endif

  #define YYDEBUG 1

  typedef void* yyscan_t;

  void yyerror(yyscan_t scanner, const char *s) { printf( "Error: %s\n", s); exit(1); }
%}

%define api.prefix {LA}

%union {
  int intVal;
  double doubleVal;
  char * stringVal;
}


%pure-parser
%lex-param {void *scanner}
%parse-param {void *scanner}




%token END           0     "end of file"

%token TOKEN_ZEROS        701
%token TOKEN_ONES       702
%token TOKEN_IDENTITY     703
%token TOKEN_LOAD              704
%token TOKEN_TRANSPOSE    711
%token TOKEN_INV        712
%token TOKEN_MULTIPLY     721
%token TOKEN_TRANSPOSEMULTIPLY  722
%token TOKEN_ADD        731
%token TOKEN_MINUS        732
%token TOKEN_LEFT_BRACKET   741
%token TOKEN_RIGHT_BRACKET    742
%token TOKEN_COMMA        743
%token TOKEN_ASSIGN     744
%token TOKEN_MAX        751
%token TOKEN_MIN        752
%token TOKEN_ROWMAX     753
%token TOKEN_ROWMIN     754
%token TOKEN_COLMAX     755
%token TOKEN_COLMIN     756 


%token <doubleVal>        DOUBLE        790
%token <intVal>           INTEGER       791
%token <stringVal>        IDENTIFIER    792
%token <stringVal>        STRINGLITERAL 793




%%

program 
  : END 
  | statements END 
  ;



statements 
  : statement
  {
    if(LAPARSEPRINTFLAG){
      printf("single statement!\n");
    }
  }
  | statements statement
  {
    if(LAPARSEPRINTFLAG){
      printf("statements\n!");
    }
  }
  ;



statement
  : IDENTIFIER TOKEN_ASSIGN expression 
  {
    if(LAPARSEPRINTFLAG){
      printf("Assign statement!\n");
    }
  }
  ;



expression                 
  : additiveExpression
  ;



additiveExpression
  : multiplicativeExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("multiplicativeExpression as additiveExpression\n");
    }
  }

  | additiveExpression TOKEN_ADD multiplicativeExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("add as additiveExpression\n");
    }
  }

  | additiveExpression TOKEN_MINUS multiplicativeExpression
  { 
    if(LAPARSEPRINTFLAG){
      printf("minus as additiveExpression\n");
    }
  }
  ;



multiplicativeExpression
  : postfixExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("postfixExpression as multiplicativeExpression\n");
    }
  }

  | multiplicativeExpression TOKEN_MULTIPLY postfixExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("multiply as multiplicativeExpression\n");
    }
  }

  | multiplicativeExpression TOKEN_TRANSPOSEMULTIPLY postfixExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("transpose multiply as multiplicativeExpression\n");
    }
  }
  ;



postfixExpression
  : primaryExpression
  {
    if(LAPARSEPRINTFLAG){
      printf("primaryExpression as postfixExpression\n");
    } 
  }

  | primaryExpression TOKEN_TRANSPOSE
  {
    if(LAPARSEPRINTFLAG){
      printf("transpose postfixExpression\n");
    } 
  }

  | primaryExpression TOKEN_INV
  {
    if(LAPARSEPRINTFLAG){
      printf("inverse postfixExpression\n");
    } 
  }
  ;



primaryExpression
  : constant
  {
    if(LAPARSEPRINTFLAG){
      printf("constant as primaryExpression\n");
    }
  }

  | IDENTIFIER
  {
    if(LAPARSEPRINTFLAG){
      printf("identifier as primaryExpression\n");
    }
  }

  | initializer
  {
    if(LAPARSEPRINTFLAG){
      printf("initializer as primaryExpression\n");
    }
  }

  | TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("parenthesized Expression\n");
    }
  }

  | TOKEN_MAX TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("max function Expression\n");
    }
  }

  | TOKEN_MIN TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("min function Expression\n");
    }
  }

  | TOKEN_ROWMAX TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("rowMax function Expression\n");
    }
  }

  | TOKEN_ROWMIN TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("rowMin function Expression\n");
    }
  }

  | TOKEN_COLMAX TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("colMax function Expression\n");
    }
  }

  | TOKEN_COLMIN TOKEN_LEFT_BRACKET expression TOKEN_RIGHT_BRACKET
  {
    if(LAPARSEPRINTFLAG){
      printf("colMin function Expression\n");
    }
  }
  ;



initializer
  : TOKEN_ZEROS TOKEN_LEFT_BRACKET INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_RIGHT_BRACKET 
  {
    if(LAPARSEPRINTFLAG){
      printf("zeros(%d, %d, %d, %d)\n",$3,$5,$7,$9);
    }
  }

  | TOKEN_ONES TOKEN_LEFT_BRACKET INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_RIGHT_BRACKET   
  { 
    if(LAPARSEPRINTFLAG){
      printf("ones(%d, %d, %d, %d)\n",$3,$5,$7,$9);
    }
  }

  | TOKEN_IDENTITY TOKEN_LEFT_BRACKET INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA INTEGER TOKEN_RIGHT_BRACKET   
  { 
    if(LAPARSEPRINTFLAG){
      printf("identity(%d, %d, %d, %d)\n",$3,$5,$7,$9);
    }
  }

  | TOKEN_LOAD TOKEN_LEFT_BRACKET INTEGER TOKEN_COMMA INTEGER TOKEN_COMMA STRINGLITERAL TOKEN_RIGHT_BRACKET 
  {
    if(LAPARSEPRINTFLAG){
      printf("load(%d, %d, %s)\n",$3,$5,$7);
    }
  }
  ;



constant
  : INTEGER
  | DOUBLE
  ;

%%

int yylex(YYSTYPE *, void *);