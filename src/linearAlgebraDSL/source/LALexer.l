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
#include "LALexer.h"
#include <stdio.h>  // For fileno()
#include <stdlib.h> // For malloc()
#include <string.h> // For strdup()

#define LALEXPRINTFLAG 0

%}

%option noinput
%option nounput

%option bison-bridge
%option reentrant
%option prefix="LA"

%%

[ \t\n]          ;

"load"                    { if(LALEXPRINTFLAG){printf("load token!\n");} return (TOKEN_LOAD); }
"zeros"                   { if(LALEXPRINTFLAG){printf("zeros token!\n");} return (TOKEN_ZEROS); }
"ones"                    { if(LALEXPRINTFLAG){printf("ones token!\n");} return (TOKEN_ONES); }
"identity"                { if(LALEXPRINTFLAG){printf("identity token!\n");} return (TOKEN_IDENTITY); }
","                       { if(LALEXPRINTFLAG){printf("comma token!\n");} return (TOKEN_COMMA); }
"rowMax"                  { if(LALEXPRINTFLAG){printf("rowMax token!\n");} return (TOKEN_ROWMAX); }
"rowMin"                  { if(LALEXPRINTFLAG){printf("rowMin token!\n");} return (TOKEN_ROWMIN); }
"rowSum"                  { if(LALEXPRINTFLAG){printf("rowSum token!\n");} return (TOKEN_ROWSUM); }
"colMax"                  { if(LALEXPRINTFLAG){printf("colMax token!\n");} return (TOKEN_COLMAX); }
"colMin"                  { if(LALEXPRINTFLAG){printf("colMin token!\n");} return (TOKEN_COLMIN); }
"colSum"                  { if(LALEXPRINTFLAG){printf("colSum token!\n");} return (TOKEN_COLSUM); }
"max"                     { if(LALEXPRINTFLAG){printf("max token!\n");} return (TOKEN_MAX); }
"min"                     { if(LALEXPRINTFLAG){printf("min token!\n");} return (TOKEN_MIN); }
"duplicateRow"            { if(LALEXPRINTFLAG){printf("duplicateRow token!\n");} return (TOKEN_DUPLICATEROW); }
"duplicateCol"            { if(LALEXPRINTFLAG){printf("duplicateCol token!\n");} return (TOKEN_DUPLICATECOL); }

[0-9]+\.[0-9]+            { 
                            yylval->doubleVal = atof(yytext);
                            if(LALEXPRINTFLAG){printf("double token! <%lf>\n", yylval->doubleVal);}
                            return (DOUBLE);
                          }

[0-9]+                    { 
                            yylval->intVal = atoi( yytext );
                            if(LALEXPRINTFLAG){printf("integer token! <%d>\n",yylval->intVal);}
                            return (INTEGER);
                          }

[A-Za-z][A-Za-z0-9_]*     { 
                            if(LALEXPRINTFLAG){printf("yytext: <%s>\n",yytext);}
                            yylval->stringVal = strdup (yytext);
                            if(LALEXPRINTFLAG){printf("identifier token! <%s>\n",yylval->stringVal);}
                            return (IDENTIFIERLITERAL);
                          }




"="                       { if(LALEXPRINTFLAG){printf("assign token!\n");} return (TOKEN_ASSIGN); }
"+"                       { if(LALEXPRINTFLAG){printf("add token!\n");} return (TOKEN_ADD); } 
"-"                       { if(LALEXPRINTFLAG){printf("minus token!\n");} return (TOKEN_MINUS); }
"'*"                      { if(LALEXPRINTFLAG){printf("transposemultiply token!\n");} return (TOKEN_TRANSPOSEMULTIPLY); }
"*"                       { if(LALEXPRINTFLAG){printf("scalemultiply token!\n");} return (TOKEN_SCALEMULTIPLY); }
"%*%"                     { if(LALEXPRINTFLAG){printf("multiply token!\n");} return (TOKEN_MULTIPLY); }
"^-1"                     { if(LALEXPRINTFLAG){printf("inv token!\n");} return (TOKEN_INV); }
"^T"                      { if(LALEXPRINTFLAG){printf("transpose token!\n");} return (TOKEN_TRANSPOSE); }
"("                       { if(LALEXPRINTFLAG){printf("( token!\n");} return (TOKEN_LEFT_BRACKET); }
")"                       { if(LALEXPRINTFLAG){printf(") token!\n");} return (TOKEN_RIGHT_BRACKET); }


\"([^\\\"]|\\.)*\"        { 
                            if(LALEXPRINTFLAG){printf("yytext: <%s>\n",yytext);}
                            yylval->stringVal = strdup (yytext);
                            if(LALEXPRINTFLAG){printf("stringLiteral token! <%s>\n",yylval->stringVal);}
                            return (STRINGLITERAL);
                          }

.           ;
%%


int yywrap(yyscan_t scanner){
  return 1;
}
