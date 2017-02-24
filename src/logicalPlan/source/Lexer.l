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
/***********************************************************************
 * SECTION 1
 ***********************************************************************/
/* The code in %{ %} is included as it is in lex.yy.c file
 * it has C global variables, prototypes, and comments 
 */

%{

#include "Lexer.h"
#include "ParserHelperFunctions.h"
#include <stdio.h>  // For fileno()
#include <stdlib.h> // For malloc()
#include <string.h> // For strdup()

#define YY_USER_INIT (yylineno = 1)

%}


/******************************************************************************
 * SECTION 2
 ******************************************************************************/
/* This is the DEFINITION section which contains substitutions, code, and
 * start stats; will be copied into lex.yy.c
 */

%option noinput
%option nounput

%option bison-bridge
%option extra-type="struct LexerExtra *"
%option reentrant

/******************************************************************************
 * SECTION 3
 ******************************************************************************/
/* This is the RULES section which defines how to "scan" and what action
 * to take for each token
 */

%%

[Ff][Ii][Ll][Tt][Ee][Rr]	return FILTER;

[Aa][Pp][Pp][Ll][Yy]		return APPLY;

[Ss][Cc][Aa][Nn]		return SCAN;

[Aa][Gg][Gg][Rr][Ee][Gg][Aa][Tt][Ee]	return AGG;

[Jj][Oo][Ii][Nn]		return JOIN;

[Oo][Uu][Tt][Pp][Uu][Tt]	return OUTPUT;

"<="			return GETS;

")"    	        	return (')');

"("    	        	return ('(');

","			return (',');

[A-Za-z][A-Za-z0-9_-]* {yylval->myChar = strdup (yytext);
  			return IDENTIFIER;
			}     

'(\\.|[^'])*'		{char *temp = strdup (yytext); /* strip the open and close quotes */
                         temp[strlen (temp) - 1] = 0;
			 yylval->myChar = strdup (temp + 1);
		         free (temp);	
			 return STRING;
			}

\n                	++yylineno;

[ \t]             	;

.                 	yyerror(yyscanner, 0, "LEX_ERROR: invalid character");

%%

void yyerror(yyscan_t scanner, struct AtomicComputationList **myStatement, const char *s) {
	struct LexerExtra * const extra = yyget_extra (scanner);
	snprintf (extra->errorMessage, sizeof (extra->errorMessage), "error at line %d: %s at %s\n", yyget_lineno (scanner), s, yyget_text (scanner));
}

int yywrap(yyscan_t scanner){
  return 1;
}
