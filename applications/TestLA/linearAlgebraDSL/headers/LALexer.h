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
#ifndef LA_LEX_H
#define LA_LEX_H


#include "LALexerTokens.h"

// by Binhang, June 2017

#ifdef __cplusplus
extern "C" {
#endif

typedef void* LAscan_t;

void LAerror(LAscan_t, const char*);
int LAlex_destroy(LAscan_t);
int LAlex_init(LAscan_t*);
void LAset_in(FILE*, LAscan_t);

// union YYSTYPE;
typedef union
#ifdef __cplusplus
    YYSTYPE
#endif
{
    char* stringVal;
    double doubleVal;
    int intVal;
} YYSTYPE;

extern YYSTYPE yylval;

int LAlex(YYSTYPE*, LAscan_t);


#ifdef __cplusplus
}
#endif

#endif