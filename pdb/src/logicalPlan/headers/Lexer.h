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
#ifndef _LABCEXER_H_
#define _LABCEXER_H_

#include "LexerTokens.h"

#ifdef __cplusplus
extern "C" {
#endif

struct LexerExtra {
    char errorMessage[500];
};

typedef void* yyscan_t;
int yylex_init_extra(struct LexerExtra*, yyscan_t*);
int yylex_destroy(yyscan_t);

typedef struct yy_buffer_state* YY_BUFFER_STATE;
YY_BUFFER_STATE yy_scan_string(const char*, yyscan_t);
void yy_delete_buffer(YY_BUFFER_STATE, yyscan_t);

struct AtomicComputationList;
void yyerror(yyscan_t, struct AtomicComputationList** myStatement, const char*);

// union YYSTYPE;
typedef union
#ifdef __cplusplus
    YYSTYPE
#endif
{
    char* myChar;
    struct AtomicComputationList* myAtomicComputationList;
    struct AtomicComputation* myAtomicComputation;
    struct TupleSpec* myTupleSpec;
    struct AttList* myAttList;
} YYSTYPE;

extern YYSTYPE yylval;
int yylex(YYSTYPE*, yyscan_t);


#ifdef __cplusplus
}
#endif

#endif
