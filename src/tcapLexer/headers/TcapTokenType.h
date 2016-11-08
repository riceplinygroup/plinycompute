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
#ifndef PDB_TCAPLEXER_TOKENTYPE_H
#define PDB_TCAPLEXER_TOKENTYPE_H

/**
 * The types of TCAP tokens.
 */
enum TcapTokenType
{
     UNKNOWN_TYPE,

     MEMO_TYPE,

     STRING_LITERAL_TYPE,

     IDENTIFIER_TYPE,

     LPAREN_TYPE,

     RPAREN_TYPE,

     EQ_TYPE,

     LOAD_TYPE,

     APPLY_TYPE,

     TO_TYPE,

     LBRACKET_TYPE,

     RBRACKET_TYPE,

     RETAIN_TYPE,

     ALL_TYPE,

     COMMA_TYPE,

     BY_TYPE,

     STORE_TYPE,

     FILTER_TYPE,

     NONE_TYPE,

     AT_SIGN_TYPE,

     FUNC_TYPE,

     METHOD_TYPE,

     HOIST_TYPE,

     FROM_TYPE,

     GREATER_THAN_TYPE
};

#endif //PDB_TCAPLEXER_TOKENTYPE_H
