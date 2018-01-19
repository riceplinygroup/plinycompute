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
enum TcapTokenType {
    UNKNOWN_TYPE,  // an unexpected lexeme in the string

    STRING_LITERAL_TYPE,  // "\"*"

    IDENTIFIER_TYPE,  // (letter | digit)+   except a keyword

    LPAREN_TYPE,  // (

    RPAREN_TYPE,  // )

    EQ_TYPE,  // =

    LOAD_TYPE,  // load

    APPLY_TYPE,  // apply

    TO_TYPE,  // to

    LBRACKET_TYPE,  // [

    RBRACKET_TYPE,  // ]

    RETAIN_TYPE,  // retain

    ALL_TYPE,  // all

    COMMA_TYPE,  // ,

    BY_TYPE,  // by

    STORE_TYPE,  // store

    FILTER_TYPE,  // filter

    NONE_TYPE,  // none

    AT_SIGN_TYPE,  // @

    FUNC_TYPE,  // func

    METHOD_TYPE,  // method

    HOIST_TYPE,  // hoist

    FROM_TYPE,  // from

    GREATER_THAN_TYPE  // >
};

#endif  // PDB_TCAPLEXER_TOKENTYPE_H
