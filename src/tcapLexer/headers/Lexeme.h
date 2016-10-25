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
#ifndef PDB_TCAPLEXER_LEXEME_H
#define PDB_TCAPLEXER_LEXEME_H

#include <string>

using std::string;

namespace pdb_detail
{
    /**
     * A token and token type.
     */
    class Lexeme
    {

    public:

        static const int UNKNOWN_TYPE = 0;

        static const int MEMO_TYPE = 1;

        static const int STRING_LITERAL_TYPE = 2;

        static const int IDENTIFIER_TYPE = 3;

        static const int LPAREN_TYPE = 4;

        static const int RPAREN_TYPE = 5;

        static const int EQ_TYPE = 6;

        static const int LOAD_TYPE = 7;

        static const int APPLY_TYPE = 8;

        static const int TO_TYPE = 9;

        static const int LBRACKET_TYPE = 10;

        static const int RBRACKET_TYPE = 11;

        static const int RETAIN_TYPE = 12;

        static const int ALL_TYPE = 13;

        static const int COMMA_TYPE = 14;

        static const int DOUBLE_GT_TYPE = 15;

        static const int BY_TYPE = 16;

        static const int STORE_TYPE = 17;

        static const int FILTER_TYPE = 18;

        static const int NONE_TYPE = 19;

        static const int AT_SIGN_TYPE = 20;

        static const int FUNC_TYPE = 21;

        static const int METHOD_TYPE = 22;

        static const int HOIST_TYPE = 23;

        static const int FROM_TYPE = 24;

        static const int GREATER_THAN_TYPE = 24;

        const int tokenType;

        Lexeme(string token, int tokenType);

        string getToken();

    private:

        string _token;
    };

}

#endif //PDB_TCAPLEXER_LEXEME_H
