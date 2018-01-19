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

#include <memory>
#include <string>

#include "TcapTokenType.h"

using std::shared_ptr;
using std::string;

namespace pdb_detail {
/**
 * A lexcial unit and its token type value.
 *
 * One of the terminals of the TCAP grammar:
 *
 * https://github.com/riceplinygroup/pdb/wiki/TCAP-Grammar
 *
 * Or, "uknown" which has type TcapTokenType::UKNOWN and lexeme "".
 */
class TcapToken {

public:
    /**
     * The content of the token.
     */
    const string lexeme;

    /**
     * The token's type.
     */
    const TcapTokenType tokenType;

    /**
     * Creates a new TcapToken.
     *
     * @param lexeme the token's content
     * @param tokenType the token's type
     * @return a new TcapToken
     */
    TcapToken(const string& lexeme, const TcapTokenType& tokenType);
};

typedef shared_ptr<TcapToken> TcapTokenPtr;
}

#endif  // PDB_TCAPLEXER_LEXEME_H
