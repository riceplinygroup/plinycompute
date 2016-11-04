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

#ifndef PDB_TCAPLEXER_TOKENSTREAM_H
#define PDB_TCAPLEXER_TOKENSTREAM_H

#include <memory>
#include <vector>

#include "Token.h"

using std::shared_ptr;
using std::vector;

using pdb_detail::Token;

namespace pdb_detail
{
    /**
     * A sequence of tokens.
     */
    class TokenStream
    {
    public:

        /**
         * Constructs a stream from the given tokens.
         *
         * @param tokens the tokens of the stream
         * @return a new token stream
         */
        TokenStream(shared_ptr<vector<Token>> tokens);

        /**
         * @return true if any tokens remain, else false.
         */
        bool hasNext();

        /**
         * @return
         */
        Token advance();

        Token peek();

 //useful for debugging.
 //       void printTypes();

    private:

        int _readIndex = 0;

        shared_ptr<vector<Token>> _tokens;

    };
}

#endif //PDB_TCAPLEXER_TOKENSTREAM_H
