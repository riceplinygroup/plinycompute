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

#include "TcapToken.h"

using std::shared_ptr;
using std::vector;

using pdb_detail::TcapToken;

namespace pdb_detail
{
    /**
     * A sequence of tokens.
     */
    class TcapTokenStream
    {
    public:

        /**
         * @return true if any tokens remain, else false.
         */
        bool hasNext();

        /**
         * Returns the current token and advances the stream to the next token.
         *
         * @return the next token in the stream or an empty string token with type UNKNOWN if all tokens have
         * been consumed.
         */
        TcapToken advance();

        /**
         * Returns the current token of the stream without advancing the stream.
         *
         * @return the next token in the stream or an empty string token with type UNKNOWN if all tokens have
         * been consumed.
         */
        TcapToken peek();

    private:

        /**
         * The index of the current token of the stream in _tokens.
         */
        vector<TcapToken>::size_type _readIndex = 0;

        /**
         * The tokens of the stream.
         */
        shared_ptr<const vector<TcapToken>> _tokens;

        /**
         * Constructs a stream from the given tokens.
         *
         * Throws invalid_argument exception if tokens is null.
         *
         * @param tokens the tokens of the stream. may not be null.
         * @return a new token stream
         */
        // private because throws exception and PDB style guide says no excepsions across API boundaries
        TcapTokenStream(shared_ptr<const vector<TcapToken>> tokens);

        friend TcapTokenStream lexTcap(const string &source); // for access to constructor

    };
}

#endif //PDB_TCAPLEXER_TOKENSTREAM_H
