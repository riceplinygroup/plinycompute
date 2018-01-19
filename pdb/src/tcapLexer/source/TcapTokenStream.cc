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
#include <iostream>
#include "TcapTokenStream.h"

using std::invalid_argument;

namespace pdb_detail {

TcapTokenStream::TcapTokenStream(shared_ptr<const vector<TcapToken>> tokens) : _tokens(tokens) {
    if (tokens == nullptr)
        throw invalid_argument("tokens may not be null");
}

bool TcapTokenStream::hasNext() {
    return _readIndex < _tokens->size();
}

TcapToken TcapTokenStream::advance() {
    if (_readIndex >= _tokens->size())
        return TcapToken("", TcapTokenType::UNKNOWN_TYPE);

    return _tokens->operator[](_readIndex++);
}

TcapToken TcapTokenStream::peek() {
    if (_readIndex >= _tokens->size())
        return TcapToken("", TcapTokenType::UNKNOWN_TYPE);

    return _tokens->operator[](_readIndex);
}
}