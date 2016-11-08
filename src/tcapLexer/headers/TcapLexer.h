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
#ifndef PDB_TCAPLEXER_TCAPLEXER_H
#define PDB_TCAPLEXER_TCAPLEXER_H

#include "TcapTokenStream.h"

namespace pdb_detail
{
    /**
     * Tokenize a string into a token stream.
     *
     * Unrecognized tokens will be reported as TokenType::UNKNOWN_TYPE
     *
     * @param source the input string
     * @return the tokenization of source
     */
    TcapTokenStream lexTcap(const string &source);
}

#endif //PDB_TCAPLEXER_TCAPLEXER_H
