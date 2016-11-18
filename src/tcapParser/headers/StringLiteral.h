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
#ifndef PDB_TCAPPARSER_STRINGLITERAL_H
#define PDB_TCAPPARSER_STRINGLITERAL_H

#include <memory>
#include <string>

using std::shared_ptr;
using std::string;

namespace pdb_detail
{
    /**
     * A TCAP string literal.
     */
    class StringLiteral
    {

    public:

        /**
         * The contents of the literal, without the enclosing quotes.
         */
        const string contents;

        /**
         * Creates a new string literal.
         *
         * @param contents The unquoated contests of the literal.
         * @return A new String literal.
         */
        StringLiteral(const string &contents);

    };
}

#endif //PDB_TCAPPARSER_STRINGLITERAL_H
