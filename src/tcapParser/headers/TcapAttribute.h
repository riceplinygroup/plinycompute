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
#ifndef PDB_TCAPPARSER_TCAPATTRIBUTE_H
#define PDB_TCAPPARSER_TCAPATTRIBUTE_H

#include "TcapIdentifier.h"
#include "StringLiteral.h"

#include <memory>

using std::shared_ptr;

namespace pdb_detail
{
    /**
     * A key value pair associated with a statement.
     */
    class TcapAttribute
    {
    public:

        /**
         * The key name of the attribute.
         */
        const TcapIdentifier name;

        /**
         * The value of the attribute.
         */
        const StringLiteral value;

        /**
         * Creates a new TCAP attribute.
         *
         * @param name The key name of the attribute.
         * @param value The value of the attribute.
         * @return a new TcapAttribute
         */
        TcapAttribute(const TcapIdentifier &name, const StringLiteral &value);
    };

    typedef shared_ptr<TcapAttribute> TcapAttributePtr;
}

#endif //PDB_TCAPATTRIBUTE_H
