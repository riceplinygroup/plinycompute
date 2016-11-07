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
#ifndef PDB_TCAPPARSER_RETAINEXPLICITCLAUSE_H
#define PDB_TCAPPARSER_RETAINEXPLICITCLAUSE_H

#include <memory>
#include <vector>

#include "RetainClause.h"
#include "TcapIdentifier.h"

using std::vector;
using std::shared_ptr;


namespace pdb_detail
{
    class RetainExplicitClause : public RetainClause
    {
    public:

        shared_ptr<vector<TcapIdentifier>> columns;

        RetainExplicitClause(TcapIdentifier column);

        RetainExplicitClause(shared_ptr<vector<TcapIdentifier>> columns);

        bool isAll();

        bool isNone();

        void match(function<void(RetainAllClause &)>, function<void(RetainExplicitClause &)> forExplicit,
                   function<void(RetainNoneClause &)> forNone);
    };
}

#endif //PDB_TCAPPARSER_RETAINEXPLICITCLAUSE_H
