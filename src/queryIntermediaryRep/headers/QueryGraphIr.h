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
#ifndef PDB_QUERYINTERMEDIARYREP_QUERYGRAPHIR_H
#define PDB_QUERYINTERMEDIARYREP_QUERYGRAPHIR_H

#include "Object.h"
#include "QueryNodeIr.h"
#include "SetExpressionIr.h"

using std::shared_ptr;
using std::vector;

using pdb::Object;

namespace pdb_detail
{
    class QueryGraphIr
    {
    public:

        QueryGraphIr()
        {
        }

        QueryGraphIr(shared_ptr<vector<shared_ptr<SetExpressionIr>>> sinkNodes) : _sinkNodes(sinkNodes)
        {
        }

        uint32_t getSinkNodeCount()
        {
            return _sinkNodes->size();
        }

        shared_ptr<SetExpressionIr> getSinkNode(uint32_t index)
        {
            return _sinkNodes->operator[](index);
        }

    private:

        shared_ptr<vector<shared_ptr<SetExpressionIr>>> _sinkNodes;

    };

    typedef shared_ptr<QueryGraphIr> QueryGraphIrPtr;
}

#endif //PDB_QUERYINTERMEDIARYREP_QUERYGRAPHIR_H
