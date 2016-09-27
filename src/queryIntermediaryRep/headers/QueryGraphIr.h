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

#include "Handle.h"
#include "Object.h"
#include "PDBVector.h"
#include "QueryNodeIr.h"
#include "SetExpressionIr.h"

using pdb::Handle;
using pdb::Object;
using pdb::Vector;

namespace pdb_detail
{
    class QueryGraphIr : public Object
    {
    public:

        QueryGraphIr(Handle<Vector<Handle<SetExpressionIr>>> sourceNodes, Handle<Vector<Handle<SetExpressionIr>>> sinkNodes)
                : _sourceNodes(sourceNodes), _sinkNodes(sinkNodes)
        {
        }

        uint32_t getSourceNodeCount()
        {
            return _sourceNodes->size();
        }

        Handle<SetExpressionIr> getSourceNode(uint32_t index)
        {
            return _sourceNodes->operator[](index);
        }

        uint32_t getSinkNodeCount()
        {
            return _sinkNodes->size();
        }

        Handle<SetExpressionIr> getSinkNode(uint32_t index)
        {
            return _sinkNodes->operator[](index);
        }


    private:

        Handle<Vector<Handle<SetExpressionIr>>> _sourceNodes;

        Handle<Vector<Handle<SetExpressionIr>>> _sinkNodes;

    };
}

#endif //PDB_QUERYINTERMEDIARYREP_QUERYGRAPHIR_H
