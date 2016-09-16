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
#ifndef PDB_QUERYINTERMEDIARYREP_PROJECTION_H
#define PDB_QUERYINTERMEDIARYREP_PROJECTION_H

#include <memory>

#include "InterfaceFunctions.h"
#include "RecordProjectionIr.h"
#include "Lambda.h"
#include "QueryNodeIr.h"
#include "UnarySetOperator.h"

using std::shared_ptr;
using std::make_shared;

using pdb::makeObject;
using pdb::Lambda;

namespace pdb_detail
{
    class ProjectionIr : public QueryNodeIr
    {
    public:

        static Handle<ProjectionIr> make(Handle<SetExpressionIr> inputSet, Handle<RecordProjectionIr> projector)
        {
            return makeObject<ProjectionIr>(inputSet, projector);
        }

        ProjectionIr(Handle<SetExpressionIr> inputSet, Handle<RecordProjectionIr> projector)
                : _inputSet(inputSet),  _projector(projector)
        {
        }

        void execute(QueryNodeIrAlgo &algo) override
        {
            algo.forProjection(*this);
        }

        virtual Handle<RecordProjectionIr> getProjector()
        {
            return _projector;
        }

        virtual Handle<SetExpressionIr> getInputSet()
        {
            return _inputSet;
        }


    private:

        /**
         * Records source.
         */
        Handle<SetExpressionIr> _inputSet;

        Handle<RecordProjectionIr> _projector;
    };
}

#endif //PDB_QUERYINTERMEDIARYREP_PROJECTION_H
