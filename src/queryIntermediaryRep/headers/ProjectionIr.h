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
#include "Selection.h"
#include "SimpleSingleTableQueryProcessor.h"
#include "QueryNodeIr.h"
#include "UnarySetOperator.h"


using std::shared_ptr;
using std::make_shared;

using pdb::Lambda;
using pdb::ProjectionQueryProcessor;
using pdb::QueryBase;
using pdb::Selection;
using pdb::SimpleSingleTableQueryProcessorPtr;

namespace pdb_detail
{
    class ProjectionIr : public SetExpressionIr
    {
    public:


        ProjectionIr()
        {
        }

        ProjectionIr(shared_ptr<SetExpressionIr> inputSet, Handle<QueryBase> originalSelection)
                : _inputSet(inputSet),  _originalSelection(originalSelection)
        {
        }

        string getName() override
        {
            return "ProjectionIr";
        }

        void execute(SetExpressionIrAlgo &algo) override
        {
            algo.forProjection(*this);
        }


        virtual shared_ptr<SetExpressionIr> getInputSet()
        {
            return _inputSet;
        }

        template <class Output, class Input>
        SimpleSingleTableQueryProcessorPtr makeProcessor()
        {
            return make_shared<ProjectionQueryProcessor<Output,Input>>(_originalSelection);
        };


    private:

        /**
         * Records source.
         */
        shared_ptr<SetExpressionIr> _inputSet;

        Handle<QueryBase> _originalSelection;
    };
}

#endif //PDB_QUERYINTERMEDIARYREP_PROJECTION_H
