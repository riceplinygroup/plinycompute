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

#include "Handle.h"
#include "IrBuilder.h"

#include "PDBVector.h"
#include "ProjectionIr.h"
#include "RecordPredicateIr.h"
#include "Selection.h"
#include "SelectionIr.h"
#include "Query.h"
#include "QueryBase.h"
#include "Set.h"
#include "SourceSetNameIr.h"


#include "Employee.h"

using pdb::Handle;
using pdb::Query;
using pdb::QueryAlgo;
using pdb::QueryBase;
using pdb::Selection;
using pdb::Object;
using pdb::Set;
using pdb::String;
using pdb::unsafeCast;
using pdb::Vector;


namespace pdb_detail
{
    shared_ptr<SetExpressionIr> makeSetExpression(Handle<QueryBase> queryNode);

    shared_ptr<SelectionIr> makeSelection(Handle<Selection<Object,Object>> selectAndProject)
    {
        class RecordPredicateFromSelection : public RecordPredicateIr
        {
        public:

            RecordPredicateFromSelection(Handle<Selection<Object,Object>> originalSelection)
                    : _originalSelection(originalSelection)
            {
            }

            Lambda<bool> toLambda(Handle<Object>& inputRecordPlaceholder) //override
            {
                return _originalSelection->getSelection(inputRecordPlaceholder);
            }

        private:

            Handle<Selection<Object,Object>> _originalSelection;

        };


        shared_ptr<SetExpressionIr> selectionInputIr;
        {
            if(selectAndProject->hasInput())
            {
                selectionInputIr = makeSetExpression(selectAndProject->getIthInput(0));
            }
            else
            {
                selectionInputIr = make_shared<SourceSetNameIr>("", "");
            }
        }


        shared_ptr<RecordPredicateFromSelection> predicate =
                make_shared<RecordPredicateFromSelection>(selectAndProject);

        return make_shared<SelectionIr>(selectionInputIr, predicate, selectAndProject->getFilterProcessor());
    }


    shared_ptr<ProjectionIr> makeProjection(Handle<Selection<Object,Object>> selectAndProject)
    {
        shared_ptr<SelectionIr> inputSet = makeSelection(selectAndProject);

        class RecordProjectionFromSelection : public RecordProjectionIr
        {
        public:

            RecordProjectionFromSelection(Handle<Selection<Object,Object>> selection) : _selection(selection)
            {

            }

            Lambda<Handle<Object>> toLambda(Handle<Object> &inputRecordPlaceholder) //override
            {
                return _selection->getProjection(inputRecordPlaceholder);
            }

        private:

            Handle<Selection<Object,Object>> _selection;

        };

        shared_ptr<RecordProjectionFromSelection> predicate =
                make_shared<RecordProjectionFromSelection>(selectAndProject);

        shared_ptr<ProjectionIr> projection =  make_shared<ProjectionIr>(inputSet, predicate);

        if(selectAndProject->getDBName() != "")
        {
            string dbName = selectAndProject->getDBName();
            string setName = selectAndProject->getSetName();
            projection->setMaterializationMode(make_shared<MaterializationModeNamedSet>(dbName, setName));
        }

        return projection;
    }

    shared_ptr<SetExpressionIr> makeSetExpression(Handle<QueryBase> queryNode)
    {
        if(queryNode.isNullPtr())
            return shared_ptr<SetExpressionIr>();

        class Algo : public QueryAlgo
        {
        public:

            Algo(Handle<QueryBase> queryNodeParam) : _queryNode(queryNodeParam)
            {
            }

            virtual void forSelection()
            {
                output = makeProjection(unsafeCast<Selection<Object,Object> ,QueryBase>(_queryNode));
            }

            virtual void forSet()
            {
                Handle<Set<Object>> set = unsafeCast<Set<Object>,QueryBase>(_queryNode);
                int numInputs = set->getNumInputs();

                string dbName = set->getDBName();
                string setName = set->getSetName();

                if(numInputs == 0)
                {
                    output = make_shared<SourceSetNameIr>(dbName, setName);
                }
                else
                {
                    Handle<QueryBase> setInput = set->getIthInput(0);

                    output = makeSetExpression(setInput);

                    output->setMaterializationMode(make_shared<MaterializationModeNamedSet>(dbName, setName));
                }
            }

            shared_ptr<SetExpressionIr> output;

        private:

            Handle<QueryBase> _queryNode;

        } algo(queryNode);

        queryNode->execute(algo);

        return algo.output;
    }

    QueryGraphIr buildIr(Handle<QueryBase> querySink)
    {
        shared_ptr<SetExpressionIr> transSink = makeSetExpression(querySink);

        shared_ptr<vector<shared_ptr<SetExpressionIr>>> sinkNodes = make_shared<vector<shared_ptr<SetExpressionIr>>>();
        sinkNodes->push_back(transSink);

        return QueryGraphIr(sinkNodes);
    }


}