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

#include "IrBuilder.h"

#include "ProjectionIr.h"
#include "RecordPredicateIr.h"
#include "Selection.h"
#include "SelectionIr.h"
#include "Query.h"
#include "QueryBase.h"
#include "Set.h"
#include "SetNameIr.h"


#include "Employee.h"

using pdb::Query;
using pdb::QueryAlgo;
using pdb::QueryBase;
using pdb::Selection;
using pdb::makeObject;
using pdb::Object;
using pdb::Set;
using pdb::String;
using pdb::unsafeCast;


namespace pdb_detail
{

    Handle<SelectionIr> makeSelection(Handle<Selection<Object,Object>> selection); // forward declaration

    Handle<SetExpressionIr> makeSetExpression(Handle<QueryBase> queryNode)
    {
        if(queryNode.isNullPtr())
            return Handle<SetExpressionIr>();

        class Algo : public QueryAlgo
        {
        public:

            Algo(Handle<QueryBase> queryNodeParam) : _queryNode(queryNodeParam)
            {
            }

            virtual void forSelection()
            {
                output = makeSelection(unsafeCast<Selection<Object,Object> ,QueryBase>(_queryNode));
            }

            virtual void forSet()
            {
                Handle<Set<Object>> set = unsafeCast<Set<Object>,QueryBase>(_queryNode);
                Handle<String> dbName = makeObject<String>(set->getDBName());
                Handle<String> setName = makeObject<String>(set->getSetName());

                output = makeObject<SetNameIr>(dbName, setName);
            }

            Handle<SetExpressionIr> output;

        private:

            Handle<QueryBase> _queryNode;

        } algo(queryNode);

        queryNode->execute(algo);

        return algo.output;
    }


    Handle<SelectionIr> makeSelection(Handle<Selection<Object,Object>> selection)
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

        Handle <QueryBase> selectionInput = selection->getIthInput(0);

        Handle<RecordPredicateFromSelection> predicate = makeObject<RecordPredicateFromSelection>(selection);

        Handle<SetExpressionIr> selectionInputIr = makeSetExpression(selectionInput);

        return SelectionIr::make(selectionInputIr, predicate);
    }


    Handle<ProjectionIr> makeProjection(Handle<SetExpressionIr> inputSet, Handle<Selection<Object,Object>> query)
    {
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

        Handle<RecordProjectionFromSelection> predicate = makeObject<RecordProjectionFromSelection>(query);
        return  ProjectionIr::make(inputSet, predicate);
    }

    Handle<QueryNodeIr> buildIr(Handle<QueryBase> query)
    {
        class Algo : public QueryAlgo
        {
        public:

            Algo(Handle<QueryBase> query) : _query(query)
            {
            }

            void forSelection()
            {
                Handle<Selection<Object,Object>> queryAsSelection =
                        unsafeCast<Selection<Object,Object> ,QueryBase>(_query);

                Handle<SelectionIr> selection = makeSelection(queryAsSelection);
                Handle<ProjectionIr> projection = makeProjection(selection, queryAsSelection);
                output = projection;
            }

            void forSet()
            {
                output = makeSetExpression(_query);
            }

            Handle<QueryNodeIr> output;

        private:

            Handle<QueryBase> _query;

        } algo(query);

        query->execute(algo);

        return algo.output;
    }


}