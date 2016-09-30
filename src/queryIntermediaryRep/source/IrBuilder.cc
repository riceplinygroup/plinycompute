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

#include <map>

#include "Handle.h"
#include "IrBuilder.h"

#include "PDBVector.h"
#include "ProjectionIr.h"
#include "RecordPredicateIr.h"
#include "Selection.h"
#include "SelectionIr.h"
#include "Query.h"
#include "QueryOutput.h"
#include "QueryBase.h"
#include "Set.h"
#include "SourceSetNameIr.h"


#include "Employee.h"

using std::map;

using pdb::Handle;
using pdb::Query;
using pdb::QueryAlgo;
using pdb::QueryBase;
using pdb::QueryOutput;
using pdb::Selection;
using pdb::Object;
using pdb::Set;
using pdb::String;
using pdb::unsafeCast;
using pdb::Vector;




namespace pdb_detail
{
    shared_ptr<SetExpressionIr> makeSetExpression(Handle<QueryBase> queryNode, map<Handle<QueryBase>, shared_ptr<SetExpressionIr>> &alreadyTranslated);

    shared_ptr<SelectionIr> makeSelection(Handle<QueryBase> selectAndProject, map<Handle<QueryBase>, shared_ptr<SetExpressionIr>> &alreadyTranslated)
    {

        shared_ptr<SetExpressionIr> selectionInputIr;
        {
            if(selectAndProject->hasInput())
            {
                selectionInputIr = makeSetExpression(selectAndProject->getIthInput(0), alreadyTranslated);
            }
            else
            {
                selectionInputIr = make_shared<SourceSetNameIr>("", "");
            }
        }

        return make_shared<SelectionIr>(selectionInputIr, selectAndProject);
    }


    shared_ptr<ProjectionIr> makeProjection(Handle<QueryBase> selectAndProject, map<Handle<QueryBase>, shared_ptr<SetExpressionIr>> &alreadyTranslated)
    {
        shared_ptr<SelectionIr> inputSet = makeSelection(selectAndProject, alreadyTranslated);

        shared_ptr<ProjectionIr> projection =  make_shared<ProjectionIr>(inputSet, selectAndProject);

        if(selectAndProject->getDBName().length() > 0)
        {
            string dbName = selectAndProject->getDBName();
            string setName = selectAndProject->getSetName();
            projection->setMaterializationMode(make_shared<MaterializationModeNamedSet>(dbName, setName));
        }

        return projection;
    }


    shared_ptr<SetExpressionIr> makeSetExpression(Handle<QueryBase> queryNode, map<Handle<QueryBase>,
                                                  shared_ptr<SetExpressionIr>> &alreadyTranslated, bool &isPreexistingNode)
    {

        if(queryNode.isNullPtr())
            return shared_ptr<SetExpressionIr>();

        if(alreadyTranslated.count(queryNode) > 0)
        {
            isPreexistingNode = false;
            return alreadyTranslated[queryNode];
        }
        else
        {
            isPreexistingNode = true;
        }

        class Algo : public QueryAlgo
        {
        public:

            Algo(Handle<QueryBase> queryNodeParam, map<Handle<QueryBase>, shared_ptr<SetExpressionIr>> &alreadyTranslated)
                    : _queryNode(queryNodeParam), _alreadyTranslated(alreadyTranslated)
            {
            }

            void forSelection() override
            {
                output = makeProjection(_queryNode, _alreadyTranslated);
            }

            void forSet() override
            {
                int numInputs = _queryNode->getNumInputs();

                string dbName = _queryNode->getDBName();
                string setName = _queryNode->getSetName();

                if(numInputs == 0)
                {
                    output = make_shared<SourceSetNameIr>(dbName, setName);
                }
                else
                {
                    Handle<QueryBase> setInput = _queryNode->getIthInput(0);

                    output = makeSetExpression(setInput, _alreadyTranslated);

                    output->setMaterializationMode(make_shared<MaterializationModeNamedSet>(dbName, setName));
                }
            }

            void forQueryOutput() override
            {

                string dbName = _queryNode->getDBName();
                string setName = _queryNode->getSetName();

                Handle<QueryBase> outputInput = _queryNode->getIthInput(0);

                output = makeSetExpression(outputInput, _alreadyTranslated);

                output->setMaterializationMode(make_shared<MaterializationModeNamedSet>(dbName, setName));
            }

            shared_ptr<SetExpressionIr> output;

        private:

            map<Handle<QueryBase>, shared_ptr<SetExpressionIr>> &_alreadyTranslated;

            Handle<QueryBase> _queryNode;

        } algo(queryNode, alreadyTranslated);

        queryNode->execute(algo);

        alreadyTranslated[queryNode] = algo.output;

        return algo.output;
    }

    shared_ptr<SetExpressionIr> makeSetExpression(Handle<QueryBase> queryNode, map<Handle<QueryBase>,
            shared_ptr<SetExpressionIr>> &alreadyTranslated)
    {
        bool forget;
        return makeSetExpression(queryNode, alreadyTranslated, forget);
    }

    QueryGraphIr buildIr(list<Handle<QueryBase>> &querySinks)
    {
        shared_ptr<vector<shared_ptr<SetExpressionIr>>> sinkNodes = make_shared<vector<shared_ptr<SetExpressionIr>>>();

        map<Handle<QueryBase>, shared_ptr<SetExpressionIr>> alreadyTranslated;

        for (list<Handle<QueryBase>>::const_iterator i = querySinks.begin(); i != querySinks.end(); ++i)
        {
            bool isNewNode;
            shared_ptr<SetExpressionIr> se = makeSetExpression(*i, alreadyTranslated, isNewNode);
            if(isNewNode)
                sinkNodes->push_back(se);

        }


        return QueryGraphIr(sinkNodes);
    }

    QueryGraphIr buildIr(Handle<QueryBase> querySink)
    {
        list<Handle<QueryBase>> sinks = { querySink };

        return buildIr(sinks);
    }


}