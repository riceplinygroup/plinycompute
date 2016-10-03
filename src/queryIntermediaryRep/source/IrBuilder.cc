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

#include <cstdint>
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
    shared_ptr<SetExpressionIr> makeSetExpressionHelp(Handle<QueryBase> queryNode, map<Handle<QueryBase>, shared_ptr<SetExpressionIr>> &alreadyTranslated, bool &madeNewNode);

    shared_ptr<SelectionIr> makeSelection(Handle<QueryBase> selectAndProject, map<Handle<QueryBase>, shared_ptr<SetExpressionIr>> &alreadyTranslated, bool &madeNewNode)
    {

        shared_ptr<SetExpressionIr> selectionInputIr;
        {
            if(selectAndProject->hasInput())
            {
                selectionInputIr = makeSetExpressionHelp(selectAndProject->getIthInput(0), alreadyTranslated, madeNewNode);
            }
            else
            {
                selectionInputIr = make_shared<SourceSetNameIr>("", "");
            }
        }

        madeNewNode = true;
        return make_shared<SelectionIr>(selectionInputIr, selectAndProject);
    }


    shared_ptr<ProjectionIr> makeProjection(Handle<QueryBase> selectAndProject, map<Handle<QueryBase>, shared_ptr<SetExpressionIr>> &alreadyTranslated, bool &madeNewNode)
    {
        shared_ptr<SelectionIr> inputSet = makeSelection(selectAndProject, alreadyTranslated, madeNewNode);

        shared_ptr<ProjectionIr> projection =  make_shared<ProjectionIr>(inputSet, selectAndProject);
        madeNewNode = true;

        if(selectAndProject->getDBName().length() > 0)
        {
            string dbName = selectAndProject->getDBName();
            string setName = selectAndProject->getSetName();
            projection->setMaterializationMode(make_shared<MaterializationModeNamedSet>(dbName, setName));
        }

        return projection;
    }


    shared_ptr<SetExpressionIr> makeSetExpressionHelp(Handle<QueryBase> queryNode, map<Handle<QueryBase>,
                                                  shared_ptr<SetExpressionIr>> &alreadyTranslated, bool &madeNewNode)
    {
        if(queryNode.isNullPtr())
            return shared_ptr<SetExpressionIr>();

        if(alreadyTranslated.count(queryNode) > 0)
        {
            return alreadyTranslated[queryNode];
        }

        class Algo : public QueryAlgo
        {
        public:

            Algo(Handle<QueryBase> queryNodeParam,
                 map<Handle<QueryBase>, shared_ptr<SetExpressionIr>> &alreadyTranslated,
                 bool &madeNewNode)
                    : _queryNode(queryNodeParam), _alreadyTranslated(alreadyTranslated), _madeNewNode(madeNewNode)
            {
            }

            void forSelection(Object&) override
            {
                output = makeProjection(_queryNode, _alreadyTranslated, _madeNewNode);
            }

            void forSet(Object&) override
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

                    output = makeSetExpressionHelp(setInput, _alreadyTranslated, _madeNewNode);

                    output->setMaterializationMode(make_shared<MaterializationModeNamedSet>(dbName, setName));
                }
            }

            void forQueryOutput(Object&) override
            {

                string dbName = _queryNode->getDBName();
                string setName = _queryNode->getSetName();

                Handle<QueryBase> outputInput = _queryNode->getIthInput(0);

                output = makeSetExpressionHelp(outputInput, _alreadyTranslated, _madeNewNode);

                output->setMaterializationMode(make_shared<MaterializationModeNamedSet>(dbName, setName));
            }

            shared_ptr<SetExpressionIr> output;

        private:

            map<Handle<QueryBase>, shared_ptr<SetExpressionIr>> &_alreadyTranslated;

            bool &_madeNewNode;

            Handle<QueryBase> _queryNode;

        } algo(queryNode, alreadyTranslated, madeNewNode);

        queryNode->execute(algo);

        alreadyTranslated[queryNode] = algo.output;

        return algo.output;
    }

    shared_ptr<SetExpressionIr> makeSetExpression(Handle<QueryBase> queryNode, map<Handle<QueryBase>,
            shared_ptr<SetExpressionIr>> &alreadyTranslated, bool &madeNewNode)
    {
        madeNewNode = false;
        return makeSetExpressionHelp(queryNode, alreadyTranslated, madeNewNode);
    }


    QueryGraphIr buildIr(function<bool()> hasNext, function<Handle<QueryBase>()> getNextElement)
    {
        shared_ptr<vector<shared_ptr<SetExpressionIr>>> sinkNodes = make_shared<vector<shared_ptr<SetExpressionIr>>>();

        map<Handle<QueryBase>, shared_ptr<SetExpressionIr>> alreadyTranslated;

        while(hasNext())
        {
            Handle<QueryBase> querySink = getNextElement();

            bool isNewNode;
            shared_ptr<SetExpressionIr> se = makeSetExpression(querySink, alreadyTranslated, isNewNode);
            if(isNewNode)
                sinkNodes->push_back(se);
        }


        return QueryGraphIr(sinkNodes);
    }

    QueryGraphIr buildIr(list<Handle<QueryBase>> &querySinks)
    {
        list<Handle<QueryBase>>::const_iterator i = querySinks.begin();

        function<bool()> hasNext = [&]() { return i != querySinks.end(); };
        function<Handle<QueryBase>()> getNextElement = [&]
        {
            Handle<QueryBase> toReturn = *i;
            i++;
            return toReturn;
        };

        return buildIr(hasNext, getNextElement);

    }

    QueryGraphIr buildIr(Handle<Vector<Handle<QueryBase>>> querySinks)
    {
        uint32_t i = 0;
        function<bool()> hasNext = [&]() { return i < querySinks->size(); };
        function<Handle<QueryBase>()> getNextElement = [&]
        {
            Handle<QueryBase> toReturn = querySinks->operator[](i);
            i++;
            return toReturn;
        };

        return buildIr(hasNext, getNextElement);
    }

    QueryGraphIr buildIrSingle(Handle<QueryBase> querySink)
    {
        bool nextEverCalled = false;

        function<bool()> hasNext = [&]() { return !nextEverCalled; };
        function<Handle<QueryBase>()> getNextElement = [&]
        {
            nextEverCalled = true;
            Handle<QueryBase> toReturn = querySink;
            querySink = nullptr;
            return toReturn;
        };

        return buildIr(hasNext, getNextElement);
    }


}