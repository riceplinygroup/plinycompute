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

#include "MaterializationModeNamedSet.h"
#include "PDBMap.h"
#include "ProjectionIr.h"
#include "SelectionIr.h"
#include "QueryAlgo.h"
#include "SourceSetNameIr.h"

using pdb::makeObject;
using pdb::Map;
using pdb::QueryAlgo;

namespace pdb_detail
{
    /**
     * A pdb::Map of QueryBase onto SetExpressionIr.
     */
    typedef Handle<Map<QueryBaseHdl, SetExpressionIrPtr>> AlreadyTransMapHdl;

    // forward declaration. See other declaration for contract.
    SetExpressionIrPtr makeOrReuseSetExpressionHelp(QueryBaseHdl queryNode, AlreadyTransMapHdl alreadyTranslated,
                                             bool &madeNewNode);

    /**
     * Translates the given user query node (assumed to be a Selection<?,?>) to a SelectionIr.
     * The returned SelectionIr's input will be a translated form of the given selection's input.
     *
     * Returns nullptr if the given selection does not have an input. madeNewNode will not me modified
     * in this case, and set to true in all other cases.
     *
     * If a SelectionIr is retruned (i.e. not the null pointer) an entry will be placed in alreadyTranslated
     * mapping the given selection to the returned SelectionIr.
     *
     * @param selection a Selection<?,?> user query selection.
     * @param alreadyTranslated a mapping of QueryBase onto SetExpressionIr that is to be updated if this method
     *                          creates a new SelectionIrPtr to correspond to selection.
     * @param madeNewNode true if selection had an input and thus a SelectionIr was returned, otherwise unmodified.
     * @return a new SelectionIr or the null pointer.
     */
    SelectionIrPtr makeSelection(QueryBaseHdl selection, AlreadyTransMapHdl &alreadyTranslated, bool &madeNewNode)
    {
        if(!selection->hasInput())
            return nullptr; // n.b.: exceptions forbidden in pdb codebase

        SetExpressionIrPtr selectionInputIr
                = makeOrReuseSetExpressionHelp(selection->getIthInput(0), alreadyTranslated, madeNewNode);

        SelectionIrPtr selectionIr = make_shared<SelectionIr>(selectionInputIr, selection);
        madeNewNode = true;

        alreadyTranslated->operator[](selection) = selectionIr;

        return selectionIr;
    }


    /**
     * Translates the given user query node (assumed to be a Selection<?,?>) to a ProjectionIr and SelectionIr
     * pair.  The created ProjectionIr is returned and its input is the created SelectionIr.
     *
     * Returns nullptr if the given selection does not have an input. madeNewNode will not me modified
     * in this case, and set to true in all other cases.
     *
     * If a ProjectionIr is returned (i.e. not the null pointer) an entry will be placed in alreadyTranslated
     * mapping the given selection to the returned ProjectionIr.
     *
     * If the given selection's DbName and SetName string are both 1 character or longer, this method
     * sets the materialization mode of the returned ProjectionIr to MaterializationModeNamedSet with those
     * values.
     *
     * @param selection a Selection<?,?> user query selection.
     * @param alreadyTranslated a mapping of QueryBase onto SetExpressionIr that is to be updated if this method
     *                          creates a new ProjectionIr to correspond to selection.
     * @param madeNewNode true if selection had an input and thus a ProjectionIr was returned, otherwise unmodified.
     * @return a new ProjectionIr or the null pointer.
     */
    ProjectionIrPtr makeProjection(QueryBaseHdl selection, AlreadyTransMapHdl alreadyTranslated, bool &madeNewNode)
    {
        if(!selection->hasInput())
            return nullptr; // n.b.: exceptions forbidden in pdb codebase

        SetExpressionIrPtr projectionInput = makeSelection(selection, alreadyTranslated, madeNewNode);

        ProjectionIrPtr projection =  make_shared<ProjectionIr>(projectionInput, selection);
        madeNewNode = true;
        alreadyTranslated->operator[](selection) = projection; // overwrite selection -> inputSet

        if(selection->getDBName().length() > 0 && selection->getSetName().length() > 0)
        {
            string dbName = selection->getDBName();
            string setName = selection->getSetName();
            projection->setMaterializationMode(make_shared<MaterializationModeNamedSet>(dbName, setName));
        }

        return projection;
    }


    /**
     * Given a user query graph node, translate it and all its reachable ancestors (following inputs recursivly)
     * into a logical query plan nodes.
     *
     * If sink.isNullPtr(), return nullptr.
     *
     * If alreadyTranslated contains the key node, just return AlreadyTransMapHdl[node].
     *
     * Otherwise, create a new SetExpressionIr corresponding to node (recursing the path of reachable nodes, possibly
     * using values in alreadyTranslated if ancestors of node have been previously seen via previous explorations
     * of sinks stored in alreadyTranslated) and return it.
     *
     * @param node the node to translate
     * @param alreadyTranslated a mapping of QueryBase onto SetExpressionIr that is to be updated if this method
     *                          creates a new ProjectionIr to correspond to selection. Also a source of "to
     *                          be reused" nodes during translation if a QueryBase has been previously observed.
     * @param madeNewNode set to true if during translation any new nodes are created, else unmodified
     * @return a translation of node (and hence subraph starting at node)
     */
    SetExpressionIrPtr makeOrReuseSetExpressionHelp(QueryBaseHdl node, AlreadyTransMapHdl alreadyTranslated,
                                                    bool &madeNewNode)
    {
        if(node.isNullPtr())
            return nullptr;

        if(alreadyTranslated->count(node) > 0)
            return alreadyTranslated->operator[](node);

        /**
         * Return a different translated sink node depending on what type of user query node sink is.
         */
        class TranslateNode : public QueryAlgo
        {
        public:

            TranslateNode(Handle<QueryBase> queryNodeParam, AlreadyTransMapHdl alreadyTranslated, bool &madeNewNode)
                    : _node(queryNodeParam), _alreadyTranslated(alreadyTranslated), _madeNewNode(madeNewNode)
            {
            }

            void forSelection(QueryBase&) override  // node is a Selection<?,?>
            {
                // user query graph's selection is our projection/selection pair.
                // makeProjection here is not a mistake.
                output = makeProjection(_node, _alreadyTranslated, _madeNewNode);
            }

            void forSet(QueryBase&) override // node is a Set<?> which is a user query graph root
            {
                int numInputs = _node->getNumInputs();

                string dbName = _node->getDBName();
                string setName = _node->getSetName();

                output = make_shared<SourceSetNameIr>(dbName, setName); // SourceSetNameIr is our root type
            }

            void forQueryOutput(QueryBase&) override // node is a QueryOutput<?> which represents a sink
            {
                // Our model doesn't split output sets into their own node in a graph.
                // So instead translate the node that is to write to user query graph's given set node
                // and change its materialization mode.

                // user query: (destination=["db","x"]) <-- (selection node) <-- ...
                // logical:   (selection node w/ materialization mode =["db","x"]) <-- ...

                string dbName = _node->getDBName();
                string setName = _node->getSetName();

                Handle<QueryBase> nodeInput = _node->getIthInput(0); // QueryOutput has one (and only one) input,
                                                                     // contractually

                output = makeOrReuseSetExpressionHelp(nodeInput, _alreadyTranslated, _madeNewNode);
                output->setMaterializationMode(make_shared<MaterializationModeNamedSet>(dbName, setName));
            }

            SetExpressionIrPtr output; // store the translation of _node

        private:

            AlreadyTransMapHdl _alreadyTranslated; // function's alreadyTranslated param

            bool &_madeNewNode; // function's madeNewNode param

            Handle<QueryBase> _node; // function's node param

        } trans(node, alreadyTranslated, madeNewNode);

        node->execute(trans);

        return trans.output;
    }

    /**
     * Given a user query graph node, translate it and all its reachable ancestors (following inputs recursivly)
     * into a logical query plan nodes.
     *
     * If sink.isNullPtr(), return nullptr.
     *
     * If alreadyTranslated contains the key node, just return AlreadyTransMapHdl[node].
     *
     * Otherwise, create a new SetExpressionIr corresponding to node (recursing the path of reachable nodes, possibly
     * using values in alreadyTranslated if ancestors of node have been previously seen via previous explorations
     * of sinks stored in alreadyTranslated) and return it.
     *
     * @param node the node to translate
     * @param alreadyTranslated a mapping of QueryBase onto SetExpressionIr that is to be updated if this method
     *                          creates a new ProjectionIr to correspond to selection. Also a source of "to
     *                          be reused" nodes during translation if a QueryBase has been previously observed.
     * @param madeNewNode set to true if during translation any new nodes are created, else set to false.
     * @return a translation of node (and hence subraph starting at node)
     */
    SetExpressionIrPtr makeOrReuseSetExpression(QueryBaseHdl sink, AlreadyTransMapHdl alreadyTranslated,
                                                bool &madeNewNode)
    {
        madeNewNode = false;
        return makeOrReuseSetExpressionHelp(sink, alreadyTranslated, madeNewNode);
    }

    /**
     * Translates the given usery query graph into an equivalent logical query plan graph.
     *
     * Expects the given graph to have all sinks returned by a series of calls to getNextElement()
     * while hasNext() is true.
     *
     * @param hasNext tests if calling getNextElement() will return the next unseen sink.
     * @param getNextElement returns a sink and advances the iterator.
     * @return a corresponding logical query plan
     */
    QueryGraphIrPtr buildIr(function<bool()> hasNext, function<QueryBaseHdl()> getNextElement)
    {
        // TODO: should be a set collection structure once that exists
        shared_ptr<vector<SetExpressionIrPtr>> transSinkNodesAccum = make_shared<vector<SetExpressionIrPtr>>();

        // accumulate all seen QueryBases and the node they were translated to
        AlreadyTransMapHdl alreadyTranslatedAccum = makeObject<Map<QueryBaseHdl, SetExpressionIrPtr>>();

        /**
         * Consume all given sinks, translating each one.
         */
        while(hasNext())
        {
            Handle<QueryBase> querySink = getNextElement();

            bool isNewNode; // will be set either way by makeOrReuseSetExpression
            SetExpressionIrPtr transSink = makeOrReuseSetExpression(querySink, alreadyTranslatedAccum, isNewNode);

            if(transSink == nullptr)
                return nullptr;

            if(isNewNode) // if a new node was created it must have been attached to an existing node (or empty graph)
                transSinkNodesAccum->push_back(transSink); // either way, it is a new sink.
                                                      // otherwise transSink is an existing sink prior to invoke
        }                                             // and thus already in transSinkNodes


        return make_shared<QueryGraphIr>(transSinkNodesAccum);
    }

    // contract in .h
    QueryGraphIrPtr buildIr(list<QueryBaseHdl> &querySinks)
    {
        /**
         * Create iterator functions over querySinks
         */
        list<Handle<QueryBase>>::const_iterator i = querySinks.begin();

        function<bool()> hasNext = [&] { return i != querySinks.end(); };

        function<Handle<QueryBase>()> getNextElement = [&]
        {
            Handle<QueryBase> toReturn = *i;
            i++;
            return toReturn;
        };

        /**
         * Build and return.
         */
        return buildIr(hasNext, getNextElement);

    }

    // contract in .h
    QueryGraphIrPtr buildIr(Handle<Vector<QueryBaseHdl>> querySinks)
    {
        /**
          * Create iterator functions over querySinks
          */
        uint32_t i = 0;
        function<bool()> hasNext = [&] { return i < querySinks->size(); };

        function<Handle<QueryBase>()> getNextElement = [&]
        {
            Handle<QueryBase> toReturn = querySinks->operator[](i);
            i++;
            return toReturn;
        };

        /**
         * Build and return.
         */
        return buildIr(hasNext, getNextElement);
    }

    // contract in .h
    QueryGraphIrPtr buildIrSingle(QueryBaseHdl querySink)
    {
        /**
         * Create iterator functions the single element querySink
         */
        bool nextEverCalled = false;

        function<bool()> hasNext = [&]() { return !nextEverCalled; };
        function<Handle<QueryBase>()> getNextElement = [&]
        {
            nextEverCalled = true;
            Handle<QueryBase> toReturn = querySink;
            querySink = nullptr;
            return toReturn;
        };

        /**
         * Build and return.
         */
        return buildIr(hasNext, getNextElement);
    }


}