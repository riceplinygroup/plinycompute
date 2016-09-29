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

#include <memory>

#include "BuildIrTests.h"
#include "ConsumableNodeIr.h"
#include "CheckEmployees.h"
#include "IrBuilder.h"
#include "MaterializationModeAlgo.h"
#include "QueryNodeIr.h"
#include "Selection.h"
#include "SelectionIr.h"
#include "Set.h"
#include "SetExpressionIrAlgo.h"
#include "SourceSetNameIr.h"
#include "Supervisor.h"
#include "ProjectionIr.h"
#include "QueryGraphIr.h"

using std::dynamic_pointer_cast;
using std::function;
using std::string;

using pdb::CheckEmployee;
using pdb::Handle;
using pdb::Lambda;
using pdb::makeObject;
using pdb::Object;
using pdb::Selection;
using pdb::Supervisor;
using pdb::Set;
using pdb::unsafeCast;

using pdb_detail::buildIr;
using pdb_detail::MaterializationModeAlgo;
using pdb_detail::MaterializationModeNone;
using pdb_detail::MaterializationModeNamedSet;
using pdb_detail::QueryGraphIr;
using pdb_detail::ProjectionIr;
using pdb_detail::RecordPredicateIr;
using pdb_detail::RecordProjectionIr;
using pdb_detail::SelectionIr;
using pdb_detail::SetExpressionIr;
using pdb_detail::SetExpressionIrAlgo;
using pdb_detail::SourceSetNameIr;


namespace pdb_tests
{
    /**
     * Create a simple pdb::Selection over a set name and ensure the built QueryIr object is correct.
     */
    void testBuildIrSelection(UnitTest &qunit)
    {
        /**
         * Create the selection to translate.
         */
        static Lambda<bool> condition = Lambda<bool>([]() { return true; });
        static Lambda<Handle<Object>> projector = Lambda<Handle<Object>>([]() { return nullptr; });
        class MySelectionType : public Selection<Object, Object>
        {

        public:

            virtual Lambda<bool> getSelection(Handle<Object> &in) override
            {
                return condition;
            }

            virtual Lambda<Handle<Object>> getProjection(Handle<Object> &in) override
            {
                return projector;
            }

            ENABLE_DEEP_COPY
        };

        Handle<MySelectionType> selection = makeObject<MySelectionType>();
        {
            Handle<Set<Object>> selectionInput = makeObject<Set<Object>>("inputDatabaseName", "inputSetName");
            selection->setInput(selectionInput);
        }

        selection->setDBName("outputDatabaseName");
        selection->setSetName("outputSetName");

        /**
         * Translate MySelection to QueryNodeIr.
         */
        QueryGraphIr queryGraph = buildIr(selection);


        /**
         * Test that projection is the only sink node.
         */
        QUNIT_IS_EQUAL(1, queryGraph.getSinkNodeCount());
        shared_ptr<SetExpressionIr> querySink = queryGraph.getSinkNode(0);

        QUNIT_IS_FALSE(querySink->getMaterializationMode()->isNone());

        class NamedSetExtractor : public MaterializationModeAlgo
        {
        public:

            void forNone(MaterializationModeNone &mode)
            {

            }

            void forNamedSet(MaterializationModeNamedSet &mode)
            {
                outDatabaseName = mode.getDatabaseName();
                outSetName = mode.getSetName();

            }

            string outDatabaseName;

            string outSetName;
        } outSetExtractor;

        querySink->getMaterializationMode()->execute(outSetExtractor);

        QUNIT_IS_EQUAL("outputDatabaseName", outSetExtractor.outDatabaseName);
        QUNIT_IS_EQUAL("outputSetName", outSetExtractor.outSetName);


        class IsProjection : public SetExpressionIrAlgo
        {
        public:

            void forProjection(ProjectionIr &recordProjection)
            {
                correct = true;
            }

            void forSelection(SelectionIr &selection)
            {
            }

            void forSourceSetName(SourceSetNameIr &setName)
            {
            }

            bool correct = false;

        } isProjection;

        querySink->execute(isProjection);

        QUNIT_IS_TRUE(isProjection.correct);

        shared_ptr<ProjectionIr> projectionIr = dynamic_pointer_cast<ProjectionIr>(querySink);

        Handle<Object> placeHolder1;
        Lambda<Handle<Object>> proj = projectionIr->getProjector()->toLambda(placeHolder1);
        QUNIT_IS_TRUE(projector == proj);



        /**
         * Test that the input to the projection is the selection
         */
        shared_ptr<SetExpressionIr> projectionInput = projectionIr->getInputSet();

        class IsSelection : public SetExpressionIrAlgo
        {
        public:

            void forProjection(ProjectionIr &recordProjection)
            {
            }

            void forSelection(SelectionIr &selection)
            {
                correct = true;
            }

            void forSourceSetName(SourceSetNameIr &setName)
            {
            }

            bool correct = false;

        } isSelection;

        projectionInput->execute(isSelection);

        QUNIT_IS_TRUE(isSelection.correct);

        shared_ptr<SelectionIr> selectionIr = dynamic_pointer_cast<SelectionIr>(projectionInput);

        Handle<Object> placeHolder2;
        Lambda<bool> cond = selectionIr->getCondition()->toLambda(placeHolder2);
        QUNIT_IS_TRUE(condition == cond);

        /**
         * Test that the input to the selection is the set "setname" in the database "databasename"
         */
        shared_ptr<SetExpressionIr> selectionInput = selectionIr->getInputSet();
        class IsSetName : public SetExpressionIrAlgo
        {
        public:

            void forProjection(ProjectionIr &recordProjection)
            {
            }

            void forSelection(SelectionIr &selection)
            {
            }

            void forSourceSetName(SourceSetNameIr &setName)
            {
                correct = true;
            }

            bool correct = false;

        } isSetName;

        selectionInput->execute(isSetName);

        QUNIT_IS_TRUE(isSetName.correct);
        if (!isSetName.correct)
            return;

        shared_ptr<SourceSetNameIr> selectionSetName = dynamic_pointer_cast<SourceSetNameIr>(selectionInput);

        QUNIT_IS_EQUAL("inputDatabaseName", selectionSetName->getDatabaseName());
        QUNIT_IS_EQUAL("inputSetName", selectionSetName->getName());

    }


}

