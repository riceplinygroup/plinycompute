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
#include "CheckEmployees.h"
#include "IrBuilder.h"
#include "QueryNodeIr.h"
#include "Selection.h"
#include "SelectionIr.h"
#include "Set.h"
#include "SetExpressionIrAlgo.h"
#include "SetNameIr.h"
#include "Supervisor.h"
#include "ProjectionIr.h"

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
using pdb_detail::QueryNodeIr;
using pdb_detail::QueryNodeIrAlgo;
using pdb_detail::ProjectionIr;
using pdb_detail::RecordPredicateIr;
using pdb_detail::RecordProjectionIr;
using pdb_detail::SelectionIr;
using pdb_detail::SetExpressionIr;
using pdb_detail::SetExpressionIrAlgo;
using pdb_detail::SetNameIr;


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
        static Lambda<bool> condition = Lambda<bool>( [] () { return true; } );
        static Lambda <Handle<Object>> projector = Lambda <Handle<Object>>( [] () { return nullptr; });
        class MySelectionType : public Selection<Object, Object>
        {
            virtual Lambda <bool> getSelection (Handle <Object> &in)
            {
                return condition;
            }

            virtual Lambda <Handle<Object>> getProjection (Handle <Object> &in)
            {
                return projector;
            }
        };

        Handle<MySelectionType> selection = makeObject<MySelectionType>();
        {
            Handle<Set<Object>> selectionInput = makeObject<Set<Object>>("databasename", "setname");
            selection->setInput(selectionInput);
        }


        /**
         * Translate MySelection to QueryNodeIr.
         */
        Handle<QueryNodeIr> buildResult = buildIr(selection);

        /**
         * Test that buildResult is a ProjectionIr.
         */
        class IsProjection : public QueryNodeIrAlgo
        {
        public:

            void forRecordPredicate(RecordPredicateIr &recordPredicate)
            {
            }

            void forRecordProjection(RecordProjectionIr &recordProjection)
            {
            }

            void forProjection(ProjectionIr &recordProjection)
            {
                correct = true;
            }

            void forSelection(SelectionIr &selection)
            {
            }

            void forSetName(SetNameIr &setName)
            {
            }

            bool correct = false;

        } isProjection;

        buildResult->execute(isProjection);

        QUNIT_IS_TRUE(isProjection.correct);

        Handle<ProjectionIr> projectionIr = unsafeCast<ProjectionIr,QueryNodeIr>(buildResult);

        Handle<Object> placeHolder1;
        Lambda<Handle<Object>> proj = projectionIr->getProjector()->toLambda(placeHolder1);
        QUNIT_IS_TRUE(projector == proj);


        /**
         * Test that the input to buildResult (projection) is a SetExpressionIr
         */
        Handle<SetExpressionIr> projectionIrInput = projectionIr->getInputSet();

        class IsSelection : public QueryNodeIrAlgo
        {
        public:

            void forRecordPredicate(RecordPredicateIr &recordPredicate)
            {
            }

            void forRecordProjection(RecordProjectionIr &recordProjection)
            {
            }

            void forProjection(ProjectionIr &recordProjection)
            {
            }

            void forSelection(SelectionIr &selection)
            {
                correct = true;
            }

            void forSetName(SetNameIr &setName)
            {
            }

            bool correct = false;

        } isSelection;

        projectionIrInput->execute(isSelection);

        QUNIT_IS_TRUE(isSelection.correct);

        Handle<SelectionIr> projectionIrInputSelection = unsafeCast<SelectionIr,SetExpressionIr>(projectionIrInput);

        Handle<Object> placeHolder2;
        Lambda<bool> cond = projectionIrInputSelection->getCondition()->toLambda(placeHolder2);
        QUNIT_IS_TRUE(condition == cond);



        /**
         * Test that the input to the selection is the set "setname" in the database "databasename"
         */
        class IsSetName : public SetExpressionIrAlgo
        {
        public:

            void forSelection(SelectionIr &selection)
            {
            }

            void forSetName(SetNameIr &setName)
            {
                correct = true;
            }

            bool correct = false;

        } isSetName;

        Handle<SetExpressionIr> inputSet = projectionIrInputSelection->getInputSet();
        inputSet->execute(isSetName);

        QUNIT_IS_TRUE(isSetName.correct);

        Handle<SetNameIr> selectionSetName = unsafeCast<SetNameIr,SetExpressionIr>(inputSet);

        QUNIT_IS_EQUAL("databasename", string(selectionSetName->getDatabaseName()->c_str()));
        QUNIT_IS_EQUAL("setname", string(selectionSetName->getName()->c_str()));

    }
}

