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
#include "BuildTcapTests.h"


#include "Handle.h"
#include "Object.h"
#include "QueryBase.h"
#include "QueryOutput.h"
#include "Selection.h"
#include "Set.h"
#include "SimpleLambda.h"
#include "TcapBuilder.h"
#include "PDBVector.h"

using pdb::Handle;
using pdb::makeObject;
using pdb::Object;
using pdb::QueryBase;
using pdb::QueryOutput;
using pdb::Selection;
using pdb::Set;
using pdb::SimpleLambda;
using pdb::Vector;

using pdb_detail::buildTcap;

namespace pdb_tests
{
    void buildTcapTest1(UnitTest &qunit)
    {
        /**
        * A user defined type to prove we can work over types other than object.
        */
        class Zebra : public Object
        {
            ENABLE_DEEP_COPY
        };


        class MySelectionType : public Selection<Zebra, Zebra>
        {

        public:

            SimpleLambda<bool> getSelection(Handle<Zebra> &in) override
            {
                return SimpleLambda<bool>([]() { return true; });
            }

            SimpleLambda<Handle<Zebra>> getProjection(Handle<Zebra> &in) override
            {
                return SimpleLambda<Handle<Zebra>>([]() { return nullptr; });
            }

            // over-ridden by the user so they can supply the selection on projection
            // temporarily added by Jia: for testing pipeline execution for logical plan with pushing-down projection
            SimpleLambda <bool> getProjectionSelection (Handle <Zebra> &in) override
            {

            }


            ENABLE_DEEP_COPY
        };


        /**
         * Setup a user query graph that looks like:
         *
         * (outputSet1)<--(selection2)<--(selection1)<--(inputSet)
         *                               |
         *                (outputSet2)<---
         *
         */
        Handle<Set<Zebra>> sourceSet = makeObject<Set<Zebra>>("somedb", "inputSetName");

        Handle<MySelectionType> selection1 = makeObject<MySelectionType>();
        selection1->setInput(sourceSet);

        Handle<MySelectionType> selection2 = makeObject<MySelectionType>();
        selection2->setInput(selection1);

        Handle<QueryOutput<Zebra>> outputSet1 = makeObject<QueryOutput<Zebra>>("somedb", "outputSetName1", selection1);


        Handle<QueryOutput<Zebra>> outputSet2 = makeObject<QueryOutput<Zebra>>("somedb", "outputSetName2", selection2);


        /**
         * Test translation of the user query graph with sinks outputSet1 and outputSet2 to a logical graph.
         *
         * The logical graph should have the form:
         *
         * (querySink) <- (internalNode1) <- (internalNode2) <- (internalNode3) <- (sourceNode)
         *
         *
         * With some following details:
         *
         * querySink: type = ProjectionIr, materialization? = yes (outputSetName2)
         *
         * internalNode1: type = SelectionIr, materialization? = no
         *
         * internalNode2: type = ProjectionIr, materialization? = yes (outputSetName1)
         *
         * internalNode3: type = SelectionIr, materialization? = no
         *
         * sourceNode: type = SourceSetNameIr, materialization? = no
         */
        Handle<Vector<Handle<QueryBase>>> sinks = makeObject<Vector<Handle<QueryBase>>>();
        sinks->push_back(outputSet2);
        sinks->push_back(outputSet1);

        string tcapProgram = buildTcap(outputSet1);

//        QUNIT_IS_EQUAL("", tcapProgram);

    }
}
