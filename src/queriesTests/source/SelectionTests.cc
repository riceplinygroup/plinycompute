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
#include "SelectionTests.h"

#include <memory>

#include "Handle.h"
#include "InterfaceFunctions.h"
#include "Lambda.h"
#include "Object.h"
#include "Selection.h"
#include "Set.h"

using std::make_shared;

using pdb::Handle;
using pdb::Lambda;
using pdb::Object;
using pdb::makeObject;
using pdb::Set;
using pdb::Selection;
using pdb::QueryAlgo;

namespace pdb_tests
{
    void testSetGetInput(UnitTest &qunit)
    {
        class MySelection : public Selection<Object,Object>
        {
            Lambda <bool> getSelection (Handle <Object> &in)
            {
                return Lambda<bool>();
            }

            Lambda <Handle<Object>> getProjection (Handle <Object> &in)
            {
                return Lambda<Handle<Object>>();
            }

        };

        Handle<Set<Object>> someSet = makeObject<Set<Object>>("A", "B");

        MySelection selection;
        selection.setInput(someSet);

        QUNIT_IS_FALSE(selection.getIthInput(0).isNullPtr());


    }

    void testSelectionExecute(UnitTest &qunit)
    {
        class MySelection : public Selection<Object,Object>
        {
            Lambda <bool> getSelection (Handle <Object> &in)
            {
                return Lambda<bool>();
            }

            Lambda <Handle<Object>> getProjection (Handle <Object> &in)
            {
                return Lambda<Handle<Object>>();
            }

        };

        class MyAlgo : public QueryAlgo
        {
        public:

            void forSelection() override
            {
                isSelection = true;
            }

            void forSet() override
            {
            }

            bool isSelection = false;

        };

        MySelection selection;
        MyAlgo algo;

        selection.execute(algo);

        QUNIT_IS_TRUE(algo.isSelection);
    }
}