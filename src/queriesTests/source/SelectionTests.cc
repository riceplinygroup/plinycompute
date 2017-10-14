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
#include "SimpleLambda.h"
#include "Object.h"
#include "Selection.h"
#include "Set.h"

using std::make_shared;

using pdb::Handle;
using pdb::SimpleLambda;
using pdb::Object;
using pdb::makeObject;
using pdb::Set;
using pdb::Selection;
using pdb::QueryAlgo;
using pdb::QueryBase;

namespace pdb_tests {
void testSetGetInput(UnitTest& qunit) {
    class MySelection : public Selection<Object, Object> {
        SimpleLambda<bool> getSelection(Handle<Object>& in) {
            return SimpleLambda<bool>();
        }

        SimpleLambda<Handle<Object>> getProjection(Handle<Object>& in) {
            return SimpleLambda<Handle<Object>>();
        }

        // over-ridden by the user so they can supply the selection on projection
        // temporarily added by Jia: for testing pipeline execution for logical plan with
        // pushing-down projection
        SimpleLambda<bool> getProjectionSelection(Handle<Object>& in) {}
    };

    Handle<Set<Object>> someSet = makeObject<Set<Object>>("A", "B");

    MySelection selection;
    selection.setInput(someSet);

    QUNIT_IS_FALSE(selection.getIthInput(0).isNullPtr());
}

void testSelectionExecute(UnitTest& qunit) {
    class MySelection : public Selection<Object, Object> {
        SimpleLambda<bool> getSelection(Handle<Object>& in) {
            return SimpleLambda<bool>();
        }

        SimpleLambda<Handle<Object>> getProjection(Handle<Object>& in) {
            return SimpleLambda<Handle<Object>>();
        }

        // over-ridden by the user so they can supply the selection on projection
        // temporarily added by Jia: for testing pipeline execution for logical plan with
        // pushing-down projection
        SimpleLambda<bool> getProjectionSelection(Handle<Object>& in) {}
    };


    MySelection selection;

    bool isSelection = false;

    selection.match(
        [&](QueryBase&) { isSelection = true; }, [&](QueryBase&) {}, [&](QueryBase&) {});

    QUNIT_IS_TRUE(isSelection);
}
}