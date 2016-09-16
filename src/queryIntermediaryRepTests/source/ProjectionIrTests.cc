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

#include "ProjectionIrTests.h"

#include "Handle.h"
#include "ProjectionIr.h"
#include "QueryNodeIrAlgo.h"
#include "RecordProjectionIr.h"
#include "SelectionIr.h"
#include "SetNameIr.h"

using pdb::Handle;

using pdb_detail::ProjectionIr;
using pdb_detail::QueryNodeIrAlgo;
using pdb_detail::RecordPredicateIr;
using pdb_detail::RecordProjectionIr;
using pdb_detail::SelectionIr;
using pdb_detail::SetNameIr;

namespace pdb_tests
{
    void testProjectionIrExecute(UnitTest &qunit)
    {
        class Algo : public QueryNodeIrAlgo
        {
        public:

            void forRecordPredicate(RecordPredicateIr &recordPredicate)
            {
            }

            void forRecordProjection(RecordProjectionIr &recordProjection)
            {
            }

            void forProjection(ProjectionIr &projection)
            {
                success = true;
            }

            void forSelection(SelectionIr &selection)
            {
            }

            void forSetName(SetNameIr &setName)
            {
            }

            bool success = false;
        } algo;

        Handle<SetExpressionIr> nullInputSet;
        Handle<RecordProjectionIr> nullProjector;
        ProjectionIr projection(nullInputSet, nullProjector);

        projection.execute(algo);


        QUNIT_IS_TRUE(algo.success);

    }
}