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

#include "SelectionIrTests.h"

#include "Handle.h"
#include "SelectionIr.h"
#include "SetExpressionIr.h"
#include "QueryNodeIrAlgo.h"
#include "RecordPredicateIr.h"
#include "SourceSetNameIr.h"

using pdb::Handle;

using pdb_detail::SelectionIr;
using pdb_detail::SetExpressionIr;
using pdb_detail::RecordPredicateIr;
using pdb_detail::ProjectionIr;
using pdb_detail::SetExpressionIrAlgo;
using pdb_detail::SourceSetNameIr;

namespace pdb_tests
{
    void testSelectionIrExecute(UnitTest &qunit)
    {
        class Algo : public SetExpressionIrAlgo
        {
        public:


            void forProjection(ProjectionIr &projection)
            {
            }

            void forSelection(SelectionIr &selection)
            {
                success = true;
            }

            void forSourceSetName(SourceSetNameIr &setName)
            {
            }

            bool success = false;
        } algo;

        shared_ptr<SetExpressionIr> nullInputSet;
        shared_ptr<RecordPredicateIr> nullCondition;
        SimpleSingleTableQueryProcessorPtr pageProcessor;

        SelectionIr selection(nullInputSet, nullCondition, pageProcessor);

       // QUNIT_IS_TRUE(selection.getProcessor() == pageProcessor);

        selection.execute(algo);


        QUNIT_IS_TRUE(algo.success);

    }
}