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
#ifndef PDB_TCAPINTERMEDIARYREP_HOIST_H
#define PDB_TCAPINTERMEDIARYREP_HOIST_H

#include <memory>
#include <string>
#include <vector>

#include "Column.h"
#include "Instruction.h"

using std::shared_ptr;
using std::string;
using std::vector;

using pdb_detail::Column;
using pdb_detail::Instruction;

namespace pdb_detail
{
    class Hoist : public Instruction
    {
    public:

        const string fieldId;

        const Column inputColumn;

        const Column outputColumn;

        /**
         * Any option columns to copy into the output table during its contruction.
         */
        const shared_ptr<vector<Column>> columnsToCopyToOutputTable;

        const string executorId;

        Hoist(string fieldId, Column inputColumn, Column outputColumn,
              shared_ptr<vector<Column>> columnsToCopyToOutputTable, string executorId)

            : Instruction(InstructionType::hoist), fieldId(fieldId), inputColumn(inputColumn),
              outputColumn(outputColumn), columnsToCopyToOutputTable(columnsToCopyToOutputTable), executorId(executorId)
        {
        }

        void match(function<void(Load&)>, function<void(ApplyFunction&)>, function<void(ApplyMethod&)>,
                   function<void(Filter&)>, function<void(Hoist&)> forHoist, function<void(GreaterThan&)>,
                   function<void(Store&)>)
        {
            forHoist(*this);
        }

    };

    typedef shared_ptr<Hoist> HoistPtr;

    HoistPtr makeHoist(string fieldId, Column inputColumn, Column outputColumn,
                       shared_ptr<vector<Column>> columnsToCopyToOutputTable, string executorId);
}

#endif //PDB_TCAPINTERMEDIARYREP_HOIST_H
