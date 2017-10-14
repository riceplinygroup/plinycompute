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
#include "Hoist.h"

using std::make_shared;
using std::invalid_argument;

namespace pdb_detail {

Hoist::Hoist(const string& fieldId,
             TableColumn inputColumn,
             TableColumn outputColumn,
             shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable,
             const string& executorId)
    : Instruction(InstructionType::hoist),
      fieldId(fieldId),
      inputColumn(inputColumn),
      outputColumn(outputColumn),
      columnsToCopyToOutputTable(columnsToCopyToOutputTable),
      executorId(executorId) {
    if (columnsToCopyToOutputTable == nullptr)
        throw invalid_argument("columnsToCopyToOutputTable may not be null");
}

void Hoist::match(function<void(Load&)>,
                  function<void(ApplyFunction&)>,
                  function<void(ApplyMethod&)>,
                  function<void(Filter&)>,
                  function<void(Hoist&)> forHoist,
                  function<void(GreaterThan&)>,
                  function<void(Store&)>) {
    forHoist(*this);
}

HoistPtr makeHoist(const string& fieldId,
                   TableColumn inputColumn,
                   TableColumn outputColumn,
                   shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable,
                   const string& executorId) {
    try {
        return HoistPtr(
            new Hoist(fieldId, inputColumn, outputColumn, columnsToCopyToOutputTable, executorId));
    } catch (const invalid_argument& e) {
        return nullptr;
    }
}
}