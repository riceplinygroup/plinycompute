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
#include "ApplyMethod.h"

using std::invalid_argument;

namespace pdb_detail {

ApplyMethod::ApplyMethod(const string& executorId,
                         const string& functionId,
                         const string& outputTableId,
                         const string& outputColumnId,
                         TableColumns inputColumns,
                         shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable)
    : ApplyBase(executorId,
                functionId,
                outputTableId,
                outputColumnId,
                inputColumns,
                columnsToCopyToOutputTable,
                InstructionType::apply_method) {}


void ApplyMethod::match(function<void(Load&)>,
                        function<void(ApplyFunction&)>,
                        function<void(ApplyMethod&)> forApplyMethod,
                        function<void(Filter&)>,
                        function<void(Hoist&)>,
                        function<void(GreaterThan&)>,
                        function<void(Store&)>) {
    forApplyMethod(*this);
}

ApplyMethodPtr makeApplyMethod(const string& executorId,
                               const string& functionId,
                               const string& outputTableId,
                               const string& outputColumnId,
                               TableColumns inputColumns,
                               shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable) {
    try {
        return ApplyMethodPtr(new ApplyMethod(executorId,
                                              functionId,
                                              outputTableId,
                                              outputColumnId,
                                              inputColumns,
                                              columnsToCopyToOutputTable));
    } catch (const invalid_argument& e) {
        return nullptr;
    }
}
}