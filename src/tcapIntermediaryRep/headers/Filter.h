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
#ifndef PDB_TCAPINTERMEDIARYREP_FILTER_H
#define PDB_TCAPINTERMEDIARYREP_FILTER_H

namespace pdb_detail
{
    class Filter : public Instruction
    {

    public:

        /**
         * The id of the table to be created by application of the instruction.
         */
        const string inputTableId;

        /**
         * The namme of the column in the input table on which the filter operation operates.
         */
        const string filterColumnId;

        /**
         * The id of the table to be created by application of the instruction.
         */
        const string outputTableId;

        /**
         * Any option columns to copy into the output table during its contruction.
         */
        const shared_ptr<vector<Column>> columnsToCopyToOutputTable;

        Filter(string inputTableId, string filterColumnId, string outputTableId,
               shared_ptr<vector<Column>> columnsToCopyToOutputTable)
            :
                Instruction(InstructionType::filter),
                inputTableId(inputTableId), filterColumnId(filterColumnId), outputTableId(outputTableId),
                columnsToCopyToOutputTable(columnsToCopyToOutputTable)
        {

        }

        void match(function<void(Load&)>, function<void(ApplyFunction&)>, function<void(ApplyMethod&)>,
                   function<void(Filter&)> forFilter, function<void(Hoist&)>, function<void(GreaterThan&)>,
                   function<void(Store&)>)
        {
            forFilter(*this);
        }
    };
}

#endif //PDB_TCAPINTERMEDIARYREP_COLUMN_H
