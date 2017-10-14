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
#ifndef PDB_TCAPINTERMEDIARYREP_LOAD_H
#define PDB_TCAPINTERMEDIARYREP_LOAD_H

#include "ApplyFunction.h"
#include "Instruction.h"

namespace pdb_detail {
/**
 * An instruction to create a table from an external locaton.
 */
class Load : public Instruction {
public:
    /**
     * The single columed table to create.
     */
    const TableColumn outputColumn;

    /**
     * The implemetnation specific source descriptor of the table to load.
     */
    const string source;

    /**
     * Creates a new Load instruction.
     *
     * @param outputColumnId The table to be created by the load
     * @param source The implemetnation specific source descriptor of the table to load.
     * @return the new Load instruction
     */
    Load(TableColumn outputColumnId, const string& source);

    // contract from super
    void match(function<void(Load&)> forLoad,
               function<void(ApplyFunction&)>,
               function<void(ApplyMethod&)>,
               function<void(Filter&)>,
               function<void(Hoist&)>,
               function<void(GreaterThan&)>,
               function<void(Store&)> forStore) override;
};

typedef shared_ptr<Load> LoadPtr;

/**
 * Creates a new load instruction.
 *
 * @param outputTableId the name of the table to create
 * @param outputColumnId the name of the singel column in the created table
 * @param source the implemetnation specific source descriptor of the table to load.
 * @return a shared pointer to the Load instruction
 */
LoadPtr makeLoad(const string& outputTableId, const string& outputColumnId, const string& source);
}

#endif  // PDB_TCAPINTERMEDIARYREP_LOAD_H
