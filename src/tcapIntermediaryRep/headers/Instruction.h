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
#ifndef PDB_TCAPINTERMEDIARYREP_INSTRUCTION_H
#define PDB_TCAPINTERMEDIARYREP_INSTRUCTION_H

#include <memory>
#include <functional>

using std::function;
using std::shared_ptr;

namespace pdb_detail
{
    enum InstructionType
    {
        filter, load, apply_function, apply_method, hoist, greater_than, store
    };

    class Load;

    class ApplyFunction;

    class ApplyMethod;

    class Filter;

    class Hoist;

    class GreaterThan;

    class Store;

    class Instruction
    {
    public:

        const InstructionType instructionType;

        Instruction(InstructionType type) : instructionType(type)
        {
        }

        virtual void match(function<void(Load&)> forLoad, function<void(ApplyFunction&)> forApplyFunc,
                           function<void(ApplyMethod&)> forApplyMethod, function<void(Filter&)> forFilter,
                           function<void(Hoist&)> forHoist, function<void(GreaterThan&)> forGreaterThan,
                           function<void(Store&)> forStore) = 0;



    };

    typedef shared_ptr<Instruction> InstructionPtr;
}

#endif //PDB_TCAPINTERMEDIARYREP_INSTRUCTION_H
