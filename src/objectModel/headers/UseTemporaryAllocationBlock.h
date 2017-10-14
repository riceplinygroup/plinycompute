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

#ifndef USE_TEMP_ALLOCATION_BLOCK_H
#define USE_TEMP_ALLOCATION_BLOCK_H

#include "Allocator.h"
#include "InterfaceFunctions.h"
#include <memory>

namespace pdb {

class UseTemporaryAllocationBlock;
typedef std::shared_ptr<UseTemporaryAllocationBlock> UseTemporaryAllocationBlockPtr;

class UseTemporaryAllocationBlock {

    AllocatorState oldInfo;

public:
    explicit UseTemporaryAllocationBlock(void* memory, size_t size) {
        oldInfo = getAllocator().temporarilyUseBlockForAllocations(memory, size);
    }

    explicit UseTemporaryAllocationBlock(size_t size) {
        oldInfo = getAllocator().temporarilyUseBlockForAllocations(size);
    }

    ~UseTemporaryAllocationBlock() {
        getAllocator().restoreAllocationBlock(oldInfo);
    }

    // forbidden, to avoid double frees
    UseTemporaryAllocationBlock(const UseTemporaryAllocationBlock&) = delete;
    UseTemporaryAllocationBlock& operator=(const UseTemporaryAllocationBlock&) = delete;
};
}

#endif
