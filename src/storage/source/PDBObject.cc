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

#ifndef PDB_Object_C
#define PDB_Object_C

#include "PDBObject.h"


PDBObject::PDBObject() {
    rawBytes = nullptr;
    dbId = (unsigned int)(-1);
    typeId = (unsigned int)(-1);
    setId = (unsigned int)(-1);
    size = 0;
    shmOffset = 0;
}

PDBObject::PDBObject(
    void* dataIn, DatabaseID dbId, UserTypeID typeId, SetID setId, size_t dataSize) {
    this->rawBytes = dataIn;
    this->dbId = dbId;
    this->typeId = typeId;
    this->setId = setId;
    this->shmOffset = 0;  // by default, it is not in shared memory, we need to update shmOffset
                          // when we allocate from shared memory
    size = dataSize;
}

PDBObject::~PDBObject() {
    freeObject();
}

void PDBObject::freeObject() {
    // if the data is in shared memory, we should not delete it,
    // because shared memory are managed at page level.
    // TODO: should be very careful about page reference count management.
    if ((rawBytes != nullptr) && (this->shmOffset == 0)) {
        delete ((char*)rawBytes);
    }
    rawBytes = nullptr;
    dbId = -1;
    typeId = -1;
    setId = -1;
    shmOffset = 0;
    size = 0;
}

#endif
