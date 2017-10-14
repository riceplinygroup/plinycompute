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
/*
 * File:   PDBObject.h
 * Author: Jia
 *
 * Created on September 27, 2015, 1:20 PM
 */

#ifndef PDBOBJECT_H
#define PDBOBJECT_H

#include "DataTypes.h"
#include <memory>

using namespace std;
// create a smart pointer for PDBObjectPtr objects
class PDBObject;
typedef shared_ptr<PDBObject> PDBObjectPtr;

/**
 * This class implements PDBObject that treats Object as a piece of raw data.
 * The class also provide meta information about this raw data,
 * such as size, databaseId, typeId, setId, offset in shared memory and so on.
 * We use PDBObject at server side.
 * Once we receive pdb::object from client or server,
 * we treat it as raw data and encapsulate it into a PDBObject instance.
 */
class PDBObject {

public:
    // Create a PDBObject instance.
    PDBObject();
    PDBObject(void* dataIn, DatabaseID dbId, UserTypeID typeId, SetID setId, size_t dataSize);
    ~PDBObject();

    // To free an object, we need to make sure whether the raw data encapsulated is in shared memory
    // or not.
    // If it is in shared memory, the raw data is just a part of a page, which will be finally
    // released by shared memory manager.
    // If it is not in shared memory, we can release it right now.
    void freeObject();


    /**
     * Simple getters/setters.
     */

    // Return a pointer to the raw data.
    void* getRaw() const {
        return rawBytes;
    }

    // Return the size of the raw data.
    size_t getSize() const {
        return size;
    }

    // return the offset in shared memory of the raw data.
    size_t getShmOffset() const {
        return shmOffset;
    }

    // return SetID of the object.
    UserTypeID getSetID() const {
        return setId;
    }

    // return TypeID of the object.
    UserTypeID getTypeID() const {
        return typeId;
    }

    // return the DatabaseID of the object.
    DatabaseID getDatabaseID() const {
        return dbId;
    }

    // set raw data.
    void setRaw(void* data) {
        this->rawBytes = data;
    }

    // set size.
    void setSize(size_t size) {
        this->size = size;
    }

    // set offset in shared memory.
    void setShmOffset(size_t size) {
        this->shmOffset = size;
    }

    // set SetID.
    void setSetID(SetID setId) {
        this->setId = setId;
    }

    // set TypeID.
    void setTypeID(UserTypeID typeId) {
        this->typeId = typeId;
    }

    // set DatabaseID.
    void setDatabaseID(DatabaseID dbId) {
        this->dbId = dbId;
    }

private:
    void* rawBytes;
    DatabaseID dbId;
    UserTypeID typeId;
    SetID setId;
    size_t size;
    size_t shmOffset;
};

#endif /* PDBOBJECT_H */
