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
 * File:   UserType.h
 * Author: Jia
 *
 * Created on September 27, 2015, 2:41 PM
 */

#ifndef USERTYPE_H
#define USERTYPE_H

#include "DataTypes.h"
#include "SequenceFile.h"
#include "PageCache.h"
#include "PageCircularBuffer.h"
#include "UserSet.h"

#include <string>
#include <map>
#include <memory>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include <boost/filesystem.hpp>

using namespace std;

// create a smart pointer for Type objects
class UserType;
typedef shared_ptr<UserType> TypePtr;

using namespace boost::filesystem;

/**
 * This class defines a UserType structure used to store all the data of the same UserTypeID.
 * A UserType instance contains one or more Sets. Each Set has an input buffer,
 * and associated with a PDBFile instance.
 * When data comes in, it goes to input buffer first, then flushed to the PDBFile instance.
 */
class UserType {
public:
    /**
     * Create a UserType instance.
     */
    UserType(NodeID nodeId,
             DatabaseID dbId,
             UserTypeID id,
             string name,
             ConfigurationPtr conf,
             pdb::PDBLoggerPtr logger,
             SharedMemPtr shm,
             string metaTypePath,
             vector<string>* dataTypePaths,
             PageCachePtr cache,
             PageCircularBufferPtr flushBuffer);

    /**
     * Release the in-memory structure that is belonging to a UserType instance.
     * It will not remove the persistent data on disk that is belonging to the UserType.
     */
    ~UserType();

    /**
     * Flush UserType data from memory to a PDBFile instance for persistence.
     */
    void flush();

    // add new set
    int addSet(string setName, SetID setId, size_t pageSize = DEFAULT_PAGE_SIZE);

    // Remove an existing set, including all the disk files associated with the set.
    // If successful, return 0.
    // Otherwise, e.g. the set doesn't exist, return -1.
    int removeSet(SetID setId);

    // Return the specified Set instance that is belonging to this type instance.
    // If no such set, returns nullptr.
    SetPtr getSet(SetID setId);

    // Initialize type instance based on disk dirs and files.
    // This function is only used for SequenceFile instances.
    bool initializeFromTypeDir(path typeDir);

    // Initialize type instance based on disk dirs and files.
    // This function is only used for PartitionedFile instances.
    bool initializeFromMetaTypeDir(path metaTypeDir);

    // Return TypeID of the type instance.
    UserTypeID getId() const {
        return id;
    }

    // Return name of the type instance.
    string getName() const {
        return name;
    }

    // Set the TypeID for the type instance.
    void setId(UserTypeID id) {
        this->id = id;
    }

    // Set the name for the type instance.
    void setName(string name) {
        this->name = name;
    }

    // Return the total number of sets belonging to the type instance.
    unsigned int getNumSets() {
        return this->numSets;
    }

    map<SetID, SetPtr>* getSets() {
        return this->sets;
    }


protected:
    /**
     * Compute the path to store the UserType data for persistence.
     */
    string encodePath(string typePath, SetID setId, string setName);

private:
    string name;
    UserTypeID id;
    NodeID nodeId;
    DatabaseID dbId;
    int numSets;
    map<SetID, SetPtr>* sets;
    ConfigurationPtr conf;
    pdb::PDBLoggerPtr logger;
    SharedMemPtr shm;
    string metaPath;
    vector<string>* dataPaths;
    PageCachePtr cache;
    PageCircularBufferPtr flushBuffer;
    pthread_mutex_t setLock;
};


#endif /* PDBTYPE_H */
