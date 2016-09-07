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
 * File:   DefaultDatabase.h
 * Author: Jia
 *
 * Created on September 28, 2015, 8:31 PM
 */

#ifndef DEFAULTDATABASE_H
#define	DEFAULTDATABASE_H

#include "Configuration.h"
#include "DataTypes.h"
#include "PDBLogger.h"
#include "UserType.h"
#include "SharedMem.h"
#include "PageCache.h"
#include "PageCircularBuffer.h"

#include <map>
#include <pthread.h>
#include <boost/filesystem.hpp>
using namespace std;

// create a smart pointer for DummyDatabase objects
class DefaultDatabase;
typedef shared_ptr <DefaultDatabase> DefaultDatabasePtr;
;

/**
 * This class implements a DefaultDatabase object, that consists of a set of UserType objects.
 */
class DefaultDatabase  {

public:
	/**
	 * Create a database instance.
	 */
    DefaultDatabase(NodeID nodeId, DatabaseID dbId, string dbName, ConfigurationPtr conf,
            pdb :: PDBLoggerPtr logger, SharedMemPtr shm, string metaDBPath,
			vector<string>* dataDBPaths, PageCachePtr cache, PageCircularBufferPtr flushBuffer);
    ~DefaultDatabase();



    /**
     * Initialize database instance (e.g. types and sets) by scanning directories and files.
     * This function can only be applied to SequenceFile instances.
     */
    bool initializeFromDBDir(boost::filesystem::path dbDir);

    /**
     * Initialize database instance (e.g. types and sets) by scanning meta directories and files.
     * This function can only be applied to PartitionedFile instances.
     */
    bool initializeFromMetaDBDir(boost::filesystem::path metaDBDir);

    /**
     * Add a type instance to the database.
     */
    bool addType(TypePtr type);

    /**
     * Create and initialize a type instance to the database.
     */
    bool addType(string name, UserTypeID id);


    /**
     * Get a type from the database.
     */
    TypePtr getType(UserTypeID typeId);


    /**
     * Remove a type from the database, and delete all the disk files associated with the type
     */
    bool removeType(UserTypeID typeID);

    /**
     * Return the DatabaseID of the database.
     */
    DatabaseID getDatabaseID();

    /**
     * Return the name of the database.
     */
    string getDatabaseName();

    /**
     * Flush data from input buffers to disk files.
     */
    void flush();

    /**
     * Get all types
     */
    map<UserTypeID, TypePtr> * getTypes();


protected:

    /**
     * Load a type instance to the database from SequenceFile instances.
     */
    void addTypeBySequenceFiles(string name, UserTypeID id, boost::filesystem::path typeDir);

    /**
     * Load a type instance to the database from PartitionedFile instances.
     */
    void addTypeByPartitionedFiles(string name, UserTypeID id, boost::filesystem::path metaTypeDir);

    /**
     * Clear a type, deleting all files and directories associated with the type.
     */
    void clearType(UserTypeID typeId, string typeName);

    /**
     * Encode type path for creating type directory.
     */
    string encodeTypePath(string dbPath, UserTypeID typeId, string typeName);

private:
    ConfigurationPtr conf;
    map<UserTypeID, TypePtr> * types;
    string dbName;
    DatabaseID dbId;
    NodeID nodeId;
    pdb :: PDBLoggerPtr logger;
    pthread_mutex_t typeOpLock;
    string metaDBPath;
    vector<string> * dataDBPaths;
    SharedMemPtr shm;
    PageCachePtr cache;
    PageCircularBufferPtr flushBuffer;

};



#endif	/* DEFAULTDATABASE_H */

