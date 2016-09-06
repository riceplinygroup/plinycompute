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
 * File:   DefaultDatabase.cc
 * Author: Jia
 *
 */


#ifndef DEFAULT_DATABASE_CC
#define DEFAULT_DATABASE_CC

#include "DefaultDatabase.h"
#include <map>
#include <memory>

using namespace std;

//create a new DefaultDatabase instance
DefaultDatabase::DefaultDatabase(NodeID nodeId, DatabaseID dbId, string dbName, ConfigurationPtr conf,
        pdb :: PDBLoggerPtr logger, SharedMemPtr shm, string metaDBPath, vector<string>* dataDBPaths, PageCachePtr cache,
		PageCircularBufferPtr flushBuffer) {
    this->nodeId = nodeId;
    this->dbId = dbId;
    this->dbName = dbName;
    this->conf = conf;
    this->logger = logger;
    this->shm = shm;
    this->metaDBPath = metaDBPath;
    this->dataDBPaths = dataDBPaths;
    unsigned int i;
    if (this->metaDBPath.compare("") != 0) {
    	this->conf->createDir(this->metaDBPath);
    }
    for (i = 0; i<this->dataDBPaths->size(); i++) {
        this->conf->createDir(this->dataDBPaths->at(i));
    }
    this->types = new map<UserTypeID, TypePtr> ();
    pthread_mutex_init(&this->typeOpLock, nullptr);
    this->cache = cache;
    this->flushBuffer = flushBuffer;
}

//destructor
DefaultDatabase::~DefaultDatabase() {
    if (this->types != nullptr) {
        delete this->types;
    }
    pthread_mutex_destroy(&this->typeOpLock);
}


//encode type path
string DefaultDatabase::encodeTypePath(string dbPath, UserTypeID typeId, string typeName) {
    char buffer[500];
    sprintf(buffer, "%s/%d_%s", dbPath.c_str(), typeId, typeName.c_str());
    return string(buffer);
}

//add a new type
bool DefaultDatabase::addType(string name, UserTypeID id) {
    if (this->types->find(id) != this->types->end()) {
        this->logger->writeLn("DefaultDatabase: type exists.");
        return false;
    }
    string metaTypePath = encodeTypePath(this->metaDBPath, id, name);
    vector<string> * dataTypePaths = new vector<string>();
    unsigned int i;
    for (i = 0; i<this->dataDBPaths->size(); i++) {
    	string dataTypePath = encodeTypePath(this->dataDBPaths->at(i), id, name);
    	//cout<<"AddType: dataTypePath:"<<dataTypePath<<"\n";
        dataTypePaths->push_back(dataTypePath);
    }
    TypePtr type = make_shared<UserType>(this->nodeId, this->dbId, id, name,
            this->conf, this->logger, this->shm, metaTypePath, dataTypePaths, this->cache, this->flushBuffer);
    this->addType(type);
    return true;
}

//add a new type
bool DefaultDatabase::addType(TypePtr type) {
    UserTypeID typeId = type->getId();
    if (this->types->find(typeId) != this->types->end()) {
        this->logger->writeLn("DefaultDatabase: type exists.");
        return false;
    }
    pthread_mutex_lock(&this->typeOpLock);
    types->insert(pair<UserTypeID, TypePtr>(typeId, type));
    pthread_mutex_unlock(&this->typeOpLock);
    return true;
}

//remove a type and associated disk files
bool DefaultDatabase::removeType(UserTypeID typeId) {
    this->logger->writeInt(typeId);
    map<UserTypeID, TypePtr>::iterator it = types->find(typeId);
    if (it != types->end()) {
        //this->logger->writeLn("DefaultDatabase: Type found, removing it...");
        pthread_mutex_lock(&this->typeOpLock);
        this->clearType(typeId, it->second->getName());
        this->types->erase(it); //will erase invoke destructor of element???
        pthread_mutex_unlock(&this->typeOpLock);
        return true;
    } else {
        this->logger->writeLn("DefaultDatabase: Type doesn't exist:");
        this->logger->writeInt(typeId);
        return false;
    }
}

//clear type data and associated disk files for removal
void DefaultDatabase::clearType(UserTypeID typeId, string typeName) {
	unsigned int i;
	remove(this->metaDBPath.c_str());
	string typePath;
	for (i = 0; i < this->dataDBPaths->size(); i++) {
		typePath = encodeTypePath(this->dataDBPaths->at(i), typeId, typeName);
		//remove(typePath.c_str());
		boost::filesystem::remove_all(typePath.c_str());
	}
}

//returns a type specified
TypePtr DefaultDatabase::getType(UserTypeID typeId) {
    map<UserTypeID, TypePtr>::iterator it = this->types->find(typeId);
    if (it != this->types->end()) {
        return it->second;
    }
    this->logger->writeLn("DefaultDatabase: type doesn't exist");
    return nullptr;
}

//flush data from memory to disk files
//flush is now fully managed by PageCache, so do nothing here
void DefaultDatabase::flush() {
    /*
    this->logger->writeLn("DefaultDatabase: flushing Database with DatabaseID:");
    this->logger->writeInt(this->dbId);
    //coarse-grained lock to synchronize type removal and type flushing
    pthread_mutex_lock(&this->typeOpLock);
    for (std::map<UserTypeID, TypePtr>::iterator it = this->types->begin();
            it != types->end(); ++it) {
        TypePtr type = it->second;
        type->flush();
    }
    pthread_mutex_unlock(&this->typeOpLock);
    */
}

//returns database id
DatabaseID DefaultDatabase::getDatabaseID() {
    return this->dbId;
}

//returns database name
string DefaultDatabase::getDatabaseName() {
    return this->dbName;
}

using namespace boost::filesystem;

/**
 * Initialize database instance (e.g. types and sets) by scanning meta directories and files.
 * This function can only be applied to PartitionedFile instances.
 */
bool DefaultDatabase::initializeFromMetaDBDir(path metaDBDir) {
	   if (exists(metaDBDir)) {
	        if (is_directory(metaDBDir)) {
	            vector<path> typeDirs;
	            copy(directory_iterator(metaDBDir), directory_iterator(), back_inserter(typeDirs));
	            vector<path>::iterator iter;
	            std::string path;
	            std::string dirName;
	            std::string name;
	            UserTypeID typeId;
	            for (iter = typeDirs.begin(); iter != typeDirs.end(); iter++) {
	                if (is_directory(* iter)) {
	                	//find a type
	                    path = std::string(iter->c_str());
	                    dirName = path.substr(path.find_last_of('/') + 1, path.length() - 1);
	                    //parse type name
	                    name = dirName.substr(dirName.find('_') + 1, dirName.length() - 1);
	                    //parse type id
	                    typeId = stoul(dirName.substr(0, dirName.find('_')));
	                    //cout << "DefaultDatabase: detect type at path: " << path << "\n";
	                    //cout << "Type name: " << name << "\n";
	                    //cout << "Type ID:" << typeId << "\n";
	                    this->addTypeByPartitionedFiles(name, typeId, path);
	                }
	            }
	        } else {
	            this->logger->writeLn("DefaultDatabase: dbDir doesn't exist:");
	            this->logger->writeLn(metaDBDir.c_str());
	            return false;
	        }
	    } else {
	        return false;
	    }
	    return true;
}

/**
 * Load a type instance to the database from PartitionedFile instances.
 */
void DefaultDatabase::addTypeByPartitionedFiles(string name, UserTypeID id, boost::filesystem::path metaTypeDir) {
    if (this->types->find(id) != this->types->end()) {
        this->logger->writeLn("DefaultDatabase: type exists.");
        return;
    }
    vector<string> * dataTypePaths = new vector<string>();
    int numDataPaths = this->dataDBPaths->size();
    int i = 0;
    string dataTypePath;
    for (i = 0; i < numDataPaths; i++) {
    	dataTypePath = this->encodeTypePath(dataDBPaths->at(i), id, name);
    	dataTypePaths->push_back(dataTypePath);
    }

    TypePtr type = make_shared<UserType>(this->nodeId, this->dbId, id, name,
            this->conf, this->logger, this->shm, string(metaTypeDir.c_str()), dataTypePaths, this->cache, this->flushBuffer);
    if (type == nullptr) {
        this->logger->writeLn("DefaultDatabase: Out of Memory.");
        exit(1);
    }
    type->initializeFromMetaTypeDir(metaTypeDir);
    this->addType(type);
}

/**
 * Initialize database instance (e.g. types and sets) by scanning meta directories and files.
 * This function can only be applied to SequenceFile instances.
 */
bool DefaultDatabase::initializeFromDBDir(path dbDir) {
    if (exists(dbDir)) {
        if (is_directory(dbDir)) {
            vector<path> typeDirs;
            copy(directory_iterator(dbDir), directory_iterator(), back_inserter(typeDirs));
            vector<path>::iterator iter;
            std::string path;
            std::string dirName;
            std::string name;
            UserTypeID typeId;
            for (iter = typeDirs.begin(); iter != typeDirs.end(); iter++) {
                if (is_directory(* iter)) {
                    this->logger->writeLn("DefaultDatabase: find a type at path:");
                    this->logger->writeLn(iter->c_str());
                    path = std::string(iter->c_str());
                    dirName = path.substr(path.find_last_of('/') + 1, path.length() - 1);
                    name = dirName.substr(dirName.find('_') + 1, dirName.length() - 1);
                    this->logger->writeLn("DefaultDatabase: typeName:");
                    this->logger->writeLn(name.c_str());
                    typeId = stoul(dirName.substr(0, dirName.find('_')));
                    this->logger->writeLn("DefaultDatabase: typeId:");
                    this->logger->writeInt(typeId);
                    cout << "DefaultDatabase: detect type at path: " << path << "\n";
                    cout << "Type name: " << name << "\n";
                    cout << "Type ID:" << typeId << "\n";
                    this->addTypeBySequenceFiles(name, typeId, path);
                }
            }
        } else {
            this->logger->writeLn("DefaultDatabase: dbDir doesn't exist:");
            this->logger->writeLn(dbDir.c_str());
            return false;
        }
    } else {
        return false;
    }
    return true;
}

/**
 * Load a type instance to the database from SequenceFile instances.
 */
void DefaultDatabase::addTypeBySequenceFiles(string name, UserTypeID id, boost::filesystem::path typeDir) {
    if (this->types->find(id) != this->types->end()) {
        this->logger->writeLn("DefaultDatabase: type exists.");
        return;
    }
    vector<string> * dataTypePaths = new vector<string>();
    dataTypePaths->push_back(std::string(typeDir.c_str()));
    TypePtr type = make_shared<UserType>(this->nodeId, this->dbId, id, name,
            this->conf, this->logger, this->shm, "", dataTypePaths, this->cache, this->flushBuffer);
    if (type == nullptr) {
        this->logger->writeLn("DefaultDatabase: Out of Memory.");
        exit(1);
    }
    type->initializeFromTypeDir(typeDir);
    this->addType(type);
}


#endif
