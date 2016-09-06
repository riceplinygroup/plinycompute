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
 * UserType.cc
 *
 *  Created on: Dec 23, 2015
 *      Author: Jia
 */

#include "UserType.h"
#include "PartitionedFile.h"

/**
 * Create a UserType instance.
 */
UserType::UserType(NodeID nodeId, DatabaseID dbId, UserTypeID id, string name,
		ConfigurationPtr conf, pdb :: PDBLoggerPtr logger, SharedMemPtr shm,
		string metaTypePath, vector<string>* dataTypePaths,
		PageCachePtr cache, PageCircularBufferPtr flushBuffer) {
	this->name = name;
	this->nodeId = nodeId;
	this->dbId = dbId;
	this->id = id;
	this->conf = conf;
	this->logger = logger;
	this->shm = shm;
	sets = new map<SetID, SetPtr>();
        pthread_mutex_init(&setLock, nullptr);
	this->dataPaths = dataTypePaths;
	unsigned int i;

	this->metaPath = metaTypePath;
	if (this->metaPath.compare("") != 0) {
		this->conf->createDir(this->metaPath);
	}
	for (i = 0; i < this->dataPaths->size(); i++) {
		string dataPath = this->dataPaths->at(i);
		this->conf->createDir(dataPath);
	}
	numSets = 0;
	this->cache = cache;
	this->flushBuffer = flushBuffer;
}

/**
 * Release the in-memory structure that is belonging to a UserType instance.
 * It will not remove the persistent data on disk that is belonging to the UserType.
 */
UserType::~UserType() {
	if (sets != nullptr) {
		delete sets;
	}
	if (dataPaths != nullptr) {
		delete dataPaths;
	}
}

/**
 * Flush UserType data from memory to a PDBFile instance for persistence.
 */
//Now all the flush is managed by PageCache class, so we remove flush here


void UserType::flush() {
       /*
	SetPtr curSet;
	this->logger->writeLn("Type: flushing type with UserTypeID: ");
	this->logger->writeInt(this->id);
	map<SetID, SetPtr>::iterator bufferIter;
	for (bufferIter = sets->begin(); bufferIter != sets->end(); bufferIter++) {
		this->logger->writeLn("SetID:");
		this->logger->writeInt(bufferIter->first);
		curSet = bufferIter->second;
		curSet->flush();
	}
       */
}



/**
 * Compute the path to store the UserType data for persistence.
 */
string UserType::encodePath(string typePath, SetID setId, string setName) {
	char buffer[500];
	sprintf(buffer, "%s/%d_%s", typePath.c_str(), setId, setName.c_str());
	return string(buffer);
}

//add new set
//Not thread-safe
int UserType::addSet(string setName, SetID setId) {
	if (this->sets->find(setId) != this->sets->end()) {
		this->logger->writeLn("UserType: set exists.");
		return -1;
	}
	string typePath;
	PartitionedFilePtr file;
		//cout<<"creating partitioned file...\n";
		string metaFilePath = this->encodePath(this->metaPath, setId, setName);
		//cout<<"metaFilePath for the set: "<<metaFilePath<<"\n";
		vector<string> dataFilePaths;
		unsigned int i;
		for(i= 0; i< this->dataPaths->size(); i++) {
			dataFilePaths.push_back(this->encodePath(this->dataPaths->at(i), setId, setName));
		}
		file = make_shared<PartitionedFile>(this->nodeId, this->dbId, this->id, setId,
            metaFilePath, dataFilePaths, this->logger,
			this->conf->getPageSize());

	//cout<<"creating set...\n";
	SetPtr set = make_shared<UserSet>(
			this->conf->getPageSize(), logger,
			shm, nodeId, dbId, id, setId, setName,
			file, this->cache);
	if (set == 0) {
		this->logger->writeLn("UserType: Out of Memory.");
		return -1;
	}
	this->logger->writeLn("UserType: set added.");
        pthread_mutex_lock(&setLock);
	this->sets->insert(pair<SetID, SetPtr>(setId, set));
        pthread_mutex_unlock(&setLock);
	this->numSets++;
	return 0;
}

//Remove an existing set.
//If successful, return 0.
//Otherwise, e.g. the set doesn't exist, return -1.
int UserType::removeSet(SetID setId) {
	map<SetID, SetPtr>::iterator setIter;
	if ((setIter = this->sets->find(setId)) != this->sets->end()) {
		this->logger->writeLn("UserType: removing input buffer for set:");
		this->logger->writeInt(setId);
		this->logger->writeLn("\n");
		setIter->second->getFile()->clear();
                pthread_mutex_lock(&setLock);
		this->sets->erase(setIter);
                pthread_mutex_unlock(&setLock);
		this->logger->writeLn("UserType: set is removed.");
		this->numSets--;
		return 0;
	}
	this->logger->writeLn("UserType: set doesn't exist.");
	return -1;
}

//Return the specified set that is belonging to this type instance.
SetPtr UserType::getSet(SetID setId) {
	map<SetID, SetPtr>::iterator setIter;
	if ((setIter = sets->find(setId)) != sets->end()) {
		return setIter->second;
	} else {
		return nullptr;
	}
}

using namespace boost::filesystem;

//Initialize type instance based on disk dirs and files.
//This function is only used for PartitionedFile instances.
bool UserType::initializeFromMetaTypeDir(path metaTypeDir) {
	//traverse all set files in type directory
	//for each set file, invoke addSet to initialize PDBFile object and CircularInputBuffer
	if (exists(metaTypeDir)) {
		if (is_directory(metaTypeDir)) {
			vector<path> metaSetFiles;
			copy(directory_iterator(metaTypeDir), directory_iterator(),
					back_inserter(metaSetFiles));
			vector<path>::iterator iter;
			std::string path;
			std::string dirName;
			std::string name;
			SetID setId;
			for (iter = metaSetFiles.begin(); iter != metaSetFiles.end();
					iter++) {
				if (is_regular_file(*iter)) {
					//find a set
					path = std::string(iter->c_str());
					dirName = path.substr(path.find_last_of('/') + 1,
							path.length() - 1);
					//parse set name
					name = dirName.substr(dirName.find('_') + 1,
							dirName.length() - 1);
					//parse set id
					setId = stoul(dirName.substr(0, dirName.find('_')));

					//check whether set exists
					if (this->sets->find(setId) != this->sets->end()) {
						this->logger->writeLn("UserType: set exists.");
						return false;
					}
					//cout << "UserType: detect set at path: " << path << "\n";
					//cout << "Set name: " << name << "\n";
					//cout << "Set ID:" << setId << "\n";

					//create PartitionedFile instance
					PartitionedFilePtr partitionedFile = make_shared<PartitionedFile>(this->nodeId,
							this->dbId, this->id, setId, path, this->logger, this->conf->getPageSize());

					//cout <<"buildMetaDataFromMetaPartition()"<<"\n";
					partitionedFile->buildMetaDataFromMetaPartition(nullptr);
					//cout <<"initializeDataFiles()"<<"\n";
					partitionedFile->initializeDataFiles();
					//cout <<"openData()"<<"\n";
					partitionedFile->openData();
					//cout <<"create set instance"<<"\n";
					//create a Set instance from file
					SetPtr set = make_shared<UserSet>(
							this->conf->getPageSize(),
							logger, this->shm,
							nodeId, dbId, id, setId, name,
							partitionedFile, this->cache);
					// add buffer to map
					if (set == 0) {
						this->logger->writeLn("UserType: out of memory.");
						exit(1);
					}
					this->sets->insert(
							pair<SetID, SetPtr>(setId, set));
					this->numSets++;
					this->logger->writeLn("UserType: set added.");
				}
			}
		} else {
			return false;
		}
	} else {
		return false;
	}
	return true;

}

//Initialize type instance based on disk dirs and files.
//This function is used for importing sets from SequenceFile instances.
//This function is mainly to provide backward compatibility for SequenceFile instances.
bool UserType::initializeFromTypeDir(path typeDir) {

/*
	//traverse all set files in type directory
	//for each set file, invoke addSet to initialize PDBFile object and CircularInputBuffer
	if (exists(typeDir)) {
		if (is_directory(typeDir)) {
			vector<path> setFiles;
			copy(directory_iterator(typeDir), directory_iterator(),
					back_inserter(setFiles));
			vector<path>::iterator iter;
			std::string path;
			std::string dirName;
			std::string name;
			SetID setId;
			for (iter = setFiles.begin(); iter != setFiles.end(); iter++) {
				if (is_regular_file(*iter)) {
					//find a set
					path = std::string(iter->c_str());
					this->logger->writeLn("PDBType: find a set at path:");
					this->logger->writeLn(path.c_str());

					dirName = path.substr(path.find_last_of('/') + 1,
							path.length() - 1);
					name = dirName.substr(dirName.find('_') + 1,
							dirName.length() - 1);
					this->logger->writeLn("PDBType: setName:");
					this->logger->writeLn(name.c_str());

					setId = stoul(dirName.substr(0, dirName.find('_')));
					this->logger->writeLn("PDBType: setId:");
					this->logger->writeInt(setId);

					if (this->sets->find(setId) != this->sets->end()) {
						this->logger->writeLn("PDBType: set exists.");
						return false;
					}
					cout << "Type: detect set at path: " << path << "\n";
					cout << "Set name: " << name << "\n";
					cout << "Set ID:" << setId << "\n";
					//create SequenceFile instance
					PDBFilePtr file = make_shared<SequenceFile>(this->nodeId,
							this->dbId, this->id, setId, path, this->logger,
							this->conf->getPageSize());
					file->getAndSetNumFlushedPages();
					this->logger->writeLn("UserType: numFlushedPages:");
					this->logger->writeInt(file->getNumFlushedPages());
					cout << "Set contains " << file->getNumFlushedPages()
							<< " pages.\n";
					if (file->getNumFlushedPages() > 0) {
						size_t metaPageSize = file->getPageSizeInMeta();
						if (this->conf->getPageSize() != metaPageSize) {
							cout
									<< "Error: Inconsistent page sizes: flushed file page size = "
									<< metaPageSize
									<< ", and current page size = "
									<< this->conf->getPageSize() << ".\n";
							exit(1);
						}
					}

					//create a Set instance from file
					SetPtr set = make_shared<UserSet>(
							this->conf->getPageSize(),
							 logger, this->shm,
							nodeId, dbId, id, setId, name,
							file, this->cache);
					// add buffer to map
					if (set == 0) {
						this->logger->writeLn("UserType: out of memory.");
						exit(1);
					}
					this->logger->writeLn("UserType: set added.");
					this->sets->insert(
							pair<SetID, SetPtr>(setId, set));
					this->numSets++;
					return true;
				}
			}
		} else {
			return false;
		}
	} else {
		return false;
	}
	return true;
*/
return false;
}

