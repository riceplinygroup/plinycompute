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
 * File:   TempSet.cc
 * Author: Jia
 *
 */


#ifndef TEMP_SET_CC
#define TEMP_SET_CC

#include "TempSet.h"
#include "PartitionedFile.h"
#include "Configuration.h"
#include <vector>
using namespace std;



TempSet::TempSet(SetID setId, string setName, string metaTempPath, vector<string> dataTempPaths,
		SharedMemPtr shm, PageCachePtr cache, pdb :: PDBLoggerPtr logger,  LocalityType localityType, LocalitySetReplacementPolicy policy, OperationType operation, DurabilityType durability, PersistenceType persistence, size_t pageSize) :
    	    			UserSet(logger, shm, 0, 0, 0, setId, setName,
    	    					cache, localityType, policy, operation, durability, persistence, pageSize) {
        vector<string> dataPaths;
	int numDataPaths = dataTempPaths.size();
	int i;
	string dataPath;
	PartitionedFilePtr file;
        for (i = 0; i < numDataPaths; i++) {
            dataPath = this->encodePath(dataTempPaths.at(i), setName);
	    dataPaths.push_back(dataPath);
	}
	file = make_shared<PartitionedFile>(0, 0,
            0, setId, this->encodePath(metaTempPath, setName), dataPaths, logger, pageSize);
        this->lastFlushedPageId = (unsigned int)(-1); //0xFFFFFFF
	this->setFile(file);
	this->getFile()->openAll();
    
    
}


TempSet::~TempSet() {

}
    
    
void TempSet::clear() {
    this->getFile()->clear();
}

string TempSet::encodePath(string tempPath, string setName) {
    char buffer[500];
    sprintf(buffer, "%s/%d_%s", tempPath.c_str(), setId, setName.c_str());
    return string(buffer);
}


#endif
