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
 * File:   TempSet.h
 * Author: Jia
 *
 * Created on November 28, 2015, 6:58 PM
 */



#ifndef TEMP_SET_H
#define	TEMP_SET_H

#include "DataTypes.h"
#include "PDBPage.h"
#include "UserSet.h"
#include "PageCircularBuffer.h"

#include <memory>
using namespace std;
class TempSet;
typedef shared_ptr<TempSet> TempSetPtr;

/**
 * This class wraps a temporary set.
 * The set is created per request of backend,
 * and managed by local storage for creation,
 * data insertion, flushing, loading and removal.
 */
class TempSet : public UserSet {
public:

	//typedef UserSet super;

	/**
	 * Create a TempSet instance.
	 * All TempSets are stored in Database with DatabaseID = 0, and in UserType with UserTypeID = 0.
	 */
    TempSet(SetID setId, string setName, string metaTempPath, vector<string> dataTempPaths,
    		SharedMemPtr shm, PageCachePtr cache, pdb :: PDBLoggerPtr logger, LocalityType localityType=ShuffleData, LocalitySetReplacementPolicy policy=MRU, OperationType operation=Write, DurabilityType durability=TryCache, PersistenceType persistence=Transient);
    ~TempSet();
    
    //TODO: virtual vector<PageIteratorPtr> * getIterators() override;

    
    /**
     * Clear temp set and all related files;
     */
    void clear();

protected:
    /**
     * Compute the path to store the set data.
     */
    string encodePath(string tempPath, string setName);
    

    
};



#endif	/* TEMP_SET_H */

