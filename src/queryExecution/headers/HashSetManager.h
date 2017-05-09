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
#ifndef HASH_SET_MANAGER
#define HASH_SET_MANAGER

//by Jia, May 2017

#include "AbstractHashSet.h"

namespace pdb {

/*
 * encapsulates a hash set manager to manage hash tables allocated on heap
 * this functionality will be replaced by Pangea storage manger
 */

class HashSetManager {

private: 
    // all hash tables allocated
    std :: map < std :: string, AbstractHashSetPtr> hashSets;

public:

    // to get a hash set
    AbstractHashSetPtr getHashSet ( std :: string name) {
        if (hashSets.count(name) == 0) {
            return nullptr;
        } else {
            return hashSets[name];
        }
    } 

    // to add a hash set
    bool addHashSet (std :: string name, AbstractHashSetPtr hashSet) {
        if (hashSets.count(name) != 0) {
            std :: cout << "Error: hash set exists: " << name << std :: endl;
            return false;
        } else {
            hashSets[name] = hashSet;
            return true;
        }
    }

    // to remove a hash set
    bool removeHashSet (std :: string name) {
        if (hashSets.count(name) == 0) {
           std :: cout << "Error: hash set doesn't exist: " << name << std :: endl;
           return false;
        } else {
            hashSets.erase(name);
            return true;
        }
    }


};




}


#endif
