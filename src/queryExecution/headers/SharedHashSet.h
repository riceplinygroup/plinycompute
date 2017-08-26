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
#ifndef SHARED_HASH_SET
#define SHARED_HASH_SET

//by Jia, May 2017

#include "AbstractHashSet.h"

namespace pdb {

class SharedHashSet;
typedef std :: shared_ptr <SharedHashSet> SharedHashSetPtr;

/*
 * This class encapsulates a shared hash set, which is one unmanaged block, which contains a PDBMap.
 * In the end, this class will be removed and replaced by Pangea hash set
 */

class SharedHashSet : public AbstractHashSet {

private:

    //the name of this partitioned hash set
    std :: string setName;

    //the hash map page
    void *  pageData = nullptr;

    //the size of the page
    size_t pageSize;
 

public:

    //constructor
    SharedHashSet ( std :: string myName, size_t pageSize ) {
        this->setName = myName;
        this->pageSize = pageSize;
        this->pageData = (void *) malloc (sizeof(char) * pageSize);
        if (pageData == nullptr) {
            std :: cout << "SharedHashSet Error: insufficient heap memory" << std :: endl;
        }
    }

    bool isValid() {
        if (pageData == nullptr) {
           return false;
        } else {
           return true;
        }
    }

    size_t getSize() override {
        return pageSize;
    }

    //destructor
    ~ SharedHashSet () {
        this->cleanup();
    }

    //get type of this hash set
    std :: string getHashSetType () override {
        return "SharedHashSet";
    }

    //get name of this hash set
    std :: string getHashSetName () {
        return setName;
    }

    //get page size
    size_t getPageSize () {
        return pageSize;
    }

    //get page for a particular partition
    void * getPage () {
        return pageData;
    }

    //cleanup
    void cleanup () override {
        if (pageData != nullptr) {
            free(pageData);
            pageData = nullptr;
        }
    }

};



}


#endif
