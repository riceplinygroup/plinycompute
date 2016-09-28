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
#ifndef REFCOUNTED_ONHEAP_SMALL_PAGE_H
#define REFCOUNTED_ONHEAP_SMALL_PAGE_H

//by Jia, Sept 2016

#include <memory>
typedef std :: shared_ptr<RefCountedOnHeapSmallPage> RefCountedOnHeapSmallPagePtr;

namespace pdb {

// this is a helper class for storing intermediate data between nodes in a pipeline

class RefCountedOnHeapSmallPage {

private:

    void * data;
    size_t size;

public:
    //destructor
    ~RefCountedOnHeapSmallPage() {
        if (data != nullptr) {
            free(data);
        }
        data = 0;
        size = 0;
    }


    //constructor
    RefCountedOnHeapSmallPage (size_t size) {
        this->data = (void *) malloc (size);
        this->size = size;
    }

    //return memory
    void * getData() {
        return this->data;
    }

    size_t getSize() {
        return this->size;
    }

   




};



}




#endif
