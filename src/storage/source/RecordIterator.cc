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
#ifndef OBJECT_ITERATOR_CC
#define OBJECT_ITERATOR_CC

//by Jia, Sept 2017

#include "RecordIterator.h"


RecordIterator::RecordIterator (PDBPagePtr page) {

    this->page = page;
    this->numObjectsIterated = 0;
    this->numObjectsInCurPage = page->getEmbeddedNumObjects();
    std :: cout << "there are " << numObjectsInCurPage << " objects" << std :: endl;
    this->curPosInPage = (char *)page->getBytes();
}

RecordIterator::~RecordIterator () {

}

Record<Object> * RecordIterator::next() {

    if (this->curPosInPage == 0) {
       return nullptr;
    }

    //1st field: record data size
    size_t objectDataSize = *((size_t *)this->curPosInPage);

    //2nd field: record data
    Record<Object> * ret = (Record<Object> *)(curPosInPage + sizeof(size_t));

    //compute offset of next record
    this->curPosInPage = this->curPosInPage + sizeof(size_t) + objectDataSize;

    this->numObjectsIterated++;
    return ret;
}   

bool RecordIterator::hasNext() {
    if (this->numObjectsInCurPage == this->numObjectsIterated) {
        this->curPosInPage = 0;
        return false;
    } else {
        return true;
    }
}

#endif
