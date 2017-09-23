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
#ifndef RECORD_ITERATOR_H
#define RECORD_ITERATOR_H

//by Jia, 2017

#include "PDBPage.h"
#include "Record.h"
#include "Object.h"
#include <memory>


class RecordIterator;
typedef std :: shared_ptr<RecordIterator> RecordIteratorPtr;


using namespace pdb;
/**
 * This class wraps an iterator to scan records in a page.
 */
class RecordIterator {

public:

    RecordIterator(PDBPagePtr page);
    ~RecordIterator();

    /**
     * Returns a pointer to a Record.
     * Returns nullptr, if there is no more record in the page.
     */
    Record<Object> * next();

    /**
     * Returns true, if there is more objects in the page, otherwise returns false.
     */
    bool hasNext();

private:

    PDBPagePtr page;
    int numObjectsInCurPage;
    char * curPosInPage;
    int numObjectsIterated;


};


#endif
