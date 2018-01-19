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

#ifndef LDA_DOCUMENT_H
#define LDA_DOCUMENT_H

#include "Object.h"
#include "Handle.h"

/* The class for LDA documents */
using namespace pdb;

class LDADocument : public Object {

/* Document ID, word ID, and the number of this word in this document */
private:
    unsigned docID;
    unsigned wordID;
    unsigned count;

public:
    ENABLE_DEEP_COPY

    LDADocument() {}

    void setDoc(unsigned fromDoc) {
        this->docID = fromDoc;
    }

    unsigned getDoc() {
        return this->docID;
    }

    void setWord(int fromWord) {
        this->wordID = fromWord;
    }

    unsigned getWord() {
        return this->wordID;
    }

    void setCount(unsigned fromCount) {
        this->count = fromCount;
    }

    unsigned getCount() {
        return this->count;
    }

    void print() {
        std::cout << "docID: " << this->docID << " wordID: " << wordID << " count: " << count
                  << std::endl;
    }

    ~LDADocument() {}
};

#endif
