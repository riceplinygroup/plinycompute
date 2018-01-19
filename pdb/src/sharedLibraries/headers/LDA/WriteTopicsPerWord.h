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
#ifndef WRITE_TOPICS_PER_WORD_H
#define WRITE_TOPICS_PER_WORD_H

#include "WriteUserSet.h"
#include "LDATopicWordProb.h"

using namespace pdb;
class WriteTopicsPerWord : public WriteUserSet<LDATopicWordProb> {

public:
    ENABLE_DEEP_COPY

    WriteTopicsPerWord() {}

    WriteTopicsPerWord(std::string dbName, std::string setName) {
        this->setOutput(dbName, setName);
    }
};


#endif
