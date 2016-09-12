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
 * CounterBuzzer.h
 *
 *  Created on: Dec 25, 2015
 *      Author: Jia
 */

#ifndef SRC_CPP_MAIN_STORAGE_HEADERS_STORAGECOUNTERBUZZER_H_
#define SRC_CPP_MAIN_STORAGE_HEADERS_STORAGECOUNTERBUZZER_H_



#include "PDBCommWork.h"
#include "PDBAlarm.h"
#include "PDBBuzzer.h"
#include <string>

namespace pdb {
class CounterBuzzer : public PDBBuzzer {
public:

    CounterBuzzer(PDBCommWork &parentHandler) : parentHandler(parentHandler) {
    }

    void handleBuzz(PDBAlarm withMe) {
        if (withMe == PDBAlarm::WorkAllDone) {
            parentHandler.queryDone();
        } else if (withMe == PDBAlarm::QueryError) {
            std :: cout << "got an error from a query worker" << std :: endl;
        }
    }

private:

    PDBCommWork &parentHandler;
};

}


#endif /* SRC_CPP_MAIN_STORAGE_HEADERS_STORAGECOUNTERBUZZER_H_ */
