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
#ifndef STORAGE_ADD_TEMP_SET_RESULT
#define STORAGE_ADD_TEMP_SET_RESULT

#include "SimpleRequestResult.h"
#include "DataTypes.h"

namespace pdb {

class StorageAddTempSetResult : public SimpleRequestResult {

public : 

    StorageAddTempSetResult () {}
    ~StorageAddTempSetResult () {}
  
    StorageAddTempSetResult(bool res, std :: string errMsg) : SimpleRequestResult (res, errMsg) {}
   
    StorageAddTempSetResult (bool res, std :: string errMsg, SetID tempsetId) : SimpleRequestResult(res, errMsg) {
        this->tempsetId = tempsetId;
    }

    SetID getTempSetID () { return this->tempsetId;}
    void setTempSetID (SetID tempsetId) { this->tempsetId = tempsetId; }

    ENABLE_DEEP_COPY


private:
    SetID tempsetId;

};
}


#endif
