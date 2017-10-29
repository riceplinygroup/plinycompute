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

#ifndef SRC_BUILTINPDBOBJECTS_HEADERS_ACK_H_
#define SRC_BUILTINPDBOBJECTS_HEADERS_ACK_H_


#include "Object.h"
#include "Handle.h"
#include "PDBString.h"

//  PRELOAD %Ack%


namespace pdb {
// this object type is sent from the server to client to acknowledge receiving a request.
class Ack : public Object {

public:
    Ack() {}

    ~Ack() {}

    String& getInfo() {
        return info;
    }

    void setInfo(String& info) {
        this->info = info;
    }

    bool getWasError() {
        return wasError;
    }

    void setWasError(bool wasError) {
        this->wasError = wasError;
    }

    ENABLE_DEEP_COPY


private:
    String info;
    bool wasError;
};
}


#endif /* SRC_BUILTINPDBOBJECTS_HEADERS_ACK_H_ */
