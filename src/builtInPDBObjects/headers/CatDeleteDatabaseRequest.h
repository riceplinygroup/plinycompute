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

#ifndef CAT_DELETE_DB_H
#define CAT_DELETE_DB_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"

// PRELOAD %CatDeleteDatabaseRequest%

namespace pdb {

// encapsulates a request to delete a Database
class CatDeleteDatabaseRequest : public Object {

public:
    ~CatDeleteDatabaseRequest() {}
    CatDeleteDatabaseRequest() {}
    CatDeleteDatabaseRequest(std::string dbName) : dbName(dbName) {}
    CatDeleteDatabaseRequest(const Handle<CatDeleteDatabaseRequest>& requestToCopy) {
        dbName = requestToCopy->dbName;
    }

    std::string dbToDelete() {
        return dbName;
    }

    ENABLE_DEEP_COPY

private:
    String dbName;
};
}

#endif
