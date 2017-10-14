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
#ifndef DISTRIBUTEDSTORAGEMANAGERREMOVEDATABASE_H
#define DISTRIBUTEDSTORAGEMANAGERREMOVEDATABASE_H

// PRELOAD %DistributedStorageRemoveDatabase%

namespace pdb {

// encapsulates a request to remove a database in the storage (and catalog)
class DistributedStorageRemoveDatabase : public Object {

public:
    DistributedStorageRemoveDatabase() {}
    ~DistributedStorageRemoveDatabase() {}

    DistributedStorageRemoveDatabase(std::string dataBase) : database(dataBase) {}

    std::string getDatabase() {
        return database;
    }

    ENABLE_DEEP_COPY

private:
    String database;
};
}

#endif  // DISTRIBUTEDSTORAGEMANAGERREMOVEDATABASE_H
