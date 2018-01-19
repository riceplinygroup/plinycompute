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

#ifndef SET_SPECIFIER_H
#define SET_SPECIFIER_H

// by Jia, Oct 2016

#include <string>
#include "DataTypes.h"
#include "Configuration.h"
#include <memory>
class SetSpecifier;
typedef std::shared_ptr<SetSpecifier> SetSpecifierPtr;


// encapsulates a request to add a set in storage
class SetSpecifier {

public:
    SetSpecifier() {}
    ~SetSpecifier() {}

    SetSpecifier(std::string dataBase,
                 std::string setName,
                 DatabaseID dbId,
                 UserTypeID typeId,
                 SetID setId,
                 size_t pageSize = DEFAULT_PAGE_SIZE)
        : dataBase(dataBase),
          setName(setName),
          dbId(dbId),
          typeId(typeId),
          setId(setId),
          pageSize(pageSize) {}

    std::string getDatabase() {
        return dataBase;
    }

    std::string getSetName() {
        return setName;
    }

    void setDatabaseId(DatabaseID dbId) {
        this->dbId = dbId;
    }

    void setTypeId(UserTypeID typeId) {
        this->typeId = typeId;
    }

    void setSetId(SetID setId) {
        this->setId = setId;
    }

    void setPageSize(size_t pageSize) {
        this->pageSize = pageSize;
    }

    DatabaseID getDatabaseId() {
        return dbId;
    }

    UserTypeID getTypeId() {
        return typeId;
    }

    SetID getSetId() {
        return setId;
    }

    size_t getPageSize() {
        return pageSize;
    }


private:
    std::string dataBase;
    std::string setName;
    DatabaseID dbId;
    UserTypeID typeId;
    SetID setId;
    size_t pageSize;
};


#endif
