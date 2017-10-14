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

#ifndef SET_IDENTIFIER_H
#define SET_IDENTIFIER_H

// by Jia, Oct 2016

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"
#include "DataTypes.h"

// PRELOAD %SetIdentifier%

namespace pdb {

// encapsulates a request to add a set in storage
class SetIdentifier : public Object {

public:
    ENABLE_DEEP_COPY

    SetIdentifier() {}
    ~SetIdentifier() {}

    SetIdentifier(std::string dataBase, std::string setName)
        : dataBase(dataBase), setName(setName) {
        setType = UserSetType;
        isAggregationResultOrNot = false;
    }

    SetIdentifier(std::string dataBase,
                  std::string setName,
                  SetType setType,
                  bool isProbingAggregationResult)
        : dataBase(dataBase),
          setName(setName),
          setType(setType),
          isAggregationResultOrNot(isProbingAggregationResult) {}


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

    DatabaseID getDatabaseId() {
        return dbId;
    }

    UserTypeID getTypeId() {
        return typeId;
    }

    SetID getSetId() {
        return setId;
    }

    SetType getSetType() {
        return setType;
    }


    bool isAggregationResult() {
        return this->isAggregationResultOrNot;
    }

    void setNumPages(size_t numPages) {
        this->numPages = numPages;
    }

    size_t getNumPages() {
        return this->numPages;
    }

    void setPageSize(size_t pageSize) {
        this->pageSize = pageSize;
    }

    size_t getPageSize() {
        return this->pageSize;
    }

private:
    String dataBase;
    String setName;
    DatabaseID dbId;
    UserTypeID typeId;
    SetID setId;
    SetType setType;
    bool isAggregationResultOrNot;
    size_t numPages;
    size_t pageSize;
};
}

#endif
