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
 * CatalogSetMetadata.h
 *
 */

#ifndef CATALOG_SET_METADATA_H_
#define CATALOG_SET_METADATA_H_

#include <iostream>
#include "Object.h"
#include "PDBString.h"
#include "PDBVector.h"

//  PRELOAD %CatalogSetMetadata%

using namespace std;

namespace pdb {

class CatalogSetMetadata : public Object {
public:
    CatalogSetMetadata() {}

    CatalogSetMetadata(pdb::String pdbSetIdIn,
                       pdb::String pdbSetKeyIn,
                       pdb::String pdbSetNameIn,
                       pdb::String pdbDatabaseIdIn,
                       pdb::String pdbDatabaseNameIn,
                       pdb::String pdbTypeIdIn,
                       pdb::String pdbTypeNameIn)
        : pdbSetId(pdbSetIdIn),
          setKey(pdbSetKeyIn),
          pdbSetName(pdbSetNameIn),
          pdbDatabaseId(pdbDatabaseIdIn),
          pdbDatabaseName(pdbDatabaseNameIn),
          typeId(pdbTypeIdIn),
          typeName(pdbTypeNameIn) {}

    CatalogSetMetadata(const CatalogSetMetadata& pdbSetToCopy) {
        pdbSetId = pdbSetToCopy.pdbSetId;
        setKey = pdbSetToCopy.setKey;
        pdbSetName = pdbSetToCopy.pdbSetName;
        pdbDatabaseId = pdbSetToCopy.pdbDatabaseId;
        pdbDatabaseName = pdbSetToCopy.pdbDatabaseName;
        typeId = pdbSetToCopy.typeId;
        typeName = pdbSetToCopy.typeName;
    }

    CatalogSetMetadata(const Handle<CatalogSetMetadata>& pdbSetToCopy) {
        pdbSetId = pdbSetToCopy->pdbSetId;
        setKey = pdbSetToCopy->setKey;
        pdbSetName = pdbSetToCopy->pdbSetName;
        pdbDatabaseId = pdbSetToCopy->pdbDatabaseId;
        pdbDatabaseName = pdbSetToCopy->pdbDatabaseName;
        typeId = pdbSetToCopy->typeId;
        typeName = pdbSetToCopy->typeName;
    }

    void setValues(pdb::String pdbSetIdIn,
                   pdb::String pdbSetKeyIn,
                   pdb::String pdbSetNameIn,
                   pdb::String pdbDatabaseIdIn,
                   pdb::String pdbDatabaseNameIn,
                   pdb::String pdbTypeIdIn,
                   pdb::String pdbTypeNameIn) {

        pdbSetId = pdbSetIdIn;
        setKey = pdbSetKeyIn;
        pdbSetName = pdbSetNameIn;
        pdbDatabaseId = pdbDatabaseIdIn;
        pdbDatabaseName = pdbDatabaseNameIn;
        typeId = pdbTypeIdIn;
        typeName = pdbTypeNameIn;
    }

    ~CatalogSetMetadata() {}

    // The set ID is used as key for storing metadata
    pdb::String getItemKey() {
        return setKey;
    }

    pdb::String getItemId() {
        return pdbSetId;
    }

    pdb::String getDBId() {
        return pdbDatabaseId;
    }

    pdb::String getObjectTypeId() {
        return typeId;
    }


    pdb::String getItemName() {
        return pdbSetName;
    }

    pdb::String getDBName() {
        return pdbDatabaseName;
    }

    pdb::String getObjectTypeName() {
        return typeName;
    }

    void setTypeName(pdb::String& typeNameIn) {
        typeName = typeNameIn;
    }

    void setDBName(pdb::String& pdbDatabaseNameIn) {
        pdbDatabaseName = pdbDatabaseNameIn;
    }

    void setDBId(pdb::String& dbIdIn) {
        pdbDatabaseId = dbIdIn;
    }

    void setTypeId(pdb::String& typeIdIn) {
        typeId = typeIdIn;
    }

    void setItemKey(pdb::String& itemKeyIn) {
        setKey = itemKeyIn;
    }

    void setItemId(pdb::String& itemIdIn) {
        pdbSetId = itemIdIn;
    }

    void setItemName(pdb::String& itemNameIn) {
        pdbSetName = itemNameIn;
    }

    string printShort() {
        string output;
        output = "   \nSet ";
        output.append(getItemId())
            .append(":")
            .append(getItemKey().c_str())
            .append(" | in DB ")
            .append(getDBId())
            .append(":")
            .append(getDBName().c_str())
            .append(" | for type ")
            .append(getObjectTypeId())
            .append(":")
            .append(getObjectTypeName().c_str());
        return output;
    }

    ENABLE_DEEP_COPY

private:
    pdb::String pdbSetId;
    pdb::String setKey;
    pdb::String pdbSetName;

    pdb::String pdbDatabaseId;
    pdb::String pdbDatabaseName;

    pdb::String typeId;
    pdb::String typeName;
};

}  // namespace

#endif /* CATALOG_SET_METADATA_H_ */
