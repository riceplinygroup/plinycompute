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
 * CatalogUserTypeMetadata.h
 *
 *  Created on: Mar 7, 2016
 *      Author: carlos
 */

#ifndef CATALOG_USER_TYPE_METADATA_H_
#define CATALOG_USER_TYPE_METADATA_H_

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
#include "PDBString.h"

//  PRELOAD %CatalogUserTypeMetadata%

using namespace std;

namespace pdb {

    /**
     *   CatalogUserTypeMetadata encapsulates information for a given user-defined object
     *   (either a data type or metric), including:
     *
     *   1) objectID: numeric identifier assigned automatically by the catalog
     *   2) objectType: (data_types or metrics)
     *   3) objectName: a string given by the user
     */

    class CatalogUserTypeMetadata : public pdb::Object {

    public:

        ENABLE_DEEP_COPY

        CatalogUserTypeMetadata(){

        }

        CatalogUserTypeMetadata(pdb :: String itemIdIn,
                                pdb :: String objectIDIn,
                                pdb :: String objectTypeIn,
                                pdb :: String objectNameIn,
                                pdb :: String libraryBytesIn) :
            itemId(itemIdIn),
            objectID(objectIDIn),
            objectType(objectTypeIn),
            objectName(objectNameIn),
            libraryBytes(libraryBytesIn)
        {}

        CatalogUserTypeMetadata(const CatalogUserTypeMetadata& pdbCatalogEntryToCopy){
            itemId = pdbCatalogEntryToCopy.itemId;
            objectID = pdbCatalogEntryToCopy.objectID;
            objectType = pdbCatalogEntryToCopy.objectType;
            objectName = pdbCatalogEntryToCopy.objectName;
            libraryBytes = pdbCatalogEntryToCopy.libraryBytes;
        }

        CatalogUserTypeMetadata(const Handle<CatalogUserTypeMetadata>& pdbCatalogEntryToCopy){
            itemId = pdbCatalogEntryToCopy->itemId;
            objectID = pdbCatalogEntryToCopy->objectID;
            objectType = pdbCatalogEntryToCopy->objectType;
            objectName = pdbCatalogEntryToCopy->objectName;
            libraryBytes = pdbCatalogEntryToCopy->libraryBytes;
        }


        ~CatalogUserTypeMetadata(){
        }


        pdb :: String getItemKey(){
            return objectName;
        }

        pdb :: String getItemId(){
            return itemId;
        }

        pdb :: String getObjectID(){
            return objectID;
        }

        pdb :: String getObjectType(){
            return objectType;
        }

        pdb :: String getItemName(){
            return objectName;
        }

        pdb :: String getLibraryBytes(){
            return libraryBytes;
        }


        void setObjectId(pdb :: String &objectIdIn){
            objectID = objectIdIn;
        }

        void setItemId(pdb :: String &itemIdIn){
            itemId = itemIdIn;
        }

        void setItemKey(pdb :: String &itemKeyIn){
            objectName = itemKeyIn;
        }

        void setItemName(pdb :: String &itemIdIn){
            objectName = itemIdIn;
        }

        void setLibraryBytes(pdb :: String &bytesIn){
            libraryBytes = bytesIn;
        }

        string printShort(){
            string output;
            output = "   Type ID: ";
            output.append(getObjectID().c_str()).append(" | Type Name: ").append(getItemName().c_str());
            return output;
        }


        friend std::ostream& operator<<(std::ostream &out, CatalogUserTypeMetadata &userDefinedObject) {
            out << "\nCatalog User-defined Type Metadata"<< endl;
            out << "-------------------"<< endl;
            out << "      Type Id: " << userDefinedObject.getObjectID().c_str() << endl;
            out << "     Type Key: " << userDefinedObject.getItemKey().c_str() << endl;
            out << "    Type Name: " << userDefinedObject.getItemName().c_str() << endl;
            out << "-------------------\n"<< endl;
           return out;
       }



    private:
        // numeric index to indicate the position on the vector
        pdb :: String itemId;
        // numeric unique ID for a user-defined object starts from 8192
        pdb :: String objectID;
        // object type, e.g. data_type, metric
        pdb :: String objectType;
        // the name of the user-defined object
        pdb :: String objectName;
        // the bytes containing the .so library file
        pdb :: String libraryBytes;

    };

} /* namespace pdb */

//#include "CatalogUserTypeMetadata.cc"

#endif /* CATALOG_USER_TYPE_METADATA_H_ */
