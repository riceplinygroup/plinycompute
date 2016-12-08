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
 * CatalogStandardUserTypeMetadata.h
 *
 *  Created on: Dec 8, 2016
 *      Author: carlos
 */

#ifndef SRC_CATALOG_CATALOGSTANDARDUSERTYPEMETADATA_H_
#define SRC_CATALOG_CATALOGSTANDARDUSERTYPEMETADATA_H_

#include <iostream>
#include <string>

using namespace std;

    /**
     *   CatalogStandardUserTypeMetadata encapsulates information for a given user-defined object
     *   (either a data type or metric), including:
     *
     *   1) objectID: numeric identifier assigned automatically by the catalog
     *   2) objectType: (data_types or metrics)
     *   3) objectName: a string given by the user
     */

class CatalogStandardUserTypeMetadata {

    public :

        CatalogStandardUserTypeMetadata();

        CatalogStandardUserTypeMetadata(string itemIdIn,
                                string objectIDIn,
                                string objectTypeIn,
                                string objectNameIn,
                                string libraryBytesIn);

        CatalogStandardUserTypeMetadata(const CatalogStandardUserTypeMetadata& pdbCatalogEntryToCopy);

        ~CatalogStandardUserTypeMetadata();

        string getItemKey();

        string getItemId();

        string getObjectID();

        string getObjectType();

        string getItemName();

        string getLibraryBytes();

        void setObjectId(string &objectIdIn);

        void setItemId(string &itemIdIn);

        void setItemKey(string &itemKeyIn);

        void setItemName(string &itemIdIn);

        void setLibraryBytes(string &bytesIn);

        string printShort();

        friend std::ostream& operator<<(std::ostream &out, CatalogStandardUserTypeMetadata &userDefinedObject) {
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
        string itemId;
        // numeric unique ID for a user-defined object starts from 8192
        string objectID;
        // object type, e.g. data_type, metric
        string objectType;
        // the name of the user-defined object
        string objectName;
        // the bytes containing the .so library file
        string libraryBytes;

};

#endif /* SRC_CATALOG_CATALOGSTANDARDUSERTYPEMETADATA_H_ */
