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
 * CatalogStandardSetMetadata.h
 *
 *  Created on: Dec 8, 2016
 *      Author: carlos
 */

#ifndef SRC_CATALOG_CATALOGSTANDARDSETMETADATA_H_
#define SRC_CATALOG_CATALOGSTANDARDSETMETADATA_H_

#include <iostream>
#include <string>

using namespace std;

class CatalogStandardSetMetadata {
    public:

        CatalogStandardSetMetadata();

        CatalogStandardSetMetadata(string pdbSetIdIn, string pdbSetKeyIn, string pdbSetNameIn,
                                     string pdbDatabaseIdIn, string pdbDatabaseNameIn, string pdbTypeIdIn, string pdbTypeNameIn);

        CatalogStandardSetMetadata(const CatalogStandardSetMetadata& pdbSetToCopy);

        void setValues(string pdbSetIdIn, string pdbSetKeyIn, string pdbSetNameIn,
                                      string pdbDatabaseIdIn, string pdbDatabaseNameIn, string pdbTypeIdIn,
                                      string pdbTypeNameIn);

        ~CatalogStandardSetMetadata();

        // The set ID is used as key for storing metadata
        string getItemKey();

        string getItemId();

        string getDBId();

        string getObjectTypeId();


        string getItemName();

        string getDBName();

        string getObjectTypeName();

        void setTypeName(string &typeNameIn);

        void setDBName(string &pdbDatabaseNameIn);

        void setDBId(string &dbIdIn);

        void setTypeId(string &typeIdIn);

        void setItemKey(string &itemKeyIn);

        void setItemId(string &itemIdIn);

        void setItemName(string &itemNameIn);

        string printShort();

        friend std::ostream& operator <<(std::ostream &out, CatalogStandardSetMetadata &catalogSet) {
            out << "\nCatalogSet Metadata"<< endl;
            out << "-------------------"<< endl;
    //        out << "      DB Id: " << catalogSet.getDBId() << endl;
    //        out << "    DB name: " << catalogSet.getDBName() << endl;
    //        out << "     Set Id: " << catalogSet.getItemId() << endl;
    //        out << "    Set Key: " << catalogSet.getItemKey() << endl;
    //        out << "   Set Name: " << catalogSet.getItemName() << endl;
    //        out << "    Type Id: " << catalogSet.getObjectTypeId() << endl;
    //        out << "  Type Name: " << catalogSet.getObjectTypeName() << endl;
            out << "-------------------\n"<< endl;
           return out;
       }

    private:
        string pdbSetId;
        string setKey;
        string pdbSetName;

        string pdbDatabaseId;
        string pdbDatabaseName;

        string typeId;
        string typeName;

};

#endif /* SRC_CATALOG_CATALOGSTANDARDSETMETADATA_H_ */
