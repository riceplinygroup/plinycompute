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
 * CatalogStandardNodeMetadata.h
 *
 *  Created on: Dec 8, 2016
 *      Author: carlos
 */

#ifndef SRC_CATALOG_CATALOGSTANDARDNODEMETADATA_H_
#define SRC_CATALOG_CATALOGSTANDARDNODEMETADATA_H_

#include <iostream>
#include <string>
#include "CatalogNodeMetadata.h"

using namespace std;

// This class serves to store information about a node in a cluster of PDB.
// It also provides methods for maintaining their associated metadata.
// Clients of this class will access this information using a handler to the catalog.

class CatalogStandardNodeMetadata {
    public:

        CatalogStandardNodeMetadata();

        CatalogStandardNodeMetadata(string nodeIdValue, string nodeIPValue, int nodePortValue, string nodeNameValue, string nodeTypeValue, int nodeStatusValue);


        //Copy constructor
        CatalogStandardNodeMetadata(const CatalogStandardNodeMetadata& pdbNodeToCopy);

        ~CatalogStandardNodeMetadata();

        void setValues(string nodeIdValue, string nodeIPValue, int nodePortValue, string nodeNameValue, string nodeTypeValue, int nodeStatusValue);

        string getItemKey();

        string getNodeIP();

        string getItemId();

        string getItemName();

        string getNodeType();

        int getNodePort();

        int getNodeStatus();

        void setItemKey(string &itemKeyIn);

        void setItemIP(pdb :: String &itemIPIn);

        void setItemId(string &itemIdIn);

        void setItemName(string &itemNameIn);

        void setNodePort(int &portIn);

        string printShort();

        void toStandardMetadata(pdb :: Handle<pdb :: CatalogNodeMetadata> &convertedItem);

        friend std::ostream& operator<<(std::ostream &out, CatalogStandardNodeMetadata &node) {
            out << "\nCluster Node Metadata"<< endl;
            out << "-------------------"<< endl;
            out << "       Node Id: " << node.getItemId().c_str() << endl;
            out << "       Node IP: " << node.getNodeIP().c_str() << endl;
            out << "     Node Port: " << node.getNodePort() << endl;
            out << "     Node Name: " << node.getItemName().c_str() << endl;
            out << "     Node Type: " << node.getNodeType().c_str() << endl;
            out << "-------------------\n"<< endl;
           return out;
       }

    private:

        string nodeId;
        string nodeIP;
        int nodePort;
        string nodeName;
        string nodeType;
        int nodeStatus;
        string nodeAddress;
};

#endif /* SRC_CATALOG_CATALOGSTANDARDNODEMETADATA_H_ */
