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
 * CatalogStandardDatabaseMetadata.h
 *
 *  Created on: Dec 8, 2016
 *      Author: carlos
 */

#ifndef SRC_CATALOG_CATALOGSTANDARDDATABASEMETADATA_H_
#define SRC_CATALOG_CATALOGSTANDARDDATABASEMETADATA_H_

#include <iostream>
#include <string>
#include <vector>
#include <map>

#include <CatalogStandardPermissionsMetadata.h>
#include "PDBDebug.h"

using namespace std;

// This class serves to store information about the databases in a given instance of PDB
// and provide methods for maintaining their associated metadata.
// Clients of this class will access this information using a handler to the catalog.

class CatalogStandardDatabaseMetadata {
public:
    CatalogStandardDatabaseMetadata();

    CatalogStandardDatabaseMetadata(string dbIdIn,
                                    string dbNameIn,
                                    string userCreatorIn,
                                    string createdOnIn,
                                    string lastModifiedIn);

    CatalogStandardDatabaseMetadata(const CatalogStandardDatabaseMetadata& pdbDatabaseToCopy);

    void setValues(string dbIdIn,
                   string dbNameIn,
                   string userCreatorIn,
                   string createdOnIn,
                   string lastModifiedIn);


    ~CatalogStandardDatabaseMetadata();


    void addPermission(CatalogStandardPermissionsMetadata& permissionsIn);

    void addNode(string& nodeIn);

    void addSet(string& setIn);

    void addSetToMap(string& setName, string& nodeIP);

    void addNodeToMap(string& nodeIP, string& setName);


    void addType(string& typeIn);


    void replaceListOfSets(vector<string>& newList);

    void replaceListOfNodes(vector<string>& newList);

    void replaceMapOfSets(map<string, vector<string>>& newMap);

    void replaceMapOfNodes(map<string, vector<string>>& newMap);

    /**
     * Deletes a set from the listOfSets, along with the set->nodes map and the nodes->set map
     * @param whichSet
     */
    void deleteSet(string setName);

    void removeNodeFromSet(string node, string set);

    void deleteNodeFromMap(string& nodeIP, string& setName);

    void deleteType(string& typeIn);


    vector<string> getListOfNodes();


    vector<string> getListOfSets();

    vector<string> getListOfTypes();


    vector<CatalogStandardPermissionsMetadata> getListOfPermissions();

    string getItemId();

    string getItemName();

    string getItemKey();

    string getUserCreator();

    string getCreatedOn();

    string getLastModified();

    void setItemKey(string& itemKeyIn);

    void setItemId(string& idIn);

    void setItemName(string& itemNameIn);

    map<string, vector<string>> getSetsInDB();

    map<string, vector<string>> getNodesInDB();

    string printShort();

    friend std::ostream& operator<<(std::ostream& out, CatalogStandardDatabaseMetadata& database) {
        out << "\nCatalog Database Metadata" << endl;
        out << "-------------------" << endl;
        out << "      DB Id: " << database.getItemId().c_str() << endl;
        out << "     DB Key: " << database.getItemKey().c_str() << endl;
        out << "    DB Name: " << database.getItemName().c_str() << endl;
        out << "\nThis Database is stored in the following nodes: " << endl;
        for (int i = 0; i < database.getListOfNodes().size(); i++) {
            //            out << "    IP: " << database.getListOfNodes().[i] << endl;
        }
        out << "\nThis Database has the following sets: " << endl;
        for (int i = 0; i < database.getListOfSets().size(); i++) {
            out << "    Set: " << database.getListOfSets()[i].c_str() << endl;
        }

        out << "-------------------\n" << endl;
        return out;
    }

private:
    string dbId;
    string dbName;
    string userCreator;
    string createdOn;
    string lastModified;

    // a map where the key is the name of a set and the value is a vector with
    // all nodes where that set has information stored
    map<string, vector<string>> setsInDB;

    // a map where the key is the IP of a node and the value is a vector with
    // all sets in that node that contain data for this database
    map<string, vector<string>> nodesInDB;

    // Contains information about nodes in the cluster with data for a given database
    vector<string> listOfNodes;

    // Contains information about sets in the cluster containing data for a given database
    // this might change, check with Jia if this is needed or not
    vector<string> listOfSets;

    // Contains information about types in the cluster containing data for a given database
    // this might change, check with Jia if this is needed or not
    vector<string> listOfTypes;

    // Contains all users' permissions for a given database
    vector<CatalogStandardPermissionsMetadata> listOfPermissions;

    void deleteSetFromSetList(string& setName);

    void deleteSetFromSetMap(string& setName);

    void deleteNodeFromSingleSet(string& node, string& setName);

    void deleteSetFromSingleNode(string& setName, string& node);

    void deleteSetFromNodeMap(string& setName);
};


#endif /* SRC_CATALOG_CATALOGSTANDARDDATABASEMETADATA_H_ */
