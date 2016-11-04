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
 * CatalogDatabaseMetadata.h
 *
 *  Created on: Sept 12, 2016
 *      Author: carlos
 */

#ifndef CATALOG_DATABASE_METADATA_H_
#define CATALOG_DATABASE_METADATA_H_

#include <iostream>
#include <map>

#include "Handle.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "CatalogNodeMetadata.h"
#include "CatalogPermissionsMetadata.h"
#include "CatalogSetMetadata.h"
#include "CatalogUserTypeMetadata.h"
#include "Object.h"

//  PRELOAD %CatalogDatabaseMetadata%

using namespace std;

namespace pdb {

// This class serves to store information about the databases in a given instance of PDB
// and provide methods for maintaining their associated metadata.
// Clients of this class will access this information using a handler to the catalog.

    class CatalogDatabaseMetadata : public Object  {
    public:

        CatalogDatabaseMetadata() {
        }

        CatalogDatabaseMetadata(pdb :: String dbIdIn, pdb::String dbNameIn, pdb::String userCreatorIn,
                                 pdb::String createdOnIn, pdb::String lastModifiedIn):
                dbId(dbIdIn),
                dbName(dbNameIn),
                userCreator(userCreatorIn),
                createdOn(createdOnIn),
                lastModified(lastModifiedIn)
        {
        }

        CatalogDatabaseMetadata(const CatalogDatabaseMetadata& pdbDatabaseToCopy) {
            dbId = pdbDatabaseToCopy.dbId;
            dbName = pdbDatabaseToCopy.dbName;
            userCreator = pdbDatabaseToCopy.userCreator;
            createdOn = pdbDatabaseToCopy.createdOn;
            lastModified = pdbDatabaseToCopy.lastModified;
            listOfNodes = pdbDatabaseToCopy.listOfNodes;
            listOfSets = pdbDatabaseToCopy.listOfSets;
            listOfTypes = pdbDatabaseToCopy.listOfTypes;
            setsInDB = pdbDatabaseToCopy.setsInDB;
            nodesInDB = pdbDatabaseToCopy.nodesInDB;
        }

        CatalogDatabaseMetadata(const Handle<CatalogDatabaseMetadata>& pdbDatabaseToCopy) {
            dbId = pdbDatabaseToCopy->dbId;
            dbName = pdbDatabaseToCopy->dbName;
            userCreator = pdbDatabaseToCopy->userCreator;
            createdOn = pdbDatabaseToCopy->createdOn;
            lastModified = pdbDatabaseToCopy->lastModified;
            listOfNodes = pdbDatabaseToCopy->listOfNodes;
            listOfSets = pdbDatabaseToCopy->listOfSets;
            listOfTypes = pdbDatabaseToCopy->listOfTypes;
            setsInDB = pdbDatabaseToCopy->setsInDB;
            nodesInDB = pdbDatabaseToCopy->nodesInDB;
        }


        void setValues(String dbIdIn, pdb::String dbNameIn, pdb::String userCreatorIn, pdb::String createdOnIn, pdb::String lastModifiedIn){
            dbId = dbIdIn;
            dbName = dbNameIn;
            userCreator = userCreatorIn;
            createdOn = createdOnIn;
            lastModified = lastModifiedIn;

        }


        ~CatalogDatabaseMetadata() {
            // TODO Auto-generated destructor stub
        }


        void addPermission( CatalogPermissionsMetadata &permissionsIn){
            listOfPermissions->push_back(permissionsIn);
        }

        void addNode(pdb::String &nodeIn){
//            cout << "Adding node " << nodeIn.c_str() << endl;
            listOfNodes->push_back(nodeIn);
        }

        void addSet(pdb::String &setIn){
//            cout << "Adding node " << setIn.c_str() << endl;
            listOfSets->push_back(setIn);
        }

        void addSetToMap(String &setName, String &nodeIP){
//            cout << "key: " << setName.c_str() << " push_back node: " << nodeIP.c_str();
            (*setsInDB)[setName].push_back(nodeIP);
        }

        void addNodeToMap(String &nodeIP, String &setName){
//            cout << "key: " << nodeIP.c_str() << " push_back set: " << setName.c_str();
            (*nodesInDB)[nodeIP].push_back(setName);
        }


        void addType(pdb::String &typeIn){
            listOfTypes->push_back(typeIn);
        }


        void replaceListOfSets(Handle< Vector <pdb::String>> &newList){
            listOfSets = newList;
        }

        void replaceListOfNodes(Handle< Vector <pdb::String>> &newList){
            listOfNodes = newList;
        }

        void replaceMapOfSets(Handle <Map <String, Vector<String>>> &newMap){
            setsInDB = newMap;
        }

        void replaceMapOfNodes(Handle <Map <String, Vector<String>>> &newMap){
            nodesInDB = newMap;
        }

        /**
         * Deletes a set from the listOfSets, along with the set->nodes map and the nodes->set map
         * @param whichSet
         */
        void deleteSet(pdb :: String setName){
            deleteSetFromSetList(setName);
            deleteSetFromSetMap(setName);
            deleteSetFromNodeMap(setName);
        }

        void deleteNodeFromMap(String &nodeIP, String &setName){
            // creates a temp vector
            pdb :: Handle <pdb:: Vector < String>  > tempListOfNodes = makeObject< Vector<String>>();

            for (int i=0; i < (*getListOfNodes()).size(); i++){
                String itemValue = (*getListOfNodes())[i];
                if (itemValue!=setName){
                    tempListOfNodes->push_back(itemValue);
                }
            }
            replaceListOfNodes(tempListOfNodes);
        }

        void deleteType(void *typeIn){
            (*listOfTypes).deleteObject(typeIn);
        }



//        pdb :: Handle <pdb:: Vector < pdb :: Handle<String> > > getListOfNodes(){
//            return listOfNodes;
//        }

        pdb :: Handle <pdb:: Vector < String> >    getListOfNodes(){
            return listOfNodes;
        }


        pdb :: Handle <pdb:: Vector <String> > getListOfSets(){
            return listOfSets;
        }

        pdb :: Handle <pdb:: Vector < String> >  getListOfTypes(){
            return listOfTypes;
        }


        pdb :: Handle <pdb:: Vector < pdb :: CatalogPermissionsMetadata> >getListOfPermissions(){
            return listOfPermissions;
        }

        String getItemId(){
            return dbId;
        }

        String getItemName(){
            return dbName;
        }

        String getItemKey(){
            return dbName;
        }

        String getUserCreator(){
            return userCreator;
        }

        String getCreatedOn(){
            return createdOn;
        }

        String getLastModified(){
            return lastModified;
        }

        void setItemKey(String &itemKeyIn){
            dbName = itemKeyIn;
        }

        void setItemId(String &idIn){
            dbId = idIn;
        }

        void setItemName(String &itemNameIn){
            dbName = itemNameIn;
        }

        Handle <Map <String, Vector<String>>> getSetsInDB(){
            return setsInDB;
        }

        Handle <Map <String, Vector<String>>> getNodesInDB(){
            return nodesInDB;
        }

        string printShort(){
            string output;
//            string spaces("\n                                               ");
            string spaces("");
            output = "   \nDB ";
            output.append(getItemId().c_str()).append(":").append(getItemKey().c_str());
                    //.append(" has (").append(to_string(getListOfSets()->size())).append(") sets: [ ");
//            for (int i=0; i < getListOfSets()->size(); i++){
//                if (i>0) output.append(", ").append(spaces).append((*getListOfSets())[i].c_str());
//                else output.append((*getListOfSets())[i].c_str());
//            }
//            output.append(" ]");
//            for (auto &item : (*nodesInDB)){
//                output.append("\n -Set: ").append(item.key.c_str()).append(" is stored in (").append(to_string(item.value.size())).append(") Nodes: [ ");
//                for (int i=0; i < item.value.size(); i++){
//                    if (i>0) output.append(", ").append(spaces).append(item.value[i].c_str());
//                    else output.append(item.value[i].c_str());
//                }
//            }
//            output.append(" ]");

            int i = 0;
            output.append("\n is stored in (").append(to_string((*nodesInDB).size())).append(")nodes: [ ");
            for (auto &item : (*nodesInDB)){
                if (i>0) output.append(", ").append(spaces).append(item.key.c_str());
                else output.append(item.key.c_str());
                i++;
            }

            output.append(" ]\n and has (").append(to_string((*setsInDB).size())).append(")sets: [ ");
            i = 0;
            for (auto &item : (*setsInDB)){
                if (i>0) output.append(", ").append(spaces).append(item.key.c_str());
                else output.append(item.key.c_str());
                i++;
            }
            output.append(" ]");

            for (auto &item : (*setsInDB)){
                output.append("\n  * Set: ").append(item.key.c_str()).append(" is stored in (").append(to_string(item.value.size())).append(")nodes: [ ");
                for (int i=0; i < item.value.size(); i++){
                    if (i>0) output.append(", ").append(spaces).append(item.value[i].c_str());
                    else output.append(item.value[i].c_str());
                }
                output.append(" ]");
            }

            return output;
        }

        friend std::ostream& operator<<(std::ostream &out, CatalogDatabaseMetadata &database) {
            out << "\nCatalog Database Metadata"<< endl;
            out << "-------------------"<< endl;
            out << "      DB Id: " << database.getItemId().c_str() << endl;
            out << "     DB Key: " << database.getItemKey().c_str() << endl;
            out << "    DB Name: " << database.getItemName().c_str() << endl;
            out << "\nThis Database is stored in the following nodes: " << endl;
            for (int i=0; i < database.getListOfNodes()->size(); i++){
    //            out << "    IP: " << database.getListOfNodes()->[i] << endl;
            }
            out << "\nThis Database has the following sets: " << endl;
            for (int i=0; i < database.getListOfSets()->size(); i++){
                out << "    Set: " << (*database.getListOfSets())[i].c_str() << endl;
            }

            out << "-------------------\n"<< endl;
           return out;
       }

        // overloads << operator
//        friend std::ostream& operator<<(std::ostream &out, CatalogDatabaseMetadata &database);

        ENABLE_DEEP_COPY

    private:
        pdb::String dbId;
        pdb::String dbName;
        pdb::String userCreator;
        pdb::String createdOn;
        pdb::String lastModified;

        Handle <Map <String, String>> mapOfSets = makeObject <Map <String, String>> ();

        // a map where the key is the name of a set and the value is a vector with
        // all nodes where that set has information stored
        Handle <Map <String, Vector<String>>> setsInDB = makeObject <Map <String, Vector<String>>> ();

        // a map where the key is the IP of a node and the value is a vector with
        // all sets in that node that contain data for this database
        Handle <Map <String, Vector<String>>> nodesInDB = makeObject <Map <String, Vector<String>>> ();

        // Contains information about nodes in the cluster with data for a given database
        pdb :: Handle <pdb:: Vector < String>  > listOfNodes = makeObject< Vector<String>>();
//        Vector <String> listOfNodes;

        // Contains information about sets in the cluster containing data for a given database
        // this might change, check with Jia if this is needed or not
        pdb :: Handle <pdb:: Vector < String>  > listOfSets = makeObject< Vector<String>>();

        // Contains information about types in the cluster containing data for a given database
        // this might change, check with Jia if this is needed or not
        pdb :: Handle <pdb:: Vector < String> >  listOfTypes = makeObject< Vector<String>>();

        // Contains information about types a given database
//        Vector < PDBCatalogRegisteredObject> listOfTypes;

        // Contains all users' permissions for a given database
        pdb :: Handle <pdb:: Vector < CatalogPermissionsMetadata> >listOfPermissions = makeObject< Vector<CatalogPermissionsMetadata>>();

        void deleteSetFromSetList(String &setName) {
            // creates a temp vector
            pdb :: Handle <pdb:: Vector < String>  > tempListOfSets = makeObject< Vector<String>>();
            for (int i=0; i < (*getListOfSets()).size(); i++){
                String itemValue = (*getListOfSets())[i];
                if (itemValue!=setName){
                    tempListOfSets->push_back(itemValue);
                }
            }
            replaceListOfSets(tempListOfSets);
        }

        void deleteSetFromSetMap(String &setName) {
            // creates a temp vector
            Handle <Map <String, Vector<String>>> tempSetsInDB = makeObject<Map <String, Vector<String>>>();
            for (auto &a : *getSetsInDB()) {
                if (a.key!=setName){
                    (*tempSetsInDB)[a.key] = a.value;
                }
            }
            replaceMapOfSets(tempSetsInDB);
        }

        void deleteSetFromNodeMap(String &setName) {
            Handle <Map <String, Vector<String>>> tempNodesInDB = makeObject<Map <String, Vector<String>>>();
            for (const auto &setsInNode : (* nodesInDB)){
                auto node = setsInNode.key;
                auto sets = setsInNode.value;
                auto newSetsInNode = (* tempNodesInDB)[node];
                for (int i = 0; i < sets.size(); i++) {
                    if (sets[i] != setName) {
                        newSetsInNode.push_back(sets[i]);
                    }
                }
            }
            replaceMapOfNodes(tempNodesInDB);
        }
    };

} /* namespace pdb */

//#include "CatalogDatabaseMetadata.cc"


#endif /* CATALOG_DATABASE_METADATA_H_ */
