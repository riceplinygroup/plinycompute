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
                                 pdb::String createdOnIn, pdb::String lastModifiedIn,
                                 pdb :: Handle <pdb:: Vector < String> >  listOfNodesIn):
                dbId(dbIdIn),
                dbName(dbNameIn),
                userCreator(userCreatorIn),
                createdOn(createdOnIn),
                lastModified(lastModifiedIn),
                listOfNodes(listOfNodesIn)
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
            cout << "Adding info about node " << nodeIn.c_str() << endl;
            listOfNodes->push_back(nodeIn);
        }

        void addSet(pdb::String &setIn){
            listOfSets->push_back(setIn);
        }

        void addSetToMap(String &key, String &value){
            (*mapOfSets)[key] = value;
        }

        void addType(pdb::String &typeIn){
            listOfTypes->push_back(typeIn);
        }

        void replaceListOfSets(Handle< Vector <pdb::String>> &newList){
            listOfSets = newList;
        }


        void deleteSet(pdb :: String whichSet){
            // creates a temp vector
            pdb :: Handle <pdb:: Vector < String>  > tempListOfSets = makeObject< Vector<String>>();

            for (int i=0; i < (*getListOfSets()).size(); i++){
                String itemValue = (*getListOfSets())[i];
                if (itemValue!=whichSet){
                    tempListOfSets->push_back(itemValue);
                }
            }
            replaceListOfSets(tempListOfSets);
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

        string printShort(){
            string output;
            string spaces("\n                                               ");
            output = "   DB Id: ";
            output.append("\n").append(getItemId().c_str()).append(" | DB: ").append(getItemKey().c_str()).append(" | Sets(").append(to_string(getListOfSets()->size())).append("): [ ");
            for (int i=0; i < getListOfSets()->size(); i++){
                if (i>0) output.append(", ").append(spaces).append((*getListOfSets())[i].c_str());
                else output.append((*getListOfSets())[i].c_str());
            }
            output.append(" ]");

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

    };

} /* namespace pdb */

//#include "CatalogDatabaseMetadata.cc"


#endif /* CATALOG_DATABASE_METADATA_H_ */
