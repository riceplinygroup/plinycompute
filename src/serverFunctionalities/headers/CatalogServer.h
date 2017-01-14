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

#ifndef CATALOG_SERVER_H
#define CATALOG_SERVER_H

#include "ServerFunctionality.h"
#include "PDBServer.h"
#include "PDBCatalog.h"
#include "CatalogClient.h"

namespace pdb {

class CatalogServer : public ServerFunctionality {

public:

	// destructor
	~CatalogServer ();

	// these give us the port and the address of the catalog
	CatalogServer (std :: string catalogDirectory, bool isMasterCatalogServer, std :: string masterIP, int masterPort);

	// from the ServerFunctionality interface
	void registerHandlers (PDBServer &forMe) override;

	// this uses the name of the object to find the corresponding identifier
	int16_t searchForObjectTypeName (string objectTypeName);

	// this uses the identerifer for the object to find the corresponding type name
	string searchForObjectTypeName (int16_t typeIdentifier);

	// this downloads the shared libreary assoicated with the identifier, putting it at the specified location
	bool getSharedLibrary (int16_t identifier, vector <char> &putResultHere, std :: string &errMsg);

    // this downloads the shared libreary assoicated with the typeName, putting it at the specified location
    bool getSharedLibraryByName (int16_t identifier, std :: string typeName, vector <char> &putResultHere, Handle <CatalogUserTypeMetadata> &itemMetadata, string &returnedBytes, std :: string &errMsg);

	// this returns the type of object in the specified set, as a type name
	int16_t getObjectType (string databaseName, string setName);

	// this creates a new database... returns true on success
	bool addDatabase (string databaseName, string &errMsg);

    // this deletes a database... returns true on success
    bool deleteDatabase (string databaseName, string &errMsg);

	// deletes a set from the database
	bool deleteSet (std :: string databaseName, std :: string setName, std :: string &errMsg);

	// this creates a new set in a given database... returns true on success
	bool addSet (int16_t typeIdentifier, string databaseName, string setName, string &errMsg);
	
    // this adds a node to a set... returns true on success
    // returns true on success, false on fail
    bool addNodeToSet (std :: string nodeIP, std :: string databaseName, std :: string setName, std :: string &errMsg);

    // this adds a node to a DB... returns true on success
    // returns true on success, false on fail
    bool addNodeToDB (std :: string nodeIP, std :: string databaseName, std :: string &errMsg);

    // this adds a node to a set... returns true on success
    // returns true on success, false on fail
    bool removeNodeFromSet (std :: string nodeIP, std :: string databaseName, std :: string setName, std :: string &errMsg);

    // this adds a node to a DB... returns true on success
    // returns true on success, false on fail
    bool removeNodeFromDB (std :: string nodeIP, std :: string databaseName, std :: string &errMsg);

	// adds a new object type... return -1 on failure
	int16_t addObjectType (vector <char> &soFile, string &errMsg);
	
	// print the content of the catalog metadata
	bool printCatalog (string item);

	// registers metadata about a node in the cluster
	bool addNodeMetadata (Handle<CatalogNodeMetadata> &nodeMetadata, std :: string &errMsg);

    // registers metadata about a database in the cluster
    bool addDatabaseMetadata (Handle<CatalogDatabaseMetadata> &dbMetadata, std :: string &errMsg);

    // updates metadata about a database in the cluster
    bool updateDatabaseMetadata (Handle<CatalogDatabaseMetadata> &dbMetadata, std :: string &errMsg);

    // registers metadata about a set in the cluster
    bool addSetMetadata (Handle<CatalogSetMetadata> &setMetadata, std :: string &errMsg);

	// returns whether or not this is the master catalog server
	bool getIsMasterCatalogServer();

	// sets the type of Catalog Server
	void setIsMasterCatalogServer(bool isMasterCatalogServerIn);

	// broadcast a piece of metadata to all available nodes in a cluster
	// returns a map with the results from updating each node in the cluster
	template<class Type>
	bool broadcastCatalogUpdate (Handle<Type> metadataToSend,
	                             map <string, pair<bool, string>> &broadcastResults,
	                             string &errMsg);

    // broadcast a piece of metadata to all available nodes in a cluster
    // returns a map with the results from deleting that item on the
	// catalog copy on each node in the cluster
    template<class Type>
    bool broadcastCatalogDelete (Handle<Type> metadataToSend,
                                 map <string, pair<bool, string>> &broadcastResults,
                                 string &errMsg);

    // returns true if the node is already registered in the Catalog
    bool isNodeRegistered(string nodeIP);

    // returns true if the database is already registered in the Catalog
    bool isDatabaseRegistered(string dbName);

    // returns true if the set for this database is already registered in the Catalog
    bool isSetRegistered(string dbName, string setName);

	// returns a reference to the catalog
	PDBCatalogPtr getCatalog();

	// returns metadata for a user-defined type along with the .so file
	// bytes enclosed as a string
	bool retrieveUserDefinedTypeMetadata(string typeName, Handle<CatalogUserTypeMetadata> &itemMetadata, string &soFileBytes, string &errMsg);


private:
	// new catalog metadata containers
    //TODO some containers will be removed, here for testing purposes
    Handle<Vector <CatalogNodeMetadata> > _allNodesInCluster;
    Handle<Vector <CatalogSetMetadata> > _setTypes;
    Handle<Vector <CatalogDatabaseMetadata> > _allDatabases;
    Handle<Vector <CatalogUserTypeMetadata> > _udfsValues;

    // **end

	// map from type name string to int, and vice/versa
	map <string, int16_t> allTypeNames;
	map <int16_t, string> allTypeCodes;

//	// map from database name to list of sets
//	map <string, vector <string>> allDatabases;
		
	// vector of nodes in the cluster
	vector <string> allNodesInCluster;

    // map from database/set pair to type of set
    map <pair <string, string>, int16_t> setTypes;

    // interface to a persistent catalog storage for storing and retrieving PDB metadata
    PDBCatalogPtr pdbCatalog;

    // Catalog client helper to connect to the Master Catalog Server
    CatalogClient catalogClientConnectionToMasterCatalogServer;

	// where the catalog is located
	std :: string catalogDirectory;

	// serialize access
	pthread_mutex_t workingMutex;

    // whether or not this is the master catalog server
    //
    // if this is the master catalog server, it will receive a metadata registration
    // request and perform the following operations:
    //
    //   1) update metadata in the local master catalog database
    //   2) iterate over all registered nodes in the cluster and send the metadata object
    //   3) update catalog version

    // if this is the a worker catalog instance, it will receive a metadata registration
    // request from the master catalog server and perform the following operations:
    //
    //   1) update metadata in the local catalog database
    //   2) update catalog version
    //   3) send ack to master catalog server

    bool isMasterCatalogServer;

    string masterIP = "localhost";
    int masterPort = 8108;
    PDBLoggerPtr catServerLogger;

};


}

#endif
