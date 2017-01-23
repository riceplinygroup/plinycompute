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
#include "PDBDebug.h"
#include "PDBServer.h"
#include "PDBCatalog.h"
#include "CatalogClient.h"

// Class for handling the Catalog Server functionality. This can be used
// either by the Catalog Master Server or any Worker Node in the cluster
//
// If this is the master catalog server, it will receive a metadata registration
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

namespace pdb {

class CatalogServer : public ServerFunctionality {

public:

	// destructor
	~CatalogServer ();

    // Creates a Catalog Server
    //        catalogDirectory: the path of the location of the catalog
    //   isMasterCatalogServer: true if this is the Master Catalog Server
	//                          workers nodes have this parameter set to false
    //                masterIP: the IP address of the Master Catalog
	//              masterPort: the port number of the Master Catalog
	//
	CatalogServer (std :: string catalogDirectory, bool isMasterCatalogServer,
	               std :: string masterIP, int masterPort);

	// from the ServerFunctionality interface
	void registerHandlers (PDBServer &forMe) override;

	// Returns the type Id of a type given its name
	int16_t searchForObjectTypeName (string objectTypeName);

	// Returns the name of a type given it's type Id
	string searchForObjectTypeName (int16_t typeIdentifier);

    // Retrieves the content of a Shared Library given it's Type Id
	// putting it at the specified location
	bool getSharedLibrary (int16_t identifier, vector <char> &putResultHere,
	                       std :: string &errMsg);

    // Retrieves the content of a Shared Library along with its registered metadata,
    // given it's typeName. Typically this method is invoked by a remote machine that
    // has no knowledge of the typeID
    bool getSharedLibraryByTypeName (std :: string typeName,
                                     Handle <CatalogUserTypeMetadata> &typeMetadata,
                                     string &sharedLibraryBytes, std :: string &errMsg);

	// Returns the type of an object in the specified set, as a type name
	int16_t getObjectType (string databaseName, string setName);

	// Creates a new database... returns true on success
	bool addDatabase (string databaseName, string &errMsg);

    // Deletes a database... returns true on success
    bool deleteDatabase (string databaseName, string &errMsg);

	// Deletes a set from the database
	bool deleteSet (std :: string databaseName, std :: string setName, std :: string &errMsg);

	// Creates a new set in a given database... returns true on success
	bool addSet (int16_t typeIdentifier, string databaseName, string setName, string &errMsg);
	
    // Adds information about a node to a set for a given database
    // returns true on success, false on fail
    bool addNodeToSet (std :: string nodeIP, std :: string databaseName, std :: string setName,
                       std :: string &errMsg);

    // Adds information about a node to a database
    // returns true on success, false on fail
    bool addNodeToDB (std :: string nodeIP, std :: string databaseName, std :: string &errMsg);

    // Removes information about a node from a set, this is invoked when storage
    // removes a set for a database in a node in the cluster
    // returns true on success, false on fail
    bool removeNodeFromSet (std :: string nodeIP, std :: string databaseName, std :: string setName,
                            std :: string &errMsg);

    // Removes information about a node from a database, this is invoked when storage
    // removes a database in a node in the cluster
    // returns true on success, false on fail
    bool removeNodeFromDB (std :: string nodeIP, std :: string databaseName, std :: string &errMsg);

    // Adds a new object type... return -1 on failure, this is done on a worker node catalog
	// the typeID is given by the master catalog
    int16_t addObjectType (int16_t typeID, string &soFile, string &errMsg);

	// Print the content of the catalog metadata that have changed since a given timestamp
	bool printCatalog (string timeStamp);

	// Adds metadata about a new node in the cluster
	bool addNodeMetadata (Handle<CatalogNodeMetadata> &nodeMetadata, std :: string &errMsg);

    // Adds metadata about a new database in the cluster
    bool addDatabaseMetadata (Handle<CatalogDatabaseMetadata> &dbMetadata, std :: string &errMsg);

    // Adds metadata about a new set in the cluster
    bool addSetMetadata (Handle<CatalogSetMetadata> &setMetadata, std :: string &errMsg);

    // Updates metadata changes about a database in the cluster
    bool updateDatabaseMetadata (Handle<CatalogDatabaseMetadata> &dbMetadata, std :: string &errMsg);

	// Returns true if this is the master catalog server
	bool getIsMasterCatalogServer();

	// Sets if this is the Master Catalog Server (true) or not (false)
	void setIsMasterCatalogServer(bool isMasterCatalogServerIn);

	// Broadcasts a metadata item to all available nodes in a cluster, when an update has occurred
	// returns a map with the results from updating each node in the cluster
	template<class Type>
	bool broadcastCatalogUpdate (Handle<Type> metadataToSend,
	                             map <string, pair<bool, string>> &broadcastResults,
	                             string &errMsg);

    // Broadcasts a metadata item to all available nodes in a cluster, when an delete has occurred
    // returns a map with the results from updating each node in the cluster
    template<class Type>
    bool broadcastCatalogDelete (Handle<Type> metadataToSend,
                                 map <string, pair<bool, string>> &broadcastResults,
                                 string &errMsg);

    // Returns true if a node is already registered in the Catalog
    bool isNodeRegistered(string nodeIP);

    // Returns true if a database is already registered in the Catalog
    bool isDatabaseRegistered(string dbName);

    // Returns true if a set for a given database is already registered in the Catalog
    bool isSetRegistered(string dbName, string setName);

	// Returns a reference to the underlying class that manages catalog metadata
    // the metadata is stored in a SQLite database
	PDBCatalogPtr getCatalog();

private:
	// new catalog metadata containers
    //TODO some containers will be removed, here for testing purposes
    Handle<Vector <CatalogNodeMetadata> > _allNodesInCluster;
    Handle<Vector <CatalogSetMetadata> > _setTypes;
    Handle<Vector <CatalogDatabaseMetadata> > _allDatabases;
    Handle<Vector <CatalogUserTypeMetadata> > _udfsValues;

	// map from type name string to int, and vice/versa
	map <string, int16_t> allTypeNames;
	map <int16_t, string> allTypeCodes;

		
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

    bool isMasterCatalogServer;

    string masterIP = "localhost";
    int masterPort = 8108;
    PDBLoggerPtr catServerLogger;

};


}

#endif
