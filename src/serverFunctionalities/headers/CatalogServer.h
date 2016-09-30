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
#include "MyDB_Catalog.h"
#include "PDBCatalog.h"

namespace pdb {

class CatalogServer : public ServerFunctionality {

public:

	// destructor
	~CatalogServer ();

	// these give us the port and the address of the catalog
	CatalogServer (std :: string catalogDirectory, bool usePangea=false);

	// from the ServerFunctionality interface
	void registerHandlers (PDBServer &forMe) override;

	// this uses the name of the object to find the corresponding identifier
	int16_t searchForObjectTypeName (string objectTypeName);

	// this uses the identerifer for the object to find the corresponding type name
	string searchForObjectTypeName (int16_t typeIdentifier);

	// return the number of pages in the given file
	size_t getNumPages (std :: string dbName, std :: string setName);

	// returns one greater than the current length in pages of the given set, incremening the value
	size_t getNewPage (std :: string dbName, std :: string setName);

	// this downloads the shared libreary assoicated with the identifier, putting it at the specified location
	bool getSharedLibrary (int16_t identifier, vector <char> &putResultHere, std :: string &errMsg);

	// this returns the type of object in the specified set, as a type name
	int16_t getObjectType (string databaseName, string setName);

	// this creates a new database... returns true on success
	bool addDatabase (string databaseName, string &errMsg);

	// deletes a set from the database
	bool deleteSet (std :: string databaseName, std :: string setName, std :: string &errMsg);

	// this creates a new set in a given database... returns true on success
	bool addSet (int16_t typeIdentifier, string databaseName, string setName, string &errMsg);
	
	// adds a new object type... return -1 on failure
	int16_t addObjectType (vector <char> &soFile, string &errMsg);
	
	// print the content of the catalog metadata
	string printCatalog (string item);

	// registers metadata about a node in the cluster
	bool addNodeMetadata (std :: string nodeIP, int port,
            std :: string nodeName, std :: string nodeType,
            int status, std :: string &errMsg);

private:

	// new catalog metadata containers
    //TODO new temp containers for metadata,
    Handle<Vector <CatalogNodeMetadata> > _allNodesInCluster = makeObject<Vector<CatalogNodeMetadata>>();
    Handle<Vector <CatalogSetMetadata> > _setTypes = makeObject<Vector<CatalogSetMetadata>>();;
    Handle<Vector <CatalogDatabaseMetadata> > _allDatabases = makeObject<Vector<CatalogDatabaseMetadata>>();
    Handle<Vector <CatalogUserTypeMetadata> > _udfsValues = makeObject<Vector<CatalogUserTypeMetadata>>();

    Handle<Vector<Handle<CatalogNodeMetadata>>> resultItemsNodes = makeObject<Vector<Handle<CatalogNodeMetadata>>>();
    Handle<Vector<Handle<CatalogSetMetadata>>> resultItemsSets = makeObject<Vector<Handle<CatalogSetMetadata>>>();
    Handle<Vector<Handle<CatalogDatabaseMetadata>>> resultItemsDBs = makeObject<Vector<Handle<CatalogDatabaseMetadata>>>();
    Handle<Vector<Handle<CatalogUserTypeMetadata>>> resultItemsUdfs = makeObject<Vector<Handle<CatalogUserTypeMetadata>>>();

    map<string, CatalogNodeMetadata> mapNodes;
    map<string, CatalogSetMetadata> mapSets;
    map<string, CatalogDatabaseMetadata> mapDBs;
    map<string, CatalogUserTypeMetadata> mapUdfs;


    // **end

	// map from type name string to int, and vice/versa
	map <string, int16_t> allTypeNames;
	map <int16_t, string> allTypeCodes;

	// map from database name to list of sets
	map <string, vector <string>> allDatabases; 
		
	// vector of nodes in the cluster
	vector <string> allNodesInCluster;

    // map from database/set pair to type of set
    map <pair <string, string>, int16_t> setTypes;

	// interface to a text file that allows us to save/retreive all of this stuff
	MyDB_CatalogPtr myCatalog;

    // interface to a persistent catalog storage for storing and retrieving PDB metadata
    PDBCatalogPtr pdbCatalog;

	// where the catalog is located
	std :: string catalogDirectory;

	// serialize access
	pthread_mutex_t workingMutex;

        //use Pangea Storage Server or not?
        bool usePangea;

};


}

#endif
