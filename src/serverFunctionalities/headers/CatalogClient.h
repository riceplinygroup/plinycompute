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

#ifndef CATALOG_CLIENT_H
#define CATALOG_CLIENT_H

namespace pdb {

class CatalogClient;

}

#include "ServerFunctionality.h"
#include "PDBLogger.h"
#include "PDBServer.h"
#include "CatalogDatabaseMetadata.h"
#include "CatalogPrintMetadata.h"

namespace pdb {

class CatalogClient : public ServerFunctionality {

public:

	// destructor
	~CatalogClient ();

	// these give us the port and the address of the catalog
	CatalogClient (int port, std :: string address, PDBLoggerPtr myLogger);

	// function to register event handlers associated with this server functionality
	virtual void registerHandlers (PDBServer &forMe) override;

	// this uses the name of the object to find the corresponding identifier
	int16_t searchForObjectTypeName (std :: string objectTypeName);

	// this downloads the shared library assoicated with the identifier, putting it at the specified location
	bool getSharedLibrary (int16_t identifier, std :: string objectFile);	

	// this registers a type with the catalog
	// returns true on success, false on fail
	bool registerType (std :: string fileContainingSharedLib, std :: string &errMsg);

        // shuts down the server that we are connected to... returns true on success
        bool shutDownServer (std :: string &errMsg);

	// this returns the type of object in the specified set, as a type name; returns "" on err
	std :: string getObjectType (std :: string databaseName, std :: string setName, std :: string &errMsg);

	// this creates a new database... returns true on success
	// returns true on success, false on fail
	bool createDatabase (std :: string databaseName, std :: string &errMsg);

	// registers metadata about a database
	bool registerDatabaseMetadata (std :: string databaseName, std :: string &errMsg);

    // registers metadata about a node in the cluster
    bool registerNodeMetadata (std :: string nodeIP, int nodePort, std :: string nodeName,
                               std :: string nodeType, int status, std :: string &errMsg);

	// this creates a new set in a given database... returns true on success
	// returns true on success, false on fail
	template <class DataType>
	bool createSet (std :: string databaseName, std :: string setName, std :: string &errMsg);

	// same as above, but here we use the type code
	bool createSet (int16_t identifier, std :: string databaseName, std :: string setName, std :: string &errMsg);

    bool printCatalogMetadata (std :: string databaseName, std :: string &errMsg);

private:

	int port;
	std :: string address;
	PDBLoggerPtr myLogger;

	// serialize access to shared library loading
	pthread_mutex_t workingMutex;
};

}

#include "CatalogClientTemplates.cc"

#endif
