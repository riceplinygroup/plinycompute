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

#ifndef STORAGE_CLIENT_H
#define STORAGE_CLIENT_H

namespace pdb {

class StorageClient;

}

#include "ServerFunctionality.h"
#include "CatalogClient.h"
#include "PDBLogger.h"
#include "PDBServer.h"

namespace pdb {

class StorageClient : public ServerFunctionality {

public:

	// these give us the port and the address of the catalog
	StorageClient (int port, std :: string address, PDBLoggerPtr myLogger, bool usePangea = false);

	// function to register event handlers associated with this server functionality
	virtual void registerHandlers (PDBServer &forMe) override;

	// this registers a type with the system
	bool registerType (std :: string fileContainingSharedLib, std :: string &errMsg);

	// this returns the type of object in the specified set, as a type name... returns "" on error
	std :: string getObjectType (std :: string databaseName, std :: string setName, std :: string &errMsg);

	// this creates a new database... returns true on success
	bool createDatabase (std :: string databaseName, std :: string &errMsg);

	// shuts down the server that we are connected to... returns true on success
	bool shutDownServer (std :: string &errMsg);

	// this creates a new set in a given database... returns true on success
	template <class DataType>
	bool createSet (std :: string databaseName, std :: string setName, std :: string &errMsg);

	// this stores data into a set... returns true on success
	template <class DataType>
	bool storeData (Handle <Vector <Handle <DataType>>> data, std :: string databaseName, std :: string setName, std :: string &errMsg);

private:

	CatalogClient myHelper;

	int port;
	std :: string address;
	PDBLoggerPtr myLogger;

        bool usePangea;

};

}

#include "StorageClientTemplate.cc"

#endif
