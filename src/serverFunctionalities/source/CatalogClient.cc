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

#ifndef CATALOG_CLIENT_CC
#define CATALOG_CLIENT_CC

#include <iostream>
#include <fstream> 
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "CatalogClient.h"
#include "CatRegisterType.h"
#include "SimpleSendDataRequest.h"
#include "SimpleRequestResult.h"
#include "CatTypeNameSearch.h"
#include "CatTypeSearchResult.h"
#include "ShutDown.h"
#include "CatSharedLibraryRequest.h"
#include "CatSetObjectTypeRequest.h"
#include "CatTypeNameSearchResult.h"
#include "CatCreateDatabaseRequest.h"
#include "CatCreateSetRequest.h"
#include "SimpleRequestResult.h"
#include "SimpleRequest.h"

namespace pdb {

CatalogClient :: CatalogClient (int portIn, std :: string addressIn, PDBLoggerPtr myLoggerIn) {

	// get the communication information
	port = portIn;
	address = addressIn;
	myLogger = myLoggerIn;

	// and let the v-table map know this information
	theVTable->setCatalogClient (this);
}

void CatalogClient :: registerHandlers (PDBServer &forMe) { /* no handlers for a catalog client!! */}

bool CatalogClient :: registerType (std :: string fileContainingSharedLib, std :: string &errMsg) {
	
	// first, load up the shared library file
	// get the file size
	std::ifstream in (fileContainingSharedLib, std::ifstream::ate | std::ifstream::binary);
	size_t fileLen = in.tellg();
	
	// this makes a an empty vector with fileLen slots
	const UseTemporaryAllocationBlock tempBlock{fileLen + 1024};
	bool res;
	{
	Handle <Vector <char>> putResultHere = makeObject <Vector <char>> (fileLen, fileLen);

	// read data into it
        int filedesc = open (fileContainingSharedLib.c_str (), O_RDONLY);
        read (filedesc, putResultHere->c_ptr (), fileLen);
        close (filedesc);

	res = simpleSendDataRequest <CatRegisterType, char, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
		[&] (Handle <SimpleRequestResult> result) {
			if (result != nullptr) {
				if (!result->getRes ().first) {
					errMsg = "Error registering type: " + result->getRes ().second;
					myLogger->error ("Error registering type: " + result->getRes ().second);
					return false;
				}
				return true;
			} else {
				errMsg = "Error registering type: got null pointer on return message.\n";
				myLogger->error ("Error registering type: got null pointer on return message.\n");
				return false;	
			}},
		putResultHere);
	}
	return res;
}

bool CatalogClient :: shutDownServer (std :: string &errMsg) {

	return simpleRequest <ShutDown, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
		[&] (Handle <SimpleRequestResult> result) {
			if (result != nullptr) {
				if (!result->getRes ().first) {
					errMsg = "Error shutting down server: " + result->getRes ().second;
					myLogger->error ("Error shutting down server: " + result->getRes ().second);
					return false;
				}
				return true;
			}
			errMsg = "Error getting type name: got nothing back from catalog";
			return false;});
}

int16_t CatalogClient :: searchForObjectTypeName (std :: string objectTypeName) {

	return simpleRequest <CatTypeNameSearch, CatTypeSearchResult, int16_t> (myLogger, port, address, false, 1024,
		[&] (Handle <CatTypeSearchResult> result) {
			if (result != nullptr)
				return result->getTypeID ();
			else
				return (int16_t) -1;},
		objectTypeName);
}

bool CatalogClient :: getSharedLibrary (int16_t identifier, std :: string objectFile) {
	
	return simpleRequest <CatSharedLibraryRequest, Vector <char>, bool> (myLogger, port, address, false, 1024,
		[&] (Handle <Vector <char>> result) {
		
			if (result == nullptr) {
				myLogger->error ("Error getting shared library: null object returned.\n");
				return false;
			}

			if (result->size () == 0) {
				myLogger->error ("Error getting shared library, no data returned.\n");
				return false;
			}

			// just write the shared library to the file
			int filedesc = open (objectFile.c_str (), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);	
			write (filedesc, result->c_ptr (), result->size ());
			close (filedesc);
			return true;},
		identifier);
}

std :: string CatalogClient :: getObjectType (std :: string databaseName, std :: string setName, std :: string &errMsg) {

	return simpleRequest <CatSetObjectTypeRequest, CatTypeNameSearchResult, std :: string> (myLogger, port, address, "", 1024,
		[&] (Handle <CatTypeNameSearchResult> result) {
			if (result != nullptr) {
				auto success = result->wasSuccessful ();
				if (!success.first) {
					errMsg = "Error getting type name: " + success.second;
					myLogger->error ("Error getting type name: " + success.second);
				} else 
					return result->getTypeName ();
			}
			errMsg = "Error getting type name: got nothing back from catalog";
			return std :: string ("");},
		databaseName, setName);
}

bool CatalogClient :: createDatabase (std :: string databaseName, std :: string &errMsg) {

	return simpleRequest <CatCreateDatabaseRequest, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
		[&] (Handle <SimpleRequestResult> result) {
			if (result != nullptr) {
				if (!result->getRes ().first) {
					errMsg = "Error creating database: " + result->getRes ().second;
					myLogger->error ("Error creating database: " + result->getRes ().second);
					return false;
				}
				return true;
			}
			errMsg = "Error getting type name: got nothing back from catalog";
			return false;},
		databaseName);
}

}
#endif
