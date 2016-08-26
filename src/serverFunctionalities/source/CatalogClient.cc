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
#include "SimpleRequestResult.h"
#include "CatTypeNameSearch.h"
#include "CatTypeSearchResult.h"
#include "CatSharedLibraryRequest.h"
#include "CatSharedLibraryResult.h"
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

bool CatalogClient :: registerType (std :: string fileContainingSharedLib) {
	
	// first, load up the shared library file
	// get the file size
	std::ifstream in (fileContainingSharedLib, std::ifstream::ate | std::ifstream::binary);
	size_t fileLen = in.tellg();
	
	// read in the file that we are supposed to send
	vector <char> putResultHere;
        int filedesc = open (fileContainingSharedLib.c_str (), O_RDONLY);
        putResultHere.resize (fileLen);
        read (filedesc, putResultHere.data (), fileLen);
        close (filedesc);

	return simpleRequest <CatRegisterType, SimpleRequestResult, bool> (myLogger, port, address, false, 
		1024 + 4 * fileLen, 
		[&] (Handle <SimpleRequestResult> result) {
			if (result != nullptr)
				if (!result->getRes ().first) {
					myLogger->error ("Error registering type: " + result->getRes ().second);
					return false;
				}
			return true;},
		putResultHere);
	
}

int16_t CatalogClient :: searchForObjectTypeName (string objectTypeName) {

	return simpleRequest <CatTypeNameSearch, CatTypeSearchResult, int16_t> (myLogger, port, address, false, 1024,
		[&] (Handle <CatTypeSearchResult> result) {
			if (result != nullptr)
				return result->getTypeID ();
			else
				return (int16_t) -1;},
		objectTypeName);
}

bool CatalogClient :: getSharedLibrary (int16_t identifier, string objectFile) {
	
	return simpleRequest <CatSharedLibraryRequest, CatSharedLibraryResult, bool> (myLogger, port, address, false, 1024,
		[&] (Handle <CatSharedLibraryResult> result) {
		
			if (result == nullptr)
				return false;

			auto success = result->getRes ();
			if (!success.first) {
				myLogger->error ("Error getting shared library: " + success.second);
				return false;
			}

			// just write the shared library to the file
			int filedesc = open (objectFile.c_str (), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);	
			write (filedesc, result->dataToSend.c_ptr (), result->dataToSend.size ());
			close (filedesc);
			return true;},
		identifier);
}

std :: string CatalogClient :: getObjectType (string databaseName, string setName) {

	return simpleRequest <CatSetObjectTypeRequest, CatTypeNameSearchResult, string> (myLogger, port, address, "", 1024,
		[&] (Handle <CatTypeNameSearchResult> result) {
			if (result != nullptr) {
				auto success = result->wasSuccessful ();
				if (!success.first) 
					myLogger->error ("Error getting type name: " + success.second);
				else 
					return result->getTypeName ();
			}
			return std :: string ("");},
		databaseName, setName);
}

bool CatalogClient :: createDatabase (string databaseName, string &errMsg) {

	return simpleRequest <CatCreateDatabaseRequest, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
		[&] (Handle <SimpleRequestResult> result) {
			if (result != nullptr)
				if (!result->getRes ().first) {
					myLogger->error ("Error creating database: " + result->getRes ().second);
					return false;
				}
			return true;},
		databaseName);
}

template <class DataType>
bool CatalogClient :: createSet (string databaseName, string setName, string &errMsg) {

	return simpleRequest <CatCreateSetRequest, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
		[&] (Handle <SimpleRequestResult> result) {
			if (result != nullptr)
				if (!result->getRes ().first) {
					myLogger->error ("Error creating set: " + result->getRes ().second);
					return false;
				}
			return true;},
		databaseName, setName, getTypeName <DataType> ());
		
}

}
#endif
