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

#include <cstddef>
#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

#include "CatalogServer.h"
#include "SimpleRequestHandler.h"
#include "BuiltInObjectTypeIDs.h"
#include "CatTypeSearchResult.h"
#include "CatSetObjectTypeRequest.h"
#include "CatRegisterType.h"
#include "CatTypeNameSearch.h"
#include "CatTypeSearchResult.h"
#include "CatSharedLibraryRequest.h"
#include "CatSharedLibraryResult.h"
#include "CatCreateDatabaseRequest.h"
#include "SimpleRequestResult.h"
#include "CatCreateSetRequest.h"


namespace pdb {

int16_t CatalogServer :: searchForObjectTypeName (std :: string objectTypeName) {

	std :: cout << "searching for " << objectTypeName << "\n";
	// return a -1 if we've never seen this type name
	if (allTypeNames.count (objectTypeName) == 0) {
		std :: cout << "Could not find it.\n";
		return -1;
	}

	std :: cout << "Found it: " << to_string (allTypeNames[objectTypeName]) << "\n";
	return allTypeNames[objectTypeName];
}


void CatalogServer :: registerHandlers (PDBServer &forMe) {

	// handle a request for an object type name search
	forMe.registerHandler (CatTypeNameSearch_TYPEID, make_shared <SimpleRequestHandler <CatTypeNameSearch>> (
		[&] (Handle <CatTypeNameSearch> request, PDBCommunicatorPtr sendUsingMe) {

			// ask the catalog serer for the type ID 
			std :: cout << "Asking about " << request->getObjectTypeName () << "\n";
			int16_t typeID = getFunctionality <CatalogServer> ().searchForObjectTypeName (request->getObjectTypeName ());
			Handle <CatTypeSearchResult> response = makeObjectOnTempAllocatorBlock <CatTypeSearchResult> (1024, typeID);				

			// return the result
			std :: string errMsg;
			bool res = sendUsingMe->sendObject (response, errMsg);
			return make_pair (res, errMsg);
		}
	));

	// handle a request to obtain a copy of a shared library
	forMe.registerHandler (CatSharedLibraryRequest_TYPEID, make_shared <SimpleRequestHandler <CatSharedLibraryRequest>> (
		[&] (Handle <CatSharedLibraryRequest> request, PDBCommunicatorPtr sendUsingMe) {

			// ask the catalog serer for the shared library
			vector <char> putResultHere;
			std :: string errMsg;
			int16_t typeID = request->getTypeID ();
			bool res = getFunctionality <CatalogServer> ().getSharedLibrary (typeID, putResultHere, errMsg);
			Handle <CatSharedLibraryResult> response = makeObjectOnTempAllocatorBlock <CatSharedLibraryResult> (1024, res, errMsg);				

			// return the result
			res = sendUsingMe->sendBytes (response, putResultHere.data (), putResultHere.size (), errMsg);
			return make_pair (res, errMsg);
		}
	));
	
	// handle a request to get the string corresponding to the name of an object type
	forMe.registerHandler (CatSetObjectTypeRequest_TYPEID, make_shared <SimpleRequestHandler <CatSetObjectTypeRequest>> (
		[&] (Handle <CatSetObjectTypeRequest> request, PDBCommunicatorPtr sendUsingMe) {

			// ask the catalog server for the type ID and then the name of the type
			int16_t typeID = getFunctionality <CatalogServer> ().getObjectType (request->getDatabaseName (), request->getSetName ());
			Handle <CatTypeSearchResult> response = makeObjectOnTempAllocatorBlock <CatTypeSearchResult> (1024, typeID);				

			// return the result
			std :: string errMsg;
			bool res = sendUsingMe->sendObject (response, errMsg);
			return make_pair (res, errMsg);
		}
	));

	// handle a request to create a database 
	forMe.registerHandler (CatCreateDatabaseRequest_TYPEID, make_shared <SimpleRequestHandler <CatCreateDatabaseRequest>> (
		[&] (Handle <CatCreateDatabaseRequest> request, PDBCommunicatorPtr sendUsingMe) {

			// ask the catalog server for the type ID and then the name of the type
			std :: string errMsg;
			bool res = getFunctionality <CatalogServer> ().addDatabase (request->dbToCreate (), errMsg);

			Handle <SimpleRequestResult> response = makeObjectOnTempAllocatorBlock <SimpleRequestResult> (1024, res, errMsg);				

			// return the result
			res = sendUsingMe->sendObject (response, errMsg);
			return make_pair (res, errMsg);
		}
	));
	
	forMe.registerHandler (CatCreateSetRequest_TYPEID, make_shared <SimpleRequestHandler <CatCreateSetRequest>> (
		[&] (Handle <CatCreateSetRequest> request, PDBCommunicatorPtr sendUsingMe) {

			// ask the catalog server for the type ID and then the name of the type
			std :: string errMsg;
			auto info = request->whichSet ();
			bool res = getFunctionality <CatalogServer> ().addSet (request->whichType (), info.first, info.second, errMsg);

			Handle <SimpleRequestResult> response = makeObjectOnTempAllocatorBlock <SimpleRequestResult> (1024, res, errMsg);				

			// return the result
			res = sendUsingMe->sendObject (response, errMsg);
			return make_pair (res, errMsg);
		}
	));

	// handles a request to register a shared library
	forMe.registerHandler (CatRegisterType_TYPEID, make_shared <SimpleRequestHandler <CatRegisterType>> (
		[&] (Handle <CatRegisterType> request, PDBCommunicatorPtr sendUsingMe) {

			// write out the file
			vector <char> soFile;
			size_t fileLen = request->getSoFile ().size ();
			soFile.resize (fileLen);
			memmove (soFile.data (), request->getSoFile ().c_ptr (), fileLen);
			std :: string errMsg;
			bool res = (addObjectType (soFile, errMsg) >= 0);

			// create the response
			Handle <SimpleRequestResult> response = makeObjectOnTempAllocatorBlock <SimpleRequestResult> (1024, res, errMsg);				
			// return the result
			res = sendUsingMe->sendObject (response, errMsg);
			return make_pair (res, errMsg);
		}
	));
			
}

std :: string CatalogServer :: searchForObjectTypeName (int16_t typeIdentifier) {

	// return a -1 if we've never seen this type name
	if (allTypeCodes.count (typeIdentifier) == 0)
		return "";

	return allTypeCodes[typeIdentifier];
}


bool CatalogServer :: getSharedLibrary (int16_t identifier, vector <char> &putResultHere, std :: string &errMsg) {

	// first, make sure we have this identifier
	if (allTypeCodes.count (identifier) == 0) {
		errMsg = "Error: didn't know the identifier you sent me";
		return false;
	}

	// now, read in the .so file, and put it in the vector
	std :: string whichFile = catalogDirectory + "/" + allTypeCodes[identifier] + ".so";
	std :: ifstream in (whichFile, std::ifstream::ate | std::ifstream::binary);
	size_t fileLen = in.tellg();

	int filedesc = open (whichFile.c_str (), O_RDONLY);
	putResultHere.resize (fileLen);
	read (filedesc, putResultHere.data (), fileLen);
	close (filedesc);

	return true;
}

int16_t CatalogServer :: getObjectType (std :: string databaseName, std :: string setName) {
	
	if (setTypes.count (make_pair (databaseName, setName)) == 0)
		return -1;

	return setTypes[make_pair (databaseName, setName)];	
}

int16_t CatalogServer :: addObjectType (vector <char> &soFile, string &errMsg) {

	// and add the new .so file
	string tempFile = catalogDirectory + "/temp.so";
	int filedesc = open (tempFile.c_str (), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
	write (filedesc, soFile.data (), soFile.size ());
	close (filedesc);	

	// check to make sure it is valid
	void *so_handle = nullptr;
	so_handle = dlopen (tempFile.c_str (), RTLD_LOCAL | RTLD_LAZY );
	if (!so_handle) {
		const char* dlsym_error = dlerror();
		dlclose (so_handle);
		errMsg = "Cannot process shared library. " + string (dlsym_error) + '\n';
		return -1;
	}

	const char* dlsym_error;
	std::string getName = "getObjectTypeName";	
	typedef char *getObjectTypeNameFunc ();
	getObjectTypeNameFunc *myFunc = (getObjectTypeNameFunc *) dlsym(so_handle, getName.c_str());

	if ((dlsym_error = dlerror())) {
		errMsg = "Error, can't load function getObjectTypeName in the shared library. " + string(dlsym_error) + '\n';
		return -1;
	}

	// now, get the type name and write the appropriate file
	string typeName (myFunc ());
	dlclose (so_handle);
	filedesc = open ((catalogDirectory + "/" + typeName + ".so").c_str (), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
	write (filedesc, soFile.data (), soFile.size ());
	close (filedesc);	

	// add the new type name
	int16_t typeCode = 8192 + allTypeNames.size ();
	allTypeNames [typeName] = typeCode;
	allTypeCodes [typeCode] = typeName;

	// and update the catalog file
	vector <string> typeNames;
	vector <int> typeCodes;

	// get the two vectors to add
	for (auto &pair : allTypeNames) {
		typeNames.push_back (pair.first);
		typeCodes.push_back (pair.second);
	}

	// and add them
	myCatalog->putStringList ("typeNames", typeNames);
	myCatalog->putIntList ("typeCodes", typeCodes);
	myCatalog->save ();
	return typeCode;
}

bool CatalogServer :: addSet (int16_t typeIdentifier, string databaseName, std :: string setName, std :: string &errMsg) {

	// make sure we are only adding to an existing database
	if (allDatabases.count (databaseName) == 0) {
		errMsg = "Database does not exist.\n";
		return false;	
	}

	vector <string> &setList = allDatabases[databaseName];

	// make sure that set does not exist
	for (string s : setList) {
		if (s == setName) {
			errMsg = "Set already exists.\n";
			return false;	
		}
	}

	// make sure that type code exists
	if (allTypeCodes.count (typeIdentifier) == 0) {
		errMsg = "Type code does not exist.\n";
		return false;
	}

	// add the set
	setList.push_back (setName);
	myCatalog->putStringList (databaseName + ".sets", setList);

	// and add the set's type
	setTypes [make_pair (databaseName, setName)] = typeIdentifier;
	myCatalog->putInt (databaseName + "." + setName + ".code", typeIdentifier);
	myCatalog->save ();
	return true;
}


bool CatalogServer :: addDatabase (std :: string databaseName, std :: string &errMsg) {

	// don't add a database that is alredy there
	if (allDatabases.count (databaseName) != 0) {
		errMsg = "Database name already exists.\n";
		return false;	
	}

	// add the database
	vector <string> empty;
	allDatabases [databaseName] = empty;

	// and update the catalog... add the database name
	vector <string> databaseNames;
	databaseNames.push_back (databaseName);
	for (auto &entry : allDatabases) {
		databaseNames.push_back (entry.first);
	}	
	myCatalog->putStringList ("databaseNames", databaseNames);
	myCatalog->putStringList (databaseName + ".sets", empty); 
	myCatalog->save ();
	return true;
}


CatalogServer :: CatalogServer (std :: string catalogDirectoryIn) {

	catalogDirectory = catalogDirectoryIn;

	myCatalog = make_shared <MyDB_Catalog> (catalogDirectory + "/catalog");

	// first, get the list of type names and type codes
	vector <string> typeNames;
	if (myCatalog->getStringList ("typeNames", typeNames)) {

		vector <int> typeCodes;
		myCatalog->getIntList ("typeCodes", typeCodes);

		for (int i = 0; i < typeCodes.size (); i++) {
			allTypeNames[typeNames[i]] = typeCodes[i]; 			
			allTypeCodes[typeCodes[i]] = typeNames[i]; 			
		}	
	}

	// get the list of databases
	vector <string> databaseNames;
	if (myCatalog->getStringList ("databaseNames", databaseNames)) {
		
		// get the sets in this database
		vector <string> setNames;
		for (string s : databaseNames) {
			myCatalog->getStringList (s + ".sets", setNames);
			allDatabases [s] = setNames;

			// for each set, record the type code
			int myCode;
			for (string setName : setNames) {
				myCatalog->getInt (s + "." + setName + ".code", myCode);
				setTypes [make_pair (s, setName)] = myCode;
			}
		}
	}
}

}
