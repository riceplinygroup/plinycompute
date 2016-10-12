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

#include "UseTemporaryAllocationBlock.h"
#include "CatalogServer.h"
#include "SimpleRequestHandler.h"
#include "BuiltInObjectTypeIDs.h"
#include "CatTypeNameSearchResult.h"
#include "CatTypeSearchResult.h"
#include "CatSetObjectTypeRequest.h"
#include "CatRegisterType.h"
#include "CatTypeNameSearch.h"
#include "CatTypeSearchResult.h"
#include "CatSharedLibraryRequest.h"
#include "CatCreateDatabaseRequest.h"
#include "SimpleRequestResult.h"
#include "CatCreateSetRequest.h"
#include "CatalogDatabaseMetadata.h"
#include "CatalogPrintMetadata.h"
#include "StorageServer.h"
#include "PangeaStorageServer.h"
namespace pdb {

int16_t CatalogServer :: searchForObjectTypeName (std :: string objectTypeName) {

	// first search for the type name in the vTable map (in case it is built in)
	if (VTableMap :: lookupBuiltInType (objectTypeName) != -1)
		return VTableMap :: lookupBuiltInType (objectTypeName);

	// return a -1 if we've never seen this type name
	if (allTypeNames.count (objectTypeName) == 0) {
		return -1;
	}

	return allTypeNames[objectTypeName];
}

void CatalogServer :: registerHandlers (PDBServer &forMe) {
    cout << "Catalog Server registering handlers" << endl;

    //TODO change this test
    // handle a request to add metadata for a new Database in the catalog
    forMe.registerHandler (CatalogNodeMetadata_TYPEID, make_shared <SimpleRequestHandler <CatalogNodeMetadata>> (
        [&] (Handle<CatalogNodeMetadata> request, PDBCommunicatorPtr sendUsingMe) {

            // ask the catalog server for the type ID and then the name of the type
            std :: string errMsg;

            cout << "--->Testing PDBCatalog register node: " << request->getNodeIP().c_str() << endl;

            bool res = getFunctionality <CatalogServer> ().addNodeMetadata (request, errMsg);
            // make the response
            const UseTemporaryAllocationBlock tempBlock{1024};
            Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

            // return the result
            res = sendUsingMe->sendObject (response, errMsg);
            return make_pair (res, errMsg);
        }
    ));

    // handle a request to add metadata for a new Database in the catalog
    forMe.registerHandler (CatalogDatabaseMetadata_TYPEID, make_shared <SimpleRequestHandler <CatalogDatabaseMetadata>> (
        [&] (Handle <CatalogDatabaseMetadata> request, PDBCommunicatorPtr sendUsingMe) {

            // ask the catalog server for the type ID and then the name of the type
            std :: string errMsg;
            bool res = getFunctionality <CatalogServer> ().addDatabaseMetadata (request, errMsg);
            cout << "--->Testing PDBCatalog register Database Metadata" << endl;

            // make the response
            const UseTemporaryAllocationBlock tempBlock{1024};
            Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

            // return the result
            res = sendUsingMe->sendObject (response, errMsg);
            return make_pair (res, errMsg);
        }
    ));

    // handle a request to add metadata for a new Set in the catalog
    forMe.registerHandler (CatalogSetMetadata_TYPEID, make_shared <SimpleRequestHandler <CatalogSetMetadata>> (
        [&] (Handle <CatalogSetMetadata> request, PDBCommunicatorPtr sendUsingMe) {

            // ask the catalog server for the type ID and then the name of the type
            std :: string errMsg;
            bool res = getFunctionality <CatalogServer> ().addSetMetadata (request, errMsg);
            cout << "--->Testing PDBCatalog register Set Metadata" << endl;

            // make the response
            const UseTemporaryAllocationBlock tempBlock{1024};
            Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

            // return the result
            res = sendUsingMe->sendObject (response, errMsg);
            return make_pair (res, errMsg);
        }
    ));

    // Handle to print metadata from the Catalog
    forMe.registerHandler (CatalogPrintMetadata_TYPEID, make_shared <SimpleRequestHandler <CatalogPrintMetadata>> (
        [&] (Handle <CatalogPrintMetadata> request, PDBCommunicatorPtr sendUsingMe) {

            // ask the catalog server for the type ID and then the name of the type
            std :: string errMsg;
            string item = request->getItemName();
            cout << "--->Testing CatalogPrintMetadata handler with item id " << item << endl;

            bool res = getFunctionality <CatalogServer> ().printCatalog (item);

//            printCatalog(item);

            // make the response
            const UseTemporaryAllocationBlock tempBlock{1024};
            Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

            // return the result
            res = sendUsingMe->sendObject (response, errMsg);
            return make_pair (res, errMsg);
        }
    ));

	// handle a request for an object type name search
	forMe.registerHandler (CatTypeNameSearch_TYPEID, make_shared <SimpleRequestHandler <CatTypeNameSearch>> (
		[&] (Handle <CatTypeNameSearch> request, PDBCommunicatorPtr sendUsingMe) {

			// in practice, we can do better than simply locking the whole catalog, but good enough for now...
			const LockGuard guard{workingMutex};

			// ask the catalog serer for the type ID 
			int16_t typeID = getFunctionality <CatalogServer> ().searchForObjectTypeName (request->getObjectTypeName ());

			// make the result
			const UseTemporaryAllocationBlock tempBlock{1024};
			Handle <CatTypeSearchResult> response = makeObject <CatTypeSearchResult> (typeID);				

			// return the result
			std :: string errMsg;
			bool res = sendUsingMe->sendObject (response, errMsg);
			return make_pair (res, errMsg);
		}
	));

	// handle a request to obtain a copy of a shared library
	forMe.registerHandler (CatSharedLibraryRequest_TYPEID, make_shared <SimpleRequestHandler <CatSharedLibraryRequest>> (
		[&] (Handle <CatSharedLibraryRequest> request, PDBCommunicatorPtr sendUsingMe) {

			// in practice, we can do better than simply locking the whole catalog, but good enough for now...
			const LockGuard guard{workingMutex};

			// ask the catalog serer for the shared library
                        // added by Jia to test a length error bug
			vector <char> * putResultHere = new vector<char>();
			std :: string errMsg;
			int16_t typeID = request->getTypeID ();
                        //std :: cout << "CatalogServer to handle CatSharedLibraryRequest to get shared library for typeID=" << typeID << std :: endl;
			bool res = getFunctionality <CatalogServer> ().getSharedLibrary (typeID, (*putResultHere), errMsg);

			if (!res) {
				const UseTemporaryAllocationBlock tempBlock{1024};
				Handle <Vector <char>> response = makeObject <Vector <char>> ();
				res = sendUsingMe->sendObject (response, errMsg);
			} else {

				// in this case, we need a big space to put the object!!
				const UseTemporaryAllocationBlock temp{1024 + (*putResultHere).size ()};
 				Handle <Vector <char>> response = makeObject <Vector <char>> ((*putResultHere).size (), (*putResultHere).size ()); 
				memmove (response->c_ptr (), (*putResultHere).data (), (*putResultHere).size ());
				res = sendUsingMe->sendObject (response, errMsg);
			}
                        delete putResultHere;
			// return the result
			return make_pair (res, errMsg);
		}
	));
	
	// handle a request to get the string corresponding to the name of an object type
	forMe.registerHandler (CatSetObjectTypeRequest_TYPEID, make_shared <SimpleRequestHandler <CatSetObjectTypeRequest>> (
		[&] (Handle <CatSetObjectTypeRequest> request, PDBCommunicatorPtr sendUsingMe) {

			// in practice, we can do better than simply locking the whole catalog, but good enough for now...
			const LockGuard guard{workingMutex};

			// ask the catalog server for the type ID and then the name of the type
			int16_t typeID = getFunctionality <CatalogServer> ().getObjectType (request->getDatabaseName (), request->getSetName ());

			// make the response
			const UseTemporaryAllocationBlock tempBlock{1024};
			Handle <CatTypeNameSearchResult> response;
			if (typeID >= 0) 
				response = makeObject <CatTypeNameSearchResult> (searchForObjectTypeName (typeID), true, "success");
			else 
				response = makeObject <CatTypeNameSearchResult> ("", false, "could not find requested type");

			// return the result
			std :: string errMsg;
			bool res = sendUsingMe->sendObject (response, errMsg);
			return make_pair (res, errMsg);
		}
	));

	// handle a request to create a database 
	forMe.registerHandler (CatCreateDatabaseRequest_TYPEID, make_shared <SimpleRequestHandler <CatCreateDatabaseRequest>> (
		[&] (Handle <CatCreateDatabaseRequest> request, PDBCommunicatorPtr sendUsingMe) {

			// in practice, we can do better than simply locking the whole catalog, but good enough for now...
			const LockGuard guard{workingMutex};

			// ask the catalog server for the type ID and then the name of the type
			std :: string errMsg;
			bool res = getFunctionality <CatalogServer> ().addDatabase (request->dbToCreate (), errMsg);

			// make the response
			const UseTemporaryAllocationBlock tempBlock{1024};
			Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);				

			// return the result
			res = sendUsingMe->sendObject (response, errMsg);
			return make_pair (res, errMsg);
		}
	));
	
	forMe.registerHandler (CatCreateSetRequest_TYPEID, make_shared <SimpleRequestHandler <CatCreateSetRequest>> (
		[&] (Handle <CatCreateSetRequest> request, PDBCommunicatorPtr sendUsingMe) {

			// in practice, we can do better than simply locking the whole catalog, but good enough for now...
			const LockGuard guard{workingMutex};

			// ask the catalog server for the type ID and then the name of the type
			std :: string errMsg;
			auto info = request->whichSet ();
			bool res = getFunctionality <CatalogServer> ().addSet (request->whichType (), info.first, info.second, errMsg);

			// make the response
			const UseTemporaryAllocationBlock tempBlock{1024};
			Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);				

			// return the result
			res = sendUsingMe->sendObject (response, errMsg);
			return make_pair (res, errMsg);
		}
	));

	// handles a request to register a shared library
	forMe.registerHandler (CatRegisterType_TYPEID, make_shared <SimpleRequestHandler <CatRegisterType>> (
		[&] (Handle <CatRegisterType> request, PDBCommunicatorPtr sendUsingMe) {

			// in practice, we can do better than simply locking the whole catalog, but good enough for now...
			const LockGuard guard{workingMutex};

			// get the next object... this holds the shared library file... it could be big, so be careful!!
			size_t objectSize = sendUsingMe->getSizeOfNextObject ();

			bool res;
			std :: string errMsg;
			void *memory = malloc (objectSize);
			Handle <Vector <char>> myFile = sendUsingMe->getNextObject <Vector <char>> (memory, res, errMsg);
			if (res) {
				vector <char> soFile;
				size_t fileLen = myFile->size ();
				soFile.resize (fileLen);
				memmove (soFile.data (), myFile->c_ptr (), fileLen);
				res = (addObjectType (soFile, errMsg) >= 0);
			}
			free (memory);

			// create the response
			const UseTemporaryAllocationBlock tempBlock{1024};
			Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);				

			// return the result
			res = sendUsingMe->sendObject (response, errMsg);
			return make_pair (res, errMsg);
		}
	));
			
}

std :: string CatalogServer :: searchForObjectTypeName (int16_t typeIdentifier) {

	// first search for the type name in the vTable map (in case it is built in)
	std :: string result = VTableMap :: lookupBuiltInType (typeIdentifier);
	if (result != "")
		return result;

	// return a -1 if we've never seen this type name
	if (allTypeCodes.count (typeIdentifier) == 0)
		return "";

	// return a non built-in type
	return allTypeCodes[typeIdentifier];
}

size_t CatalogServer :: getNumPages (std :: string dbName, std :: string setName) { 
//	int numPages;
//	if (!myCatalog->getInt (dbName + "." + setName + ".fileSize", numPages)) {
//		return -1;
//	} else {
//		return numPages;
//	}
	return 0;
}

size_t CatalogServer :: getNewPage (std :: string dbName, std :: string setName) {
//	int numPages;
//	if (!myCatalog->getInt (dbName + "." + setName + ".fileSize", numPages)) {
//		myCatalog->putInt (dbName + "." + setName + ".fileSize", 1);
//		myCatalog->save ();
//		return 0;
//	} else {
//		numPages++;
//		myCatalog->putInt (dbName + "." + setName + ".fileSize", numPages);
//		myCatalog->save ();
//		return numPages - 1;
//	}
    return 0;
}

bool CatalogServer :: getSharedLibrary (int16_t identifier, vector <char> &putResultHere, std :: string &errMsg) {

        //std :: cout << "CatalogServer getSharedLibrary: typeId=" << identifier << std :: endl;
	// first, make sure we have this identifier
	if (allTypeCodes.count (identifier) == 0) {
		errMsg = "Error: didn't know the identifier you sent me";
		return false;
	}

	// now, read in the .so file, and put it in the vector
	std :: string whichFile = catalogDirectory + "/" + allTypeCodes[identifier] + ".so";
        //std :: cout << "to fetch file:" << whichFile << std :: endl;
	std :: ifstream in (whichFile, std::ifstream::ate | std::ifstream::binary);
	size_t fileLen = in.tellg();
        struct stat st;
        stat(whichFile.c_str (), &st);
        fileLen = st.st_size;
	int filedesc = open (whichFile.c_str (), O_RDONLY);
        //std :: cout << "CatalogServer getSharedLibrary: fileLen=" << fileLen << std :: endl;
	putResultHere.resize (fileLen);
	read (filedesc, putResultHere.data (), fileLen);
	close (filedesc);

	return true;
}

int16_t CatalogServer :: getObjectType (std :: string databaseName, std :: string setName) {
	cout << "getObjectType make_pair " << databaseName << " " << setName << endl;
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

   cout << "getObjectTypeName= " << getName << endl;
	if ((dlsym_error = dlerror())) {
		errMsg = "Error, can't load function getObjectTypeName in the shared library. " + string(dlsym_error) + '\n';
		cout << errMsg << endl;
		return -1;
	}
    cout << "all ok" << endl;

	// now, get the type name and write the appropriate file
	string typeName (myFunc ());
	dlclose (so_handle);
	filedesc = open ((catalogDirectory + "/" + typeName + ".so").c_str (), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
	write (filedesc, soFile.data (), soFile.size ());
	close (filedesc);	

	// add the new type name, if we don't already have it
	if (allTypeNames.count (typeName) == 0) {
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
//	myCatalog->putStringList ("typeNames", typeNames);
//	myCatalog->putIntList ("typeCodes", typeCodes);
//	myCatalog->save ();

	cout << "before creating object" << endl;
    //allocates 128Mb to register .so libraries
    makeObjectAllocatorBlock (1024 * 1024 * 128, true);

	Handle<CatalogUserTypeMetadata> objectMetadata = makeObject<CatalogUserTypeMetadata>();
    cout << "before calling " << endl;

	pdbCatalog->registerUserDefinedObject(objectMetadata, std::string(soFile.begin(), soFile.end()), typeName, catalogDirectory + "/" + typeName + ".so", "data_types", errMsg);

	return typeCode;
	} else 
		return allTypeNames [typeName];
}

bool CatalogServer :: deleteSet (std :: string databaseName, std :: string setName, std :: string &errMsg) {

    // allocate memory temporarily
    // TODO change this later
    makeObjectAllocatorBlock (1024 * 1024 * 128, true);

	if (allDatabases.count (databaseName) == 0) {
		errMsg = "Database does not exist.\n";
		return false;	
	}
	
	// delete the set from the map of sets
	bool foundIt = false;
	vector <string> &setList = allDatabases[databaseName];
	for (int i = 0; i < setList.size (); i++) {
		if (setList[i] == setName) {
			setList.erase (setList.begin () + i);
			foundIt = true;
			break;
        }
	}

	if (!foundIt) {
		errMsg = "Database does not exist in set.\n";
		return false;	
	}

	// write back the changed list
//	myCatalog->putStringList (databaseName + ".sets", setList);
//
//	// delete the type code info
//	myCatalog->deleteKey (databaseName + "." + setName + ".code");
//	myCatalog->deleteKey (databaseName + "." + setName + ".fileSize");
//	myCatalog->save ();
    int catalogType = PDBCatalogMsgType::CatalogPDBSet;
    Handle<CatalogSetMetadata> metadataObject = makeObject<CatalogSetMetadata>();

    // creates Strings
    String setKeyCatalog = String(databaseName + "." + setName);
    String setNameCatalog = String(setName);
    String dbName(databaseName);

    // populates object metadata
    metadataObject->setItemKey(setKeyCatalog);
    metadataObject->setItemName(setNameCatalog);
    metadataObject->setDBName(dbName);

    // deletes metadata in sqlite
	pdbCatalog->deleteMetadataInCatalog( metadataObject, catalogType, errMsg);

    // prepares object to update database entry in sqlite
    catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
    Handle<CatalogDatabaseMetadata> dbMetadataObject = makeObject<CatalogDatabaseMetadata>();

    Handle<Vector<Handle<CatalogDatabaseMetadata>>> resultItems = makeObject<Vector<Handle<CatalogDatabaseMetadata>>>();
    Handle<Vector<CatalogDatabaseMetadata>> vectorResultItems = makeObject<Vector<CatalogDatabaseMetadata>>();
    map<string, CatalogDatabaseMetadata> mapRes;

    if(pdbCatalog->getMetadataFromCatalog(false, databaseName,resultItems,vectorResultItems,mapRes,errMsg,catalogType) == false)
        cout << errMsg<< endl;

    for (int i=0; i < (*vectorResultItems).size(); i++){
        if ((*vectorResultItems)[i].getItemKey().c_str()==databaseName) *dbMetadataObject = (*vectorResultItems)[i];
    }

    (*dbMetadataObject).deleteSet(setNameCatalog);
//    cout << "\n\nPrint modified metadata: " << (*dbMetadataObject).printShort() << endl;

    // updates the corresponding database metadata
    if (pdbCatalog->updateMetadataInCatalog(dbMetadataObject, catalogType, errMsg)==true) cout << "All is ok" << endl;
    else{
        cout << "Error: " << errMsg << endl;
        return false;
    }

    // after it deletes the set metadata in the local catalog, iterate over all nodes,
    // make connections and broadcast the objects
    if (isMasterCatalogServer){
        cout << "About to broadcast set registration to nodes in the cluster: " << endl;

        // get the results of each broadcast
        map<string, pair <bool, string>> updateResults;
        errMsg = "";
//        if (broadcastCatalogUpdate (metadataObject, updateResults, errMsg)){
//            cout << " Broadcasting was Ok. " << endl;
//        } else {
//            cout << " Error broadcasting." << endl;
//        }
        for (auto &item : updateResults){
            cout << "Node IP: " << item.first << ((item.second.first == true) ? "updated correctly!" : "couldn't be updated due to error: ")
                 << item.second.second << endl;
        }
    }
    else{
        cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
    }

	// delete the file from the storage
        if (usePangea == false) {
        	if (!getFunctionality <StorageServer> ().deleteSet (std :: make_pair (databaseName, setName))) {
	        	errMsg = "Deleted set from catalog, but problem deleting from storage server.\n";
         		return false;	
        	}
        } else {
              if (!getFunctionality <PangeaStorageServer> ().removeSet (databaseName, setName)) {
                        errMsg = "Deleted set from catalog, but problem deleting from storage server.\n";
                        return false;
              }
        }

	return true;
}

bool CatalogServer :: addSet (int16_t typeIdentifier, std :: string databaseName, std :: string setName, std :: string &errMsg) {

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

	// make sure that type code exists, if we get one that is not built in
	if (typeIdentifier >= 8192 && allTypeCodes.count (typeIdentifier) == 0) {
		errMsg = "Type code does not exist.\n";
		return false;
	}

	// add the set
	setList.push_back (setName);
//	myCatalog->putStringList (databaseName + ".sets", setList);
//
//	// and add the set's type
	setTypes [make_pair (databaseName, setName)] = typeIdentifier;
//	myCatalog->putInt (databaseName + "." + setName + ".code", typeIdentifier);
//	myCatalog->save ();

    //TODO this might change depending on what metadata
    int catalogType = PDBCatalogMsgType::CatalogPDBSet;
    Handle<CatalogSetMetadata> metadataObject = makeObject<CatalogSetMetadata>();

    // creates Strings
    String setKeyCatalog = String(databaseName + "." + setName);
    String setNameCatalog = String(setName);
    String dbName(databaseName);
    String typeName(allTypeCodes[typeIdentifier]);

    // populates object metadata
    metadataObject->setItemKey(setKeyCatalog);
    metadataObject->setItemName(setNameCatalog);
    metadataObject->setDBName(dbName);
    metadataObject->setTypeName(typeName);

    // stores metadata in sqlite
    pdbCatalog->addMetadataToCatalog(metadataObject, catalogType, errMsg);

    // prepares data for the DB metadata
    catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
    Handle<CatalogDatabaseMetadata> dbMetadataObject = makeObject<CatalogDatabaseMetadata>();

    Handle<Vector<Handle<CatalogDatabaseMetadata>>> resultItems = makeObject<Vector<Handle<CatalogDatabaseMetadata>>>();
    Handle<Vector<CatalogDatabaseMetadata>> vectorResultItems = makeObject<Vector<CatalogDatabaseMetadata>>();
    map<string, CatalogDatabaseMetadata> mapRes;

    if(pdbCatalog->getMetadataFromCatalog(false, databaseName,resultItems,vectorResultItems,mapRes,errMsg,catalogType) == false)
        cout << errMsg<< endl;

    for (int i=0; i < (*vectorResultItems).size(); i++){
        if ((*vectorResultItems)[i].getItemKey().c_str()==databaseName) *dbMetadataObject = (*vectorResultItems)[i];
    }

//    (*dbMetadataObject).addSet(setKeyCatalog);
    (*dbMetadataObject).addSet(setNameCatalog);
    (*dbMetadataObject).addType(typeName);

    // updates the corresponding database metadata
    if (pdbCatalog->updateMetadataInCatalog(dbMetadataObject, catalogType, errMsg)==true) cout << "All is ok" << endl;
    else{
        cout << "Error: " << errMsg << endl;
        return false;
    }

    // after it registered the database metadata in the local catalog, iterate over all nodes,
    // make connections and broadcast the objects
    if (isMasterCatalogServer){
        cout << "About to broadcast set registration to nodes in the cluster: " << endl;

        // get the results of each broadcast
        map<string, pair <bool, string>> updateResults;
        errMsg = "";
        if (broadcastCatalogUpdate (metadataObject, updateResults, errMsg)){
            cout << " Broadcasting was Ok. " << endl;
        } else {
            cout << " Error broadcasting." << endl;
        }
        for (auto &item : updateResults){
            cout << "Node IP: " << item.first << ((item.second.first == true) ? "updated correctly!" : "couldn't be updated due to error: ")
                 << item.second.second << endl;
        }
    }
    else{
        cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
    }

    // TODO, remove it, just used for debugging
//    printCatalog("");

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
//	myCatalog->putStringList ("databaseNames", databaseNames);
//	myCatalog->putStringList (databaseName + ".sets", empty);
//	myCatalog->save ();

    //allocates 24Mb to process metadata info
    makeObjectAllocatorBlock (1024 * 1024 * 24, true);

	//TODO this might change depending on what metadata
	int catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
	Handle<CatalogDatabaseMetadata> metadataObject = makeObject<CatalogDatabaseMetadata>();
	String dbName = String(databaseName);
	metadataObject->setItemName(dbName);

    // stores metadata in sqlite
	pdbCatalog->addMetadataToCatalog(metadataObject, catalogType, errMsg);

	// adds metadata in memory
	mapDBs.insert(make_pair(databaseName,*metadataObject));

    // after it registered the database metadata in the local catalog, iterate over all nodes,
    // make connections and broadcast the objects
    if (isMasterCatalogServer){
        cout << "About to broadcast database registration to nodes in the cluster: " << endl;

        // get the results of each broadcast
        map<string, pair <bool, string>> updateResults;
        errMsg = "";
        if (broadcastCatalogUpdate (metadataObject, updateResults, errMsg)){
            cout << " Broadcasting was Ok. " << endl;
        } else {
            cout << " Error broadcasting." << endl;
        }
        for (auto &item : updateResults){
            cout << "Node IP: " << item.first << ((item.second.first == true) ? "updated correctly!" : "couldn't be updated due to error: ")
                 << item.second.second << endl;
        }
    }
    else{
        cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
    }
	//TODO
	return true;
}

CatalogServer :: ~CatalogServer () {
	pthread_mutex_destroy(&workingMutex);
}

CatalogServer :: CatalogServer (std :: string catalogDirectoryIn, bool usePangea, bool isMasterCatalogServer) {

    // allocates 64Mb for Catalog related metadata
    //TODO some of these containers will be removed, here just for testing
    pdb::makeObjectAllocatorBlock (1024 * 1024 * 64, true);
    _allNodesInCluster = makeObject<Vector<CatalogNodeMetadata>>();
    _setTypes = makeObject<Vector<CatalogSetMetadata>>();;
    _allDatabases = makeObject<Vector<CatalogDatabaseMetadata>>();
    _udfsValues = makeObject<Vector<CatalogUserTypeMetadata>>();

    resultItemsNodes = makeObject<Vector<Handle<CatalogNodeMetadata>>>();
    resultItemsSets = makeObject<Vector<Handle<CatalogSetMetadata>>>();
    resultItemsDBs = makeObject<Vector<Handle<CatalogDatabaseMetadata>>>();
    resultItemsUdfs = makeObject<Vector<Handle<CatalogUserTypeMetadata>>>();


    // by default this is a non-master catalog server, otherwise call the setIsMasterCatalogServer()
    // method after an instance has been created in order to change this flag.

    this->isMasterCatalogServer = isMasterCatalogServer;

	catalogDirectory = catalogDirectoryIn;
    this->usePangea = usePangea;
    cout << "Catalog Server ctor is Master Catalog= " << this->isMasterCatalogServer << endl;
    cout << "Catalog Server ctor uses Pangea= " << this->usePangea << endl;

    PDBLoggerPtr catalogLogger = make_shared<PDBLogger>("catalogLogger");

	// creates instance of catalog
    pdbCatalog = make_shared <PDBCatalog> (catalogLogger, catalogDirectory + "/pdbCatalog");
    // retrieves catalog from an sqlite db and loads metadata into memory
    pdbCatalog->open();

    //TODO adde indivitual mutexes
	// set up the mutex
	pthread_mutex_init(&workingMutex, nullptr);

	cout << "Loading catalog metadata." << endl;
    string errMsg;

    string emptyString("");
    // retrieves metadata for user-defined types from sqlite storage and loads them into memory
    if(pdbCatalog->getMetadataFromCatalog(false, emptyString, resultItemsUdfs,
                                          _udfsValues, mapUdfs,
                                          errMsg, PDBCatalogMsgType::CatalogPDBRegisteredObject) == false)
        cout << errMsg<< endl;

    for (int i=0; i < (*_udfsValues).size(); i++){
        string _typeName = (*_udfsValues)[i].getItemKey().c_str();
        int16_t _typeId = (int16_t)atoi((*_udfsValues)[i].getObjectID().c_str());

        allTypeNames[_typeName] = _typeId;
        allTypeCodes[_typeId] = _typeName;

    }

    // retrieves metadata for databases from sqlite storage and loads them into memory
    if(pdbCatalog->getMetadataFromCatalog(false, emptyString, resultItemsDBs,_allDatabases, mapDBs, errMsg, PDBCatalogMsgType::CatalogPDBDatabase) == false)
        cout << errMsg<< endl;

    // get the list of databases
    vector <string> databaseNames;

    for (int i=0; i < (*_allDatabases).size(); i++){

        string _dbName = (*_allDatabases)[i].getItemKey().c_str();

        databaseNames.push_back(_dbName);
        for (int j=0; j < (*(*_allDatabases)[i].getListOfSets()).size(); j++){

            string _setName = (*(*_allDatabases)[i].getListOfSets())[j].c_str();
            string _typeName = (*(*_allDatabases)[i].getListOfTypes())[j].c_str();

            cout << "Database " << _dbName << " has set " << _setName << " and type " << _typeName << endl;
            // populates information about databases
            allDatabases [_dbName].push_back(_setName);

            // populates information about types and sets for a given database
            cout << "ADDDDDDDing type= " << _typeName << " db= " << _dbName << " _set=" << _setName << " typeId= " << (int16_t)std::atoi(pdbCatalog->getUserDefinedTypesList()[_typeName].getObjectID().c_str()) << endl;
            setTypes [make_pair (_dbName, _setName)] = (int16_t)std::atoi(pdbCatalog->getUserDefinedTypesList()[_typeName].getObjectID().c_str());

        }
    }

//    for (int i=0; i< resultItemsDBs->size(); i++){
//        string itemId = (*resultItemsDBs)[i]->getItemKey();
//    cout << "********************  Handle<Vector<PDBCatalog>*****************************" << endl;
//    cout << "------->printing key: " << (*_allDatabases)[i]  << endl;
//    cout << "*************************************************" << endl;
//    cout << "***********************  Handle<Vector<Handle<PDBCatalog> **************************" << endl;
//    cout << "------->printing key: " << (*(*resultItemsDBs)[i])  << endl;
//    cout << "*************************************************" << endl;
//    cout << "********************  map < string, PDBCatalog>*****************************" << endl;
//    cout << "------->printing key: " << mapDBs[itemId]  << endl;
//    cout << "*************************************************" << endl;
//    cout << "********************  Handle<Vector<PDBCatalog>.printShort()*****************************" << endl;
//    cout << "------->printing key: " << ((*_allDatabases)[i]).printShort()  << endl;
//    cout << "*************************************************" << endl;
//    cout << "***********************  Handle<Vector<Handle<PDBCatalog>.printShort() **************************" << endl;
//    cout << "------->printing key: " << (*(*resultItemsDBs)[i]).printShort()  << endl;
//    cout << "*************************************************" << endl;
//    cout << "********************  map < string, PDBCatalog>.printShort()*****************************" << endl;
//    cout << "------->printing key: " << mapDBs[itemId].printShort()  << endl;
//    cout << "*************************************************" << endl;
//    }


    // retrieves metadata for nodes in the cluster from sqlite storage and loads them into memory
    if(pdbCatalog->getMetadataFromCatalog(false, emptyString,resultItemsNodes,_allNodesInCluster,mapNodes,errMsg,PDBCatalogMsgType::CatalogPDBNode) == false)
        cout << errMsg<< endl;

    for (int i=0; i < (*_allNodesInCluster).size(); i++){
        string _nodeAddress = (*_allNodesInCluster)[i].getItemId().c_str();
        string _nodeIP = (*_allNodesInCluster)[i].getNodeIP().c_str();
        int _nodePort = (*_allNodesInCluster)[i].getNodePort();
        string _nodeName = (*_allNodesInCluster)[i].getItemName().c_str();
        string _nodeType = (*_allNodesInCluster)[i].getNodeType().c_str();
        int status = (*_allNodesInCluster)[i].getNodeStatus();
        cout << _nodeAddress << " | " << _nodeIP << " | " << _nodePort << " | " << _nodeName << " | " << _nodeType << " | " << status << endl;
        allNodesInCluster.push_back(_nodeAddress);
    }

    cout << "Catalog Metadata successfully loaded!" << endl;
        
}

//TODO review and debug/clean these new catalog-related methods
bool CatalogServer :: printCatalog (string item) {

    pdbCatalog->getModifiedMetadata(item);

    return true;
}

bool CatalogServer :: addNodeMetadata (Handle<CatalogNodeMetadata> &nodeMetadata, std :: string &errMsg) {

    // adds the port to the node IP address
    string _nodeIP = nodeMetadata->getNodeIP().c_str();
    string nodAddress = _nodeIP + ":" + to_string(nodeMetadata->getNodePort());

    // don't add a node that is already registered
    if(std::find(allNodesInCluster.begin(), allNodesInCluster.end(), nodAddress) != allNodesInCluster.end()){
        errMsg = "Node " + nodAddress + " is already registered.\n";
        return false;
    }

    // add the node info to container
    allNodesInCluster.push_back (nodAddress);

    int metadataCategory = PDBCatalogMsgType::CatalogPDBNode;
    Handle<CatalogNodeMetadata> metadataObject = makeObject<CatalogNodeMetadata>();
    *metadataObject = *nodeMetadata;

    pdbCatalog->addMetadataToCatalog(metadataObject, metadataCategory, errMsg);
    mapNodes.insert(make_pair(_nodeIP,*metadataObject));

    // after it registered the node in the local catalog, iterate over all nodes,
    // make connections and broadcast the objects
    if (isMasterCatalogServer){
        cout << "About to broadcast node registration to nodes in the cluster: " << endl;

        // get the results of each broadcast
        map<string, pair <bool, string>> updateResults;
        errMsg = "";

        broadcastCatalogUpdate (metadataObject, updateResults, errMsg);
        for (auto &item : updateResults){
            cout << "Node IP: " << item.first << ((item.second.first == true) ? " updated correctly!" : " couldn't be updated due to error: ")
                 << item.second.second << endl;
        }
    } else {
        cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
    }

    //TODO
    return true;
}

bool CatalogServer :: addDatabaseMetadata (Handle<CatalogDatabaseMetadata> &dbMetadata, std :: string &errMsg) {

    // adds the port to the node IP address
    string dbName = dbMetadata->getItemName().c_str();

    // don't add a node that is already registered
    if(allDatabases.find(dbName) != allDatabases.end()){
        errMsg = "Db name: " + dbName + " is already registered.\n";
        return false;
    }

    vector<string> sets;
    // add the node info to container
    allDatabases.insert (make_pair(dbName, sets));

    int metadataCategory = PDBCatalogMsgType::CatalogPDBDatabase;
    Handle<CatalogDatabaseMetadata> metadataObject = makeObject<CatalogDatabaseMetadata>();
    *metadataObject = *dbMetadata;

    pdbCatalog->addMetadataToCatalog(metadataObject, metadataCategory, errMsg);
    mapDBs.insert(make_pair(dbName,*metadataObject));

    // after it registered the node in the local catalog, iterate over all nodes,
    // make connections and broadcast the objects
    if (isMasterCatalogServer){
        // get the results of each broadcast
        map<string, pair <bool, string>> updateResults;
        errMsg = "";
        broadcastCatalogUpdate (metadataObject, updateResults, errMsg);

        for (auto &item : updateResults){
            cout << "Node IP: " << item.first << ((item.second.first == true) ? "updated correctly!" : "couldn't be updated due to error: ")
                 << item.second.second << endl;
        }
    } else {
        cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
    }

    //TODO
    return true;
}

bool CatalogServer :: addSetMetadata (Handle<CatalogSetMetadata> &setMetadata, std :: string &errMsg) {

    // gets the set name
    string setName = string(setMetadata->getItemName().c_str());
    // gets the database name
    string dbName = string(setMetadata->getDBName().c_str());
    // gets the type Id
    int16_t typeId = (int16_t)atoi((*setMetadata).getObjectTypeId().c_str());

    // don't add a set that is already registered
    if(allDatabases.find(setName) != allDatabases.end()){
        errMsg = "Set name: " + setName + " is already registered.\n";
        return false;
    }

    // add the node info to container
    // TODO get the values from the catalog instead!!!
    // change the 1 in the last param
    cout << "inserting set-----------------> dbName= " << dbName << " setName " << setName << " id " << (*setMetadata).getObjectTypeId().c_str() <<endl;
    setTypes.insert (make_pair(make_pair(dbName, setName), typeId));

    int metadataCategory = PDBCatalogMsgType::CatalogPDBSet;
    Handle<CatalogSetMetadata> metadataObject = makeObject<CatalogSetMetadata>();
    *metadataObject = *setMetadata;

    cout << "Adding set metadata for set " << setName << endl;
    pdbCatalog->addMetadataToCatalog(metadataObject, metadataCategory, errMsg);
    mapSets.insert(make_pair(setName,*metadataObject));

    // after it registered the set in the local catalog, iterate over all nodes,
    // make connections and broadcast the objects
    if (isMasterCatalogServer){
        // get the results of each broadcast
        map<string, pair <bool, string>> updateResults;
        errMsg = "";

        broadcastCatalogUpdate (metadataObject, updateResults, errMsg);
        for (auto &item : updateResults){
            cout << "Node IP: " << item.first << ((item.second.first == true) ? "updated correctly!" : "couldn't be updated due to error: ")
                 << item.second.second << endl;
        }
    } else {
        cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
    }

    //TODO
    return true;
}

template <class Type>
bool CatalogServer :: broadcastCatalogUpdate (Handle<Type> metadataToSend,
                                              map <string, pair<bool, string>> &broadcastResults,
                                              string &errMsg) {
    PDBLoggerPtr catalogLogger = make_shared<PDBLogger>("distCatalogLogger");

    for (auto &item : mapNodes){

        string nodeAddress = string(item.second.getNodeIP().c_str()) + ":" + to_string(item.second.getNodePort());
        string nodeIP = item.second.getNodeIP().c_str();
        int nodePort = item.second.getNodePort();
        bool res = false;

        CatalogClient clusterCatalogClient = CatalogClient(nodePort, nodeIP, catalogLogger);

        //TODO new mechanism for identifying the master node not based on the name!
        if (string(item.second.getNodeType().c_str()).compare("master") !=0){

            // sends the request to a node in the cluster
            res = clusterCatalogClient.registerGenericMetadata (metadataToSend, errMsg);

            // adds the result of the update
            broadcastResults.insert(make_pair(nodeIP, make_pair(res , errMsg)));

        } else {

            cout << "Don't broadcast to " << nodeAddress << " because it has the master catalog." << endl;

        }

    }

    return true;
}

bool CatalogServer :: getIsMasterCatalogServer(){
    return isMasterCatalogServer;
}

void CatalogServer :: setIsMasterCatalogServer(bool isMasterCatalogServerIn){
    isMasterCatalogServer = isMasterCatalogServerIn;
}

}

// implicit instantiation
template bool CatalogServer :: broadcastCatalogUpdate<CatalogNodeMetadata> (
                                                                            Handle<CatalogNodeMetadata> metadataToSend, map <string, pair<bool,
                                                                            string>> &broadcastResults,
                                                                            string &errMsg);

template bool CatalogServer :: broadcastCatalogUpdate<CatalogDatabaseMetadata> (
        Handle<CatalogDatabaseMetadata> metadataToSend,
        map <string, pair<bool, string>> &broadcastResults,
        string &errMsg
        );
template bool CatalogServer :: broadcastCatalogUpdate<CatalogSetMetadata> (
        Handle<CatalogSetMetadata> metadataToSend,
        map <string, pair<bool, string>> &broadcastResults,
        string &errMsg
        );
