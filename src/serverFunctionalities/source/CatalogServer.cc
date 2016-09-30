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

    //TODO change this test
    // handle a request to add metadata for a new Database in the catalog
    forMe.registerHandler (CatalogNodeMetadata_TYPEID, make_shared <SimpleRequestHandler <CatalogNodeMetadata>> (
        [&] (Handle <CatalogNodeMetadata> request, PDBCommunicatorPtr sendUsingMe) {

            // ask the catalog server for the type ID and then the name of the type
            std :: string errMsg;
            cout << "--->Testing PDBCatalog register node" << endl;
//            nodeIP, nodeIP, port, nodeName, nodeType, status

            bool res = getFunctionality <CatalogServer> ().addNodeMetadata (request->getNodeIP (), request->getNodePort (),
                                                                            request->getItemName (), request->getNodeType (),
                                                                            request->getNodeStatus (), errMsg);
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
//            bool res = getFunctionality <CatalogServer> ().addDatabase (request->dbToCreate (), errMsg);
            cout << "--->Testing PDBCatalog handler" << endl;
            bool res = true;

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
            bool res = true;

            cout << "before calling " << endl;

            printCatalog(item);

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

	cout << "AFTEEEEEEEEEEEEEEEEEEEEEEER " << typeName << " = " << pdbCatalog->getUserDefinedTypesList()[typeName].getObjectID().c_str() << endl;
	return typeCode;
	} else 
		return allTypeNames [typeName];
}

bool CatalogServer :: deleteSet (std :: string databaseName, std :: string setName, std :: string &errMsg) {

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
//	setTypes [make_pair (databaseName, setName)] = typeIdentifier;
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

    // adds metadata to catalog
    pdbCatalog->addMetadataToCatalog(metadataObject, catalogType, errMsg);

    // prepares data for the DB metadata
    catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
    Handle<CatalogDatabaseMetadata> dbMetadataObject = makeObject<CatalogDatabaseMetadata>();

    Handle<Vector<Handle<CatalogDatabaseMetadata>>> resultItems = makeObject<Vector<Handle<CatalogDatabaseMetadata>>>();
    Handle<Vector<CatalogDatabaseMetadata>> vectorResultItems = makeObject<Vector<CatalogDatabaseMetadata>>();
    map<string, CatalogDatabaseMetadata> mapRes;

    if(pdbCatalog->getMetadataFromCatalog(databaseName,resultItems,vectorResultItems,mapRes,errMsg,catalogType) == false)
        cout << errMsg<< endl;

    for (int i=0; i < (*vectorResultItems).size(); i++){
        if ((*vectorResultItems)[i].getItemKey().c_str()==databaseName) *dbMetadataObject = (*vectorResultItems)[i];
    }

    (*dbMetadataObject).addSet(setKeyCatalog);
    (*dbMetadataObject).addType(typeName);

    if (pdbCatalog->updateMetadataInCatalog(dbMetadataObject, catalogType, errMsg)==true) cout << "All is ok" << endl;
    else cout << "Error: " << errMsg << endl;

    //TODO add set to DB.catalog vector and update in catalog
    //TODO
    Handle<Vector<Handle<CatalogDatabaseMetadata>>> resultItems2 = makeObject<Vector<Handle<CatalogDatabaseMetadata>>>();
    Handle<Vector<CatalogDatabaseMetadata>> vectorResultItems2 = makeObject<Vector<CatalogDatabaseMetadata>>();
    map<string, CatalogDatabaseMetadata> mapRes2;

    if(pdbCatalog->getMetadataFromCatalog(databaseName,resultItems2,vectorResultItems2,mapRes2,errMsg,catalogType) == false)
        cout << errMsg<< endl;

//    for (int i=0; i< (*vectorResultItems2).size(); i++){
//        cout << "**********************"<< (*vectorResultItems2)[i].getItemKey().c_str() << "***************************" << endl;
//        cout << (*vectorResultItems2)[i] << endl;
//        cout << "short print " << (*vectorResultItems2)[i].printShort() << endl;
//        cout << "**********************in server call***************************" << endl;
//
//    }

    // TODO, remove it, just used for debugging
    printCatalog("");

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
	pdbCatalog->addMetadataToCatalog(metadataObject, catalogType, errMsg);
	//TODO
	return true;
}

CatalogServer :: ~CatalogServer () {
	pthread_mutex_destroy(&workingMutex);
}

CatalogServer :: CatalogServer (std :: string catalogDirectoryIn, bool usePangea) {

	catalogDirectory = catalogDirectoryIn;
        this->usePangea = usePangea;

    PDBLoggerPtr catalogLogger = make_shared<PDBLogger>("catalogLogger");

    // original Chris' catalog, will be replaced by sqlite
//	myCatalog = make_shared <MyDB_Catalog> (catalogDirectory + "/catalog");

	// creates instance of catalog
    pdbCatalog = make_shared <PDBCatalog> (catalogLogger, catalogDirectory + "/pdbCatalog");
    // retrieves catalog from an sqlite db and loads metadata into memory
    pdbCatalog->open();

	// set up the mutex
	pthread_mutex_init(&workingMutex, nullptr);

	// first, get the list of type names and type codes
//	vector <string> typeNames;
//	if (myCatalog->getStringList ("typeNames", typeNames)) {
//
//		vector <int> typeCodes;
//		myCatalog->getIntList ("typeCodes", typeCodes);
//
//		for (int i = 0; i < typeCodes.size (); i++) {
////			allTypeNames[typeNames[i]] = typeCodes[i];
////			allTypeCodes[typeCodes[i]] = typeNames[i];
//			cout << "ID= " << typeCodes[i] << " type " << typeNames[i];
//		}
//	}

	//NewCatalog way to load metadata udfs
	cout << "JUST TESTING ----->" << endl;
    int catalogType = PDBCatalogMsgType::CatalogPDBRegisteredObject;
    string errMsg;
    Handle<CatalogDatabaseMetadata> dbMetadataObject = makeObject<CatalogDatabaseMetadata>();

    //    Handle<Vector<Handle<CatalogNodeMetadata>>> resultItemsNodes = makeObject<Vector<Handle<CatalogNodeMetadata>>>();
    //    Handle<Vector<Handle<CatalogSetMetadata>>> resultItemsSets = makeObject<Vector<Handle<CatalogSetMetadata>>>();
    //    Handle<Vector<Handle<CatalogDatabaseMetadata>>> resultItemsDBs = makeObject<Vector<Handle<CatalogDatabaseMetadata>>>();
    //    Handle<Vector<Handle<CatalogUserTypeMetadata>>> resultItemsUdfs = makeObject<Vector<Handle<CatalogUserTypeMetadata>>>();

//    Handle<Vector <CatalogNodeMetadata> > _allNodesInCluster = makeObject<Vector<CatalogNodeMetadata>>();
//    Handle<Vector <CatalogSetMetadata> > _setTypes = makeObject<Vector<CatalogSetMetadata>>();;
//    Handle<Vector <CatalogDatabaseMetadata> > _allDatabases = makeObject<Vector<CatalogDatabaseMetadata>>();
//    Handle<Vector <CatalogUserTypeMetadata> > _udfsValues = makeObject<Vector<CatalogUserTypeMetadata>>();
//
//    map<string, CatalogNodeMetadata> mapNodes;
//    map<string, CatalogSetMetadata> mapSets;
//    map<string, CatalogDatabaseMetadata> mapDBs;
//    map<string, CatalogUserTypeMetadata> mapUdfs;
    string emptyString("");
    // populates types metadata
    if(pdbCatalog->getMetadataFromCatalog(emptyString,resultItemsUdfs,_udfsValues,mapUdfs,errMsg,catalogType) == false)
        cout << errMsg<< endl;

    for (int i=0; i < (*_udfsValues).size(); i++){
        string _typeName = (*_udfsValues)[i].getItemKey().c_str();
        int16_t _typeId = (int16_t)atoi((*_udfsValues)[i].getObjectID().c_str());

        allTypeNames[_typeName] = _typeId;
        allTypeCodes[_typeId] = _typeName;

        cout << (*_udfsValues)[i].getItemKey().c_str() << " | " << (*_udfsValues)[i].getObjectID().c_str() << endl;
    }

    catalogType = PDBCatalogMsgType::CatalogPDBDatabase;

    if(pdbCatalog->getMetadataFromCatalog(emptyString,resultItemsDBs,_allDatabases,mapDBs,errMsg,catalogType) == false)
        cout << errMsg<< endl;

    // get the list of databases
    vector <string> databaseNames;
//
//    vector <string> setNames;

    for (int i=0; i < (*_allDatabases).size(); i++){
        string _dbName = (*_allDatabases)[i].getItemKey().c_str();
        string _setName;
        string _typeName;
        cout << _dbName << " | " << (*_allDatabases)[i].getItemId().c_str() << endl;
        databaseNames.push_back(_dbName);
        for (int j=0; j < (*(*_allDatabases)[i].getListOfSets()).size(); j++){
            _setName = (*(*_allDatabases)[i].getListOfSets())[j].c_str();
            _typeName = (*(*_allDatabases)[i].getListOfTypes())[j].c_str();

            cout << "Database " << _dbName << " has set " << _setName << " and type " << _typeName << endl;
            allDatabases [_dbName].push_back(_setName);
            setTypes [make_pair (_dbName, _setName)] = (int16_t)std::atoi(pdbCatalog->getUserDefinedTypesList()[_typeName].getObjectID().c_str());

        }
    }

    cout << "from containers" << endl;
    for (auto &item : setTypes){
        cout << " DB: " << item.first.first << endl;
        cout << " Set: " << item.first.second << endl;
        cout << " Type Id: " << setTypes[make_pair(item.first.first, item.first.second)] << endl;
        cout << " Type Name: " << allTypeCodes[item.second]  << endl;
    }
    cout << "from containers" << endl;

    cout << "JUST TESTING ----->" << endl;

//    for (int i = 0; i < pdbCatalog->get.size (); i++) {
//        allTypeNames[typeNames[i]] = typeCodes[i];
//        allTypeCodes[typeCodes[i]] = typeNames[i];
//    }

	//end

	// get the list of databases
//	vector <string> databaseNames;
//	if (myCatalog->getStringList ("databaseNames", databaseNames)) {
//
//		// get the sets in this database
//		vector <string> setNames;
//		for (string s : databaseNames) {
//			myCatalog->getStringList (s + ".sets", setNames);
//			allDatabases [s] = setNames;
//
//			// for each set, record the type code
//			int myCode;
//			for (string setName : setNames) {
//				myCatalog->getInt (s + "." + setName + " | ", myCode);
//				cout << " Adding " << s << "." << setName << " | " << myCode << endl;
//				setTypes [make_pair (s, setName)] = myCode;
//			}
//		}
//	}
        
}

//TODO review and debug/clean these new catalog-related methods
string CatalogServer :: printCatalog (string item) {

    string catalogContents;

    pdbCatalog->printsAllCatalogMetadata();

    return catalogContents;
}

bool CatalogServer :: addNodeMetadata (std :: string nodeIP, int port,
                                       std :: string nodeName, std :: string nodeType,
                                       int status, std :: string &errMsg) {

    // adds the port to the node IP address
    nodeIP.append(":").append(to_string(port));

    // don't add a node that is alredy registered
    if(std::find(allNodesInCluster.begin(), allNodesInCluster.end(), nodeIP) != allNodesInCluster.end()){
        errMsg = "Node " + nodeIP.append(to_string(port)) + " is already registered.\n";
        return false;
    }

    // add the node info
    allNodesInCluster.push_back (nodeIP);

    //allocates 24Mb to process metadata info
    makeObjectAllocatorBlock (1024 * 1024 * 24, true);

    //TODO this might change depending on what metadata
    int catalogType = PDBCatalogMsgType::CatalogPDBNode;
    Handle<CatalogNodeMetadata> metadataObject = makeObject<CatalogNodeMetadata>(String(nodeIP), String(nodeIP), port, String(nodeName), String(nodeType), status);

    pdbCatalog->addMetadataToCatalog(metadataObject, catalogType, errMsg);
    //TODO
    return true;
}


}
