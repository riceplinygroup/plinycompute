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
#include "CatDeleteDatabaseRequest.h"
#include "CatDeleteSetRequest.h"
#include "CatAddNodeToDatabaseRequest.h"
#include "CatAddNodeToSetRequest.h"
#include "CatRemoveNodeFromDatabaseRequest.h"
#include "CatRemoveNodeFromSetRequest.h"
#include "SimpleRequestResult.h"
#include "SimpleRequest.h"

namespace pdb {

CatalogClient :: CatalogClient(int portIn, std :: string addressIn, PDBLoggerPtr myLoggerIn, bool pointsToCatalogMasterIn){
    pointsToCatalogMaster = pointsToCatalogMasterIn;
    CatalogClient(portIn, addressIn, myLoggerIn);
}

CatalogClient :: CatalogClient () {
}

CatalogClient :: CatalogClient (int portIn, std :: string addressIn, PDBLoggerPtr myLoggerIn) {

	// get the communication information
	port = portIn;
	address = addressIn;
	myLogger = myLoggerIn;

	// and let the v-table map know this information
    if (!theVTable->getCatalogClient()) {
        theVTable->setCatalogClient(this);
    }

	// set up the mutex
	pthread_mutex_init(&workingMutex, nullptr);
}

CatalogClient :: ~CatalogClient () {

//    std::cout << "Catalog being destroyed " << address << ":" << port << std::endl;

    // Clean up the VTable catalog ptr if it is using this CatalogClient
    if (theVTable->getCatalogClient() == this) {
        theVTable->setCatalogClient(nullptr);
    }

	pthread_mutex_destroy (&workingMutex);
}

void CatalogClient :: registerHandlers (PDBServer &forMe) { /* no handlers for a catalog client!! */}

bool CatalogClient :: registerType (std :: string fileContainingSharedLib, std :: string &errMsg) {
	
	const LockGuard guard{workingMutex};

	// first, load up the shared library file
	// get the file size
	std::ifstream in (fileContainingSharedLib, std::ifstream::ate | std::ifstream::binary);
	if (in.fail()){
	    errMsg = "The file " + fileContainingSharedLib + " doesn't exist or cannot be opened.\n";
	    return false;
	}
	size_t fileLen = in.tellg();
	cout << "file " << fileContainingSharedLib << endl;
	cout << "size " << fileLen << endl;
	
	std::cout << "Registering type " << fileContainingSharedLib << std::endl;

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

bool CatalogClient :: getPointsToMasterCatalog(){
    return pointsToCatalogMaster;
}

void CatalogClient :: setPointsToMasterCatalog(bool pointsToMaster){
    pointsToCatalogMaster = pointsToMaster;
}



int16_t CatalogClient :: searchForObjectTypeName (std :: string objectTypeName) {
        std :: cout << "searchForObjectTypeName for " << objectTypeName << std :: endl;
	return simpleRequest <CatTypeNameSearch, CatTypeSearchResult, int16_t> (myLogger, port, address, false, 1024,
		[&] (Handle <CatTypeSearchResult> result) {
			if (result != nullptr){
                                std :: cout << "searchForObjectTypeName: getTypeId=" << result->getTypeID() << std :: endl; 
				return result->getTypeID ();
                        }
			else {
                                std :: cout << "searchForObjectTypeName: error in getting typeId" << std :: endl;
				return (int16_t) -1;}},
		objectTypeName);
}

//bool CatalogClient :: getSharedLibrary (int16_t identifier, std :: string objectFile) {
//
//	const LockGuard guard{workingMutex};
//        std :: cout << "CatalogClient to fetch shared library for TypeID=" << identifier << std :: endl;
//        myLogger->error(std :: string( "CatalogClient to fetch shared library for TypeID=") + std :: to_string(identifier));
//	return simpleRequest <CatSharedLibraryRequest, Vector <char>, bool> (myLogger, port, address, false, 1024,
//		[&] (Handle <Vector <char>> result) {
//	                std :: cout << "To handle result of CatSharedLibraryRequest from CatalogServer..." << std :: endl;
//                        myLogger->debug("CatalogClient: To handle result of CatSharedLibraryRequest from CatalogServer...");
//			if (result == nullptr) {
//				myLogger->error ("Error getting shared library: null object returned.\n");
//				return false;
//			}
//
//			if (result->size () == 0) {
//				myLogger->error ("Error getting shared library, no data returned.\n");
//				return false;
//			}
//
//			// just write the shared library to the file
//			int filedesc = open (objectFile.c_str (), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
//			write (filedesc, result->c_ptr (), result->size ());
//			close (filedesc);
//			return true;},
//		identifier);
//}

bool CatalogClient :: getSharedLibrary (int16_t identifier, std :: string objectFile) {

    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 124};
    std :: cout << "CatalogClient: getSharedLibrary for id=" << identifier << std :: endl;
    Handle <CatalogUserTypeMetadata> tempMetadataObject = makeObject<CatalogUserTypeMetadata>();
    vector <char> * putResultHere = new vector<char>();
    string returnedBytes;
    string errMsg;
    //using a dummyName b/c it's being searched by typeId
    string typeNameToSearch ="dummyName";

    bool res = getSharedLibraryByName (identifier,
                            typeNameToSearch,
                            objectFile,
                            (*putResultHere),
                            tempMetadataObject,
                            returnedBytes,
                            errMsg);
    delete putResultHere;
    return res;

    //this is not needed b/c it's get copied in the previous step
            // just write the shared library to the file
//            int filedesc = open (objectFile.c_str (), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
//            write (filedesc, result->c_ptr (), result->size ());
//            close (filedesc);
}

bool CatalogClient :: getSharedLibraryByName (int16_t identifier,
                                              std :: string& typeNameToSearch,
                                              std :: string objectFile,
                                              vector <char> &putResultHere,
                                              Handle<CatalogUserTypeMetadata> &resultToCaller,
                                              string &returnedBytes,
                                              std :: string &errMsg) {

    // this allocates 128MB memory for the request
//    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 256};

    std :: cout << "inside CatalogClient getSharedLibraryByName for type=" << typeNameToSearch << " and id=" << identifier << std :: endl;

    //const LockGuard guard{workingMutex}; //this function will only be invoked from getVTablePtrUsingCatalog() which contains a lock guard already
        std :: cout << "CatalogClient to fetch shared library for TypeName=" << typeNameToSearch << " and id=" << identifier << std :: endl;
//        myLogger->error(std :: string( "CatalogClient to fetch shared library for typeNameToSearch=") + typeNameToSearch);
//    return simpleRequest <CatSharedLibraryByNameRequest, Vector <char>, bool> (myLogger, port, address, false, 1024,
        return simpleRequest <CatSharedLibraryByNameRequest, CatalogUserTypeMetadata, bool> (myLogger, port, address, false, 1024 * 1024 * 4,

//        [&] (Handle <Vector <char>> result) {
                [&] (Handle <CatalogUserTypeMetadata> result) {

                    std :: cout << "In CatalogClient- Handling CatSharedLibraryByNameRequest received from CatalogServer..." << std :: endl;
                        myLogger->debug("CatalogClient: To handle result of CatSharedLibraryByNameRequest from CatalogServer...");

            if (result == nullptr) {
                std :: cout << "FATAL ERROR: can't connect to remote server to fetch shared library for typeId=" << identifier << std :: endl;
                myLogger->error ("Error getting shared library: null object returned.\n");
                //return false;
                exit(-1);
            }
            std :: cout << "Getting the returned typeId" << std :: endl;

            // gets the typeId returned by the Master Catalog
            std :: cout << std :: string (result->getObjectID()) << std :: endl;
            int16_t returnedTypeId = std::atoi((result->getObjectID()).c_str());

            cout << "Cat Client - Object Id returned " <<  returnedTypeId << endl;

            if (returnedTypeId == -1) {
                errMsg = "Error getting shared library: type not found in Master Catalog.\n";
                myLogger->error ("Error getting shared library: type not found in Master Catalog.\n");
                cout << errMsg << endl;
                return false;
            }

            cout << "Cat Client - Finally bytes returned " << result->getLibraryBytes().size() << endl;

            if (result->getLibraryBytes().size() == 0) {
                errMsg = "Error getting shared library, no data returned.\n";
                myLogger->error ("Error getting shared library, no data returned.\n");
                cout << errMsg << endl;
                return false;
            }

            // gets metadata and bytes of the registered type
            returnedBytes = string(result->getLibraryBytes().c_str(), result->getLibraryBytes().size());
            cout << "   Metadata in Catalog Client " <<  (*result).getObjectID() << " | " << (*result).getItemKey() << " | " << (*result).getItemName() << endl;

            typeNameToSearch = std :: string((*result).getItemName());
//            memmove (&putResultHere, result->c_ptr (), result->size ());

            cout << "copying bytes received in CatClient # bytes " << returnedBytes.size() << endl;
            std::copy(returnedBytes.begin(), returnedBytes.end(), std::back_inserter(putResultHere));
            std::cout <<"bytes copied!" << std :: endl;
            // just write the shared library to the file
            int filedesc = open (objectFile.c_str (), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
            write (filedesc, returnedBytes.c_str (), returnedBytes.size());
            close (filedesc);
            std :: cout << "objectFile is written by CatalogClient" << std :: endl;
            return true;},
            identifier, typeNameToSearch);
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

bool CatalogClient :: createSet (int16_t typeID, std :: string databaseName, std :: string setName, std :: string &errMsg) {
        std :: cout << "CatalogClient: to create set..." << std :: endl;
        return simpleRequest <CatCreateSetRequest, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
                [&] (Handle <SimpleRequestResult> result) {
                        std :: cout << "CatalogClient: received response for creating set" << std :: endl;
                        if (result != nullptr) {
                                if (!result->getRes ().first) {
                                        errMsg = "Error creating set: " + result->getRes ().second;
                                        std :: cout << "errMsg" << std :: endl;
                                        myLogger->error ("Error creating set: " + result->getRes ().second);
                                        return false;
                                }
                                std :: cout << "CatalogClient: created set" << std :: endl;
                                return true;
                        }
                        errMsg = "Error getting type name: got nothing back from catalog";
                        std :: cout << errMsg << std :: endl;
                        return false;},
                databaseName, setName, typeID);

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

bool CatalogClient :: deleteSet (std :: string databaseName, std :: string setName, std :: string &errMsg) {

        return simpleRequest <CatDeleteSetRequest, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
                [&] (Handle <SimpleRequestResult> result) {
                        if (result != nullptr) {
                                if (!result->getRes ().first) {
                                        errMsg = "Error deleting set: " + result->getRes ().second;
                                        myLogger->error ("Error deleting set: " + result->getRes ().second);
                                        return false;
                                }
                                return true;
                        }
                        errMsg = "Error getting type name: got nothing back from catalog";
                        return false;},
                databaseName, setName);

}

bool CatalogClient :: deleteDatabase (std :: string databaseName, std :: string &errMsg) {

    return simpleRequest <CatDeleteDatabaseRequest, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
        [&] (Handle <SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes ().first) {
                    errMsg = "Error deleting database: " + result->getRes ().second;
                    myLogger->error ("Error deleting database: " + result->getRes ().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return false;},
        databaseName);
}

//GGGGG
bool CatalogClient :: addNodeToSet (std :: string nodeIP, std :: string databaseName, std :: string setName, std :: string &errMsg) {

        return simpleRequest <CatAddNodeToSetRequest, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
                [&] (Handle <SimpleRequestResult> result) {
                        if (result != nullptr) {
                                if (!result->getRes ().first) {
                                        errMsg = "Error creating set: " + result->getRes ().second;
                                        myLogger->error ("Error creating set: " + result->getRes ().second);
                                        return false;
                                }
                                return true;
                        }
                        errMsg = "Error getting type name: got nothing back from catalog";
                        return false;},
                databaseName, setName, nodeIP);

}

bool CatalogClient :: addNodeToDB (std :: string nodeIP, std :: string databaseName, std :: string &errMsg) {

    return simpleRequest <CatAddNodeToDatabaseRequest, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
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
            databaseName, nodeIP);
}

bool CatalogClient :: removeNodeFromSet (std :: string nodeIP, std :: string databaseName, std :: string setName, std :: string &errMsg) {

        return simpleRequest <CatRemoveNodeFromSetRequest, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
                [&] (Handle <SimpleRequestResult> result) {
                        if (result != nullptr) {
                                if (!result->getRes ().first) {
                                        errMsg = "Error deleting set: " + result->getRes ().second;
                                        myLogger->error ("Error deleting set: " + result->getRes ().second);
                                        return false;
                                }
                                return true;
                        }
                        errMsg = "Error getting type name: got nothing back from catalog";
                        return false;},
                databaseName, setName, nodeIP);

}

bool CatalogClient :: removeNodeFromDB (std :: string nodeIP, std :: string databaseName, std :: string &errMsg) {

    return simpleRequest <CatRemoveNodeFromDatabaseRequest, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
        [&] (Handle <SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes ().first) {
                    errMsg = "Error deleting database: " + result->getRes ().second;
                    myLogger->error ("Error deleting database: " + result->getRes ().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return false;},
        databaseName, nodeIP);
}

//TODO review these catalog-related methods
bool CatalogClient :: registerDatabaseMetadata (std :: string itemToSearch, std :: string &errMsg) {
    cout << "inside registerDatabaseMetadata" << endl;

    return simpleRequest <CatalogDatabaseMetadata, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
        [&] (Handle <SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes ().first) {
                    errMsg = "Error registering database metadata: " + result->getRes ().second;
                    myLogger->error ("Error registering database metadata: " + result->getRes ().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return false;}
        );
}

bool CatalogClient :: registerNodeMetadata (pdb :: Handle<pdb :: CatalogNodeMetadata> nodeData, std :: string &errMsg) {

    cout << "registerNodeMetadata for item: " << (*nodeData) << endl;

    return simpleRequest <CatalogNodeMetadata, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
        [&] (Handle <SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes ().first) {
                    errMsg = "Error registering node metadata: " + result->getRes ().second;
                    myLogger->error ("Error registering node metadata: " + result->getRes ().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error registering node metadata in the catalog";
            return false;},
            nodeData
        );
}


// List metadata
bool CatalogClient :: printCatalogMetadata (std :: string timeStamp, std :: string &errMsg) {

    cout << "print greater than " << timeStamp << endl;
    return simpleRequest <CatalogPrintMetadata, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
        [&] (Handle <SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes ().first) {
                    errMsg = "Error printing catalog metadata: " + result->getRes ().second;
                    myLogger->error ("Error printing catalog metadata: " + result->getRes ().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error printing catalog metadata.";
            return false;},
            timeStamp
    );
}


template <class Type>
bool CatalogClient :: registerGenericMetadata (pdb :: Handle<Type> metadataItem, std :: string &errMsg) {

//    cout << "invoking -----> CatalogClient :: registerGenericMetadata Register Metadata for item: " << (*metadataItem).printShort() << endl;
//    cout << "to address " << address << " | " << port << endl;
    // TODO replace the hard-coded 1024 *1024 arg below
    return simpleRequest <Type, SimpleRequestResult, bool> (myLogger, port, address, false, 1024 * 1024,
        [&] (Handle <SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes ().first) {
                    errMsg = "Error registering node metadata: " + result->getRes ().second;
                    myLogger->error ("Error registering node metadata: " + result->getRes ().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error registering node metadata in the catalog";
            return false;},
            metadataItem
        );
}

template <class Type>
bool CatalogClient :: deleteGenericMetadata (pdb :: Handle<Type> metadataItem, std :: string &errMsg) {

//    cout << "invoking -----> CatalogClient :: deleteGenericMetadata Remove Metadata for item: " << endl;
//    cout << "to address " << address << " | " << port << endl;
    // TODO replace the hard-coded 1024 *1024 arg below
    return simpleRequest <Type, SimpleRequestResult, bool> (myLogger, port, address, false, 1024 * 1024,
        [&] (Handle <SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes ().first) {
                    errMsg = "Error removing node metadata: " + result->getRes ().second;
                    myLogger->error ("Error removing node metadata: " + result->getRes ().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error removing node metadata in the catalog";
            return false;},
            metadataItem
        );
}


// implicit instantiation
template bool CatalogClient :: registerGenericMetadata<CatalogNodeMetadata> (Handle<CatalogNodeMetadata> metadataItem,  string &errMsg);
template bool CatalogClient :: registerGenericMetadata<CatalogDatabaseMetadata> (Handle<CatalogDatabaseMetadata> metadataItem, string &errMsg);
template bool CatalogClient :: registerGenericMetadata<CatalogSetMetadata> (Handle<CatalogSetMetadata> metadataItem, string &errMsg);

template bool CatalogClient :: deleteGenericMetadata<CatDeleteDatabaseRequest> (Handle<CatDeleteDatabaseRequest> metadataItem, string &errMsg);
template bool CatalogClient :: deleteGenericMetadata<CatDeleteSetRequest> (Handle<CatDeleteSetRequest> metadataItem, string &errMsg);

//TODO change these template type with correct type
//template bool CatalogClient :: deleteGenericMetadata<CatalogNodeMetadata> (Handle<CatalogNodeMetadata> metadataItem,  string &errMsg);

}
#endif
