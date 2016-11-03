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

    std::cout << "Catalog being destroyed " << address << ":" << port << std::endl;

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

int16_t CatalogClient :: searchForObjectTypeName (std :: string objectTypeName) {

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

bool CatalogClient :: getSharedLibrary (int16_t identifier, std :: string objectFile) {
	
	const LockGuard guard{workingMutex};
        std :: cout << "CatalogClient to fetch shared library for TypeID=" << identifier << std :: endl;
        myLogger->error(std :: string( "CatalogClient to fetch shared library for TypeID=") + std :: to_string(identifier));
	return simpleRequest <CatSharedLibraryRequest, Vector <char>, bool> (myLogger, port, address, false, 1024,
		[&] (Handle <Vector <char>> result) {
	                std :: cout << "To handle result of CatSharedLibraryRequest from CatalogServer..." << std :: endl;	
                        myLogger->debug("CatalogClient: To handle result of CatSharedLibraryRequest from CatalogServer...");
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

bool CatalogClient :: createSet (int16_t typeID, std :: string databaseName, std :: string setName, std :: string &errMsg) {

        return simpleRequest <CatCreateSetRequest, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
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

    cout << "invoking -----> CatalogClient :: registerGenericMetadata Register Metadata for item: " << (*metadataItem).printShort() << endl;
    cout << "to address " << address << " | " << port << endl;
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

    cout << "invoking -----> CatalogClient :: deleteGenericMetadata Remove Metadata for item: " << endl;
    cout << "to address " << address << " | " << port << endl;
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

template bool CatalogClient :: deleteGenericMetadata<CatDeleteSetRequest> (Handle<CatDeleteSetRequest> metadataItem, string &errMsg);
//TODO change these template type with correct type
//template bool CatalogClient :: deleteGenericMetadata<CatalogNodeMetadata> (Handle<CatalogNodeMetadata> metadataItem,  string &errMsg);
//template bool CatalogClient :: deleteGenericMetadata<CatalogSetMetadata> (Handle<CatalogSetMetadata> metadataItem, string &errMsg);

}
#endif
