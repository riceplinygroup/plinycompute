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

#include <chrono>
#include <ctime>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "CatAddNodeToDatabaseRequest.h"
#include "CatAddNodeToSetRequest.h"
#include "CatalogClient.h"
#include "CatCreateDatabaseRequest.h"
#include "CatCreateSetRequest.h"
#include "CatDeleteDatabaseRequest.h"
#include "CatDeleteSetRequest.h"
#include "CatRegisterType.h"
#include "CatRemoveNodeFromDatabaseRequest.h"
#include "CatRemoveNodeFromSetRequest.h"
#include "CatSetObjectTypeRequest.h"
#include "CatSharedLibraryRequest.h"
#include "CatTypeNameSearch.h"
#include "CatTypeNameSearchResult.h"
#include "CatTypeSearchResult.h"
#include "PDBDebug.h"
#include "ShutDown.h"
#include "SimpleRequest.h"
#include "SimpleRequestResult.h"
#include "SimpleSendDataRequest.h"

namespace pdb {

// Constructor
CatalogClient::CatalogClient(int portIn,
                             std::string addressIn,
                             PDBLoggerPtr myLoggerIn,
                             bool pointsToCatalogMasterIn) {
    pointsToCatalogMaster = pointsToCatalogMasterIn;
    CatalogClient(portIn, addressIn, myLoggerIn);
}

// default constructor
CatalogClient::CatalogClient() {}

// Constructor
CatalogClient::CatalogClient(int portIn, std::string addressIn, PDBLoggerPtr myLoggerIn) {

    // get the communicator information
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

// destructor
CatalogClient::~CatalogClient() {
    // Clean up the VTable catalog ptr if it is using this CatalogClient
    if (theVTable->getCatalogClient() == this) {
        theVTable->setCatalogClient(nullptr);
    }

    pthread_mutex_destroy(&workingMutex);
}

void CatalogClient::registerHandlers(PDBServer& forMe) { /* no handlers for a catalog client!! */
}

// sends a request to a Catalog Server to register a Data Type defined in a Shared Library
bool CatalogClient::registerType(std::string fileContainingSharedLib, std::string& errMsg) {

    const LockGuard guard{workingMutex};

    // first, load up the shared library file
    std::ifstream in(fileContainingSharedLib, std::ifstream::ate | std::ifstream::binary);
    if (in.fail()) {
        errMsg = "The file " + fileContainingSharedLib + " doesn't exist or cannot be opened.\n";
        return false;
    }

    size_t fileLen = in.tellg();

    PDB_COUT << "file " << fileContainingSharedLib << endl;
    PDB_COUT << "size " << fileLen << endl;
    PDB_COUT << "Registering type " << fileContainingSharedLib << std::endl;

    const UseTemporaryAllocationBlock tempBlock{fileLen + 1024};
    bool res = false;
    {
        Handle<Vector<char>> putResultHere = makeObject<Vector<char>>(fileLen, fileLen);

        // reads the bytes from the Shared Library
        int filedesc = open(fileContainingSharedLib.c_str(), O_RDONLY);
        read(filedesc, putResultHere->c_ptr(), fileLen);
        close(filedesc);

        res = simpleSendDataRequest<CatRegisterType, char, SimpleRequestResult, bool>(
            myLogger,
            port,
            address,
            false,
            1024,
            [&](Handle<SimpleRequestResult> result) {
                if (result != nullptr) {
                    if (!result->getRes().first) {
                        errMsg = "Error registering type: " + result->getRes().second;
                        myLogger->error("Error registering type: " + result->getRes().second);
                        return false;
                    }
                    return true;
                } else {
                    errMsg = "Error registering type: got null pointer on return message.\n";
                    myLogger->error(
                        "Error registering type: got null pointer on return message.\n");
                    return false;
                }
            },
            putResultHere);
    }
    return res;
}

// makes a request to shut down a PDB server
bool CatalogClient::shutDownServer(std::string& errMsg) {

    return simpleRequest<ShutDown, SimpleRequestResult, bool>(
        myLogger, port, address, false, 1024, [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error shutting down server: " + result->getRes().second;
                    myLogger->error("Error shutting down server: " + result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return false;
        });
}

// returns true if this Catalog Client points to the Master Catalog (false otherwise)
bool CatalogClient::getPointsToMasterCatalog() {
    return pointsToCatalogMaster;
}

// sets if this Catalog Client points to the Master Catalog (true), or not (false)
void CatalogClient::setPointsToMasterCatalog(bool pointsToMaster) {
    pointsToCatalogMaster = pointsToMaster;
}

// searches for a User-Defined Type give its name and returns it's TypeID
int16_t CatalogClient::searchForObjectTypeName(std::string objectTypeName) {
    PDB_COUT << "searchForObjectTypeName for " << objectTypeName << std::endl;
    return simpleRequest<CatTypeNameSearch, CatTypeSearchResult, int16_t>(
        myLogger,
        port,
        address,
        -1,
        1024 * 1024,
        [&](Handle<CatTypeSearchResult> result) {
            if (result != nullptr) {
                PDB_COUT << "searchForObjectTypeName: getTypeId=" << result->getTypeID()
                         << std::endl;
                return result->getTypeID();
            } else {
                PDB_COUT << "searchForObjectTypeName: error in getting typeId" << std::endl;
                return (int16_t)-1;
            }
        },
        objectTypeName);
}

// retrieves the content of a Shared Library given it's Type Id
bool CatalogClient::getSharedLibrary(int16_t identifier, std::string sharedLibraryFileName) {

    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 124};
    PDB_COUT << "CatalogClient: getSharedLibrary for id=" << identifier << std::endl;
    Handle<CatalogUserTypeMetadata> tempMetadataObject = makeObject<CatalogUserTypeMetadata>();
    string sharedLibraryBytes;
    string errMsg;

    // using an empty Name b/c it's being searched by typeId
    string typeNameToSearch = "";

    bool res = getSharedLibraryByTypeName(identifier,
                                          typeNameToSearch,
                                          sharedLibraryFileName,
                                          tempMetadataObject,
                                          sharedLibraryBytes,
                                          errMsg);

    PDB_COUT << "CatalogClient: deleted putResultHere" << std::endl;
    return res;
}

// retrieves a Shared Library given it's typeName
bool CatalogClient::getSharedLibraryByTypeName(int16_t identifier,
                                               std::string& typeNameToSearch,
                                               std::string sharedLibraryFileName,
                                               Handle<CatalogUserTypeMetadata>& typeMetadata,
                                               string& sharedLibraryBytes,
                                               std::string& errMsg) {

    PDB_COUT << "inside CatalogClient getSharedLibraryByTypeName for type=" << typeNameToSearch
             << " and id=" << identifier << std::endl;

    return simpleRequest<CatSharedLibraryByNameRequest, CatalogUserTypeMetadata, bool>(
        myLogger,
        port,
        address,
        false,
        1024 * 1024 * 4,
        [&](Handle<CatalogUserTypeMetadata> result) {

            auto begin = std::chrono::high_resolution_clock::now();
            auto afterLoad = begin;
            auto afterWrite = begin;
            auto afterCopy = begin;

            PDB_COUT << "In CatalogClient- Handling CatSharedLibraryByNameRequest received from "
                        "CatalogServer..."
                     << std::endl;
            if (result == nullptr) {
                std::cout << "FATAL ERROR: can't connect to remote server to fetch shared library "
                             "for typeId="
                          << identifier << std::endl;
                exit(-1);
            }

            // gets the typeId returned by the Master Catalog
            int16_t returnedTypeId = std::atoi((result->getObjectID()).c_str());
            PDB_COUT << "Cat Client - Object Id returned " << returnedTypeId << endl;

            if (returnedTypeId == -1) {
                errMsg = "Error getting shared library: type not found in Master Catalog.\n";
                PDB_COUT << "Error getting shared library: type not found in Master Catalog.\n"
                         << endl;
                return false;
            }

            PDB_COUT << "Cat Client - Finally bytes returned " << result->getLibraryBytes().size()
                     << endl;

            if (result->getLibraryBytes().size() == 0) {
                errMsg = "Error getting shared library, no data returned.\n";
                PDB_COUT << "Error getting shared library, no data returned.\n" << endl;
                return false;
            }

            // gets metadata and bytes of the registered type
            typeNameToSearch = std::string((*result).getItemName());
            sharedLibraryBytes =
                string(result->getLibraryBytes().c_str(), result->getLibraryBytes().size());

            PDB_COUT << "   Metadata in Catalog Client " << (*result).getObjectID() << " | "
                     << (*result).getItemKey() << " | " << (*result).getItemName() << endl;

            afterLoad = std::chrono::high_resolution_clock::now();

            PDB_COUT << "copying bytes received in CatClient # bytes " << sharedLibraryBytes.size()
                     << endl;
            afterCopy = std::chrono::high_resolution_clock::now();

            // just write the shared library to the file
            int filedesc =
                open(sharedLibraryFileName.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
            PDB_COUT << "Writing file " << sharedLibraryFileName.c_str() << "\n";
            write(filedesc, sharedLibraryBytes.c_str(), sharedLibraryBytes.size());
            close(filedesc);
            afterWrite = std::chrono::high_resolution_clock::now();

            PDB_COUT << "objectFile is written by CatalogClient" << std::endl;
            PDB_COUT << "Time Duration afterLoad:\t "
                     << std::to_string(std::chrono::duration_cast<std::chrono::duration<float>>(
                                           afterLoad - begin)
                                           .count())
                     << " secs." << std::endl;
            PDB_COUT << "Time Duration afterCopy:\t "
                     << std::to_string(std::chrono::duration_cast<std::chrono::duration<float>>(
                                           afterCopy - afterLoad)
                                           .count())
                     << " secs." << std::endl;
            PDB_COUT << "Time Duration afterWrite:\t "
                     << std::to_string(std::chrono::duration_cast<std::chrono::duration<float>>(
                                           afterWrite - afterCopy)
                                           .count())
                     << " secs." << std::endl;

            return true;
        },
        identifier,
        typeNameToSearch);
}

// sends a request to obtain the TypeId of a Type, given a database and set
std::string CatalogClient::getObjectType(std::string databaseName,
                                         std::string setName,
                                         std::string& errMsg) {

    return simpleRequest<CatSetObjectTypeRequest, CatTypeNameSearchResult, std::string>(
        myLogger,
        port,
        address,
        "",
        1024,
        [&](Handle<CatTypeNameSearchResult> result) {
            if (result != nullptr) {
                auto success = result->wasSuccessful();
                if (!success.first) {
                    errMsg = "Error getting type name: " + success.second;
                    myLogger->error("Error getting type name: " + success.second);
                } else
                    return result->getTypeName();
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return std::string("");
        },
        databaseName,
        setName);
}

// sends a request to the Catalog Server to create Metadata for a new Set
bool CatalogClient::createSet(int16_t typeID,
                              std::string databaseName,
                              std::string setName,
                              std::string& errMsg) {
    PDB_COUT << "CatalogClient: to create set..." << std::endl;
    return simpleRequest<CatCreateSetRequest, SimpleRequestResult, bool>(
        myLogger,
        port,
        address,
        false,
        1024,
        [&](Handle<SimpleRequestResult> result) {
            PDB_COUT << "CatalogClient: received response for creating set" << std::endl;
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error creating set: " + result->getRes().second;
                    std::cout << "errMsg" << std::endl;
                    myLogger->error("Error creating set: " + result->getRes().second);
                    return false;
                }
                PDB_COUT << "CatalogClient: created set" << std::endl;
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            std::cout << errMsg << std::endl;
            return false;
        },
        databaseName,
        setName,
        typeID);
}

// sends a request to the Catalog Server to create Metadata for a new Database
bool CatalogClient::createDatabase(std::string databaseName, std::string& errMsg) {

    return simpleRequest<CatCreateDatabaseRequest, SimpleRequestResult, bool>(
        myLogger,
        port,
        address,
        false,
        1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error creating database: " + result->getRes().second;
                    myLogger->error("Error creating database: " + result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return false;
        },
        databaseName);
}

// sends a request to the Catalog Server to remove Metadata for a Set that is deleted
bool CatalogClient::deleteSet(std::string databaseName, std::string setName, std::string& errMsg) {

    return simpleRequest<CatDeleteSetRequest, SimpleRequestResult, bool>(
        myLogger,
        port,
        address,
        false,
        1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error deleting set: " + result->getRes().second;
                    myLogger->error("Error deleting set: " + result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return false;
        },
        databaseName,
        setName);
}

// sends a request to the Catalog Server to remove Metadata for a Database that has been deleted
bool CatalogClient::deleteDatabase(std::string databaseName, std::string& errMsg) {

    return simpleRequest<CatDeleteDatabaseRequest, SimpleRequestResult, bool>(
        myLogger,
        port,
        address,
        false,
        1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error deleting database: " + result->getRes().second;
                    myLogger->error("Error deleting database: " + result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return false;
        },
        databaseName);
}

// sends a request to the Catalog Server to add Information about a Node to a Set
bool CatalogClient::addNodeToSet(std::string nodeIP,
                                 std::string databaseName,
                                 std::string setName,
                                 std::string& errMsg) {

    return simpleRequest<CatAddNodeToSetRequest, SimpleRequestResult, bool>(
        myLogger,
        port,
        address,
        false,
        1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error creating set: " + result->getRes().second;
                    myLogger->error("Error creating set: " + result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return false;
        },
        databaseName,
        setName,
        nodeIP);
}

// sends a request to the Catalog Server to add Information about a Node to a Database
bool CatalogClient::addNodeToDB(std::string nodeIP, std::string databaseName, std::string& errMsg) {

    return simpleRequest<CatAddNodeToDatabaseRequest, SimpleRequestResult, bool>(
        myLogger,
        port,
        address,
        false,
        1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error creating database: " + result->getRes().second;
                    myLogger->error("Error creating database: " + result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return false;
        },
        databaseName,
        nodeIP);
}

// sends a request to the Catalog Server to remove Information about a Node from a Set
bool CatalogClient::removeNodeFromSet(std::string nodeIP,
                                      std::string databaseName,
                                      std::string setName,
                                      std::string& errMsg) {

    return simpleRequest<CatRemoveNodeFromSetRequest, SimpleRequestResult, bool>(
        myLogger,
        port,
        address,
        false,
        1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error deleting set: " + result->getRes().second;
                    myLogger->error("Error deleting set: " + result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return false;
        },
        databaseName,
        setName,
        nodeIP);
}

// sends a request to the Catalog Server to remove Information about a Node from a Database
bool CatalogClient::removeNodeFromDB(std::string nodeIP,
                                     std::string databaseName,
                                     std::string& errMsg) {

    return simpleRequest<CatRemoveNodeFromDatabaseRequest, SimpleRequestResult, bool>(
        myLogger,
        port,
        address,
        false,
        1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error deleting database: " + result->getRes().second;
                    myLogger->error("Error deleting database: " + result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return false;
        },
        databaseName,
        nodeIP);
}

// sends a request to the Catalog Server to add metadata about a Database
bool CatalogClient::registerDatabaseMetadata(std::string itemToSearch, std::string& errMsg) {
    PDB_COUT << "inside registerDatabaseMetadata" << endl;

    return simpleRequest<CatalogDatabaseMetadata, SimpleRequestResult, bool>(
        myLogger, port, address, false, 1024, [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error registering database metadata: " + result->getRes().second;
                    myLogger->error("Error registering database metadata: " +
                                    result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error getting type name: got nothing back from catalog";
            return false;
        });
}

// sends a request to the Catalog Server to add metadata about a Node
bool CatalogClient::registerNodeMetadata(pdb::Handle<pdb::CatalogNodeMetadata> nodeData,
                                         std::string& errMsg) {

    PDB_COUT << "registerNodeMetadata for item: " << (*nodeData) << endl;

    return simpleRequest<CatalogNodeMetadata, SimpleRequestResult, bool>(
        myLogger,
        port,
        address,
        false,
        1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error registering node metadata: " + result->getRes().second;
                    myLogger->error("Error registering node metadata: " + result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error registering node metadata in the catalog";
            return false;
        },
        nodeData);
}


// sends a request to the Catalog Server to print all metadata newer than a given timestamp
string CatalogClient::printCatalogMetadata(
        pdb::Handle<pdb::CatalogPrintMetadata> itemToSearch,
        std::string& errMsg) {

    PDB_COUT << "itemToSearch " << itemToSearch->getItemName().c_str() << endl;
    PDB_COUT << "from TimeStamp " << itemToSearch->getTimeStamp().c_str() << endl;

    return simpleRequest<pdb::CatalogPrintMetadata, CatalogPrintMetadata, string>(
        myLogger,
        port,
        address,
        "",
        1024,
        [&](Handle<CatalogPrintMetadata> result) {
            if (result != nullptr) {
                string res = result->getMetadataToPrint();
                return res;
            }
            errMsg = "Error printing catalog metadata.";
            return errMsg;
        },
        itemToSearch);
}

// sends a request to the Catalog Server to print all metadata for a given category
string CatalogClient::printCatalogMetadata(
        std::string &categoryToPrint,
        std::string& errMsg) {

    pdb::Handle<pdb::CatalogPrintMetadata> itemToPrint =
            pdb::makeObject<CatalogPrintMetadata>("","0",categoryToPrint);

    return simpleRequest<pdb::CatalogPrintMetadata, CatalogPrintMetadata, string>(
        myLogger,
        port,
        address,
        "",
        1024,
        [&](Handle<CatalogPrintMetadata> result) {
            if (result != nullptr) {
                string resultToPrint = result->getMetadataToPrint();
                return resultToPrint;
            }
            errMsg = "Error printing catalog metadata.";
            return errMsg;
        },
        itemToPrint);
}

string CatalogClient::listRegisteredDatabases (
        std :: string &errMsg) {

    string category = "databases";
    return printCatalogMetadata(category, errMsg);
}

string CatalogClient::listRegisteredSetsForADatabase (
        std :: string databaseName,
        std :: string &errMsg) {

    string category = "sets";
    return printCatalogMetadata(category, errMsg);
}

string CatalogClient::listNodesInCluster (
        std :: string &errMsg) {

    string category = "nodes";
    return printCatalogMetadata(category, errMsg);
}

string CatalogClient::listUserDefinedTypes (
        std :: string &errMsg) {

    string category = "udts";
    return printCatalogMetadata(category, errMsg);
}


// sends a request to the Catalog Serve to close the SQLite handler
bool CatalogClient::closeCatalogSQLite(std::string& errMsg) {
    return simpleRequest<CatalogCloseSQLiteDBHandler, SimpleRequestResult, bool>(
        myLogger, port, address, false, 1024, [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error printing catalog metadata: " + result->getRes().second;
                    myLogger->error("Error printing catalog metadata: " + result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error printing catalog metadata.";
            return false;
        });
}

// templated method to send a request to the Catalog Server to register Metadata about an Item in
// the Catalog
template <class Type>
bool CatalogClient::registerGenericMetadata(pdb::Handle<Type> metadataItem, std::string& errMsg) {

    return simpleRequest<Type, SimpleRequestResult, bool>(
        myLogger,
        port,
        address,
        false,
        1024 * 1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error registering node metadata: " + result->getRes().second;
                    myLogger->error("Error registering node metadata: " + result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error registering node metadata in the catalog";
            return false;
        },
        metadataItem);
}

// templated method to send a request to the Catalog Server to delete Metadata about an Item in the
// Catalog
template <class Type>
bool CatalogClient::deleteGenericMetadata(pdb::Handle<Type> metadataItem, std::string& errMsg) {

    return simpleRequest<Type, SimpleRequestResult, bool>(
        myLogger,
        port,
        address,
        false,
        1024 * 1024,
        [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = "Error removing node metadata: " + result->getRes().second;
                    myLogger->error("Error removing node metadata: " + result->getRes().second);
                    return false;
                }
                return true;
            }
            errMsg = "Error removing node metadata in the catalog";
            return false;
        },
        metadataItem);
}

/* Explicit instantiation to register various types of Metadata */
template bool CatalogClient::registerGenericMetadata(Handle<CatalogNodeMetadata> metadataItem,
                                                     string& errMsg);
template bool CatalogClient::registerGenericMetadata(Handle<CatalogDatabaseMetadata> metadataItem,
                                                     string& errMsg);
template bool CatalogClient::registerGenericMetadata(Handle<CatalogSetMetadata> metadataItem,
                                                     string& errMsg);

/* Explicit instantiation to delete various types of Metadata */
template bool CatalogClient::deleteGenericMetadata(Handle<CatDeleteDatabaseRequest> metadataItem,
                                                   string& errMsg);
template bool CatalogClient::deleteGenericMetadata(Handle<CatDeleteSetRequest> metadataItem,
                                                   string& errMsg);
}
#endif
