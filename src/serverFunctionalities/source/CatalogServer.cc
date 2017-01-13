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

#include <chrono>
#include <ctime>

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
#include "CatSharedLibraryByNameRequest.h"
#include "CatCreateDatabaseRequest.h"
#include "SimpleRequestResult.h"
#include "CatCreateSetRequest.h"
#include "CatDeleteDatabaseRequest.h"
#include "CatDeleteSetRequest.h"
#include "CatAddNodeToDatabaseRequest.h"
#include "CatAddNodeToSetRequest.h"
#include "CatRemoveNodeFromDatabaseRequest.h"
#include "CatRemoveNodeFromSetRequest.h"
#include "CatalogDatabaseMetadata.h"
#include "CatalogPrintMetadata.h"
#include "CatalogCloseSQLiteDBHandler.h"

namespace pdb {

    PDBCatalogPtr CatalogServer :: getCatalog(){
        return pdbCatalog;
    }


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

            cout << "--->CatalogServer handler CatalogNodeMetadata_TYPEID calling addNodeMetadata " << request->getItemKey().c_str() << endl;
                const UseTemporaryAllocationBlock block{1024*1024};
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
                cout << "--->CatalogServer handler CatalogDatabaseMetadata_TYPEID calling addDatabaseMetadata" << endl;
                const UseTemporaryAllocationBlock block{1024*1024};
                bool res;
                string itemKey = request->getItemKey().c_str();
                cout << " lookin for key " << itemKey << endl;
                // if database exists update its metadata
                if (isDatabaseRegistered(itemKey) == false){
                    cout << " Key is new then call addDatabaseMetadata " << endl;
                    res = getFunctionality <CatalogServer> ().addDatabaseMetadata (request, errMsg);
                }else{
                    cout << " Key already exists call updateDatabaseMetadata " << endl;
                    res = getFunctionality <CatalogServer> ().updateDatabaseMetadata (request, errMsg);
                }

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
                cout << "--->CatalogServer handler CatalogSetMetadata_TYPEID calling addSetMetadata" << endl;
                const UseTemporaryAllocationBlock block{1024*1024};
                bool res = getFunctionality <CatalogServer> ().addSetMetadata (request, errMsg);

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
                const UseTemporaryAllocationBlock block{1024*1024};
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

                            std :: cout << "received CatTypeNameSearch message" << std :: endl;
                // in practice, we can do better than simply locking the whole catalog, but good enough for now...
                const LockGuard guard{workingMutex};
                            std :: cout << "got lock" << std :: endl;
                const UseTemporaryAllocationBlock block{1024*1024};
                // ask the catalog serer for the type ID
                int16_t typeID = getFunctionality <CatalogServer> ().searchForObjectTypeName (request->getObjectTypeName ());
                std :: cout << "searchForObjectTypeName for "<<request->getObjectTypeName() << " is "  << typeID << std :: endl;
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
                const UseTemporaryAllocationBlock block{1024*1024};
                // ask the catalog serer for the shared library
                            // added by Jia to test a length error bug
                vector <char> * putResultHere = new vector<char>();
                std :: string errMsg;
                int16_t typeID = request->getTypeID ();
                            std :: cout << "CatalogServer to handle CatSharedLibraryRequest to get shared library for typeID=" << typeID << std :: endl;
                            getLogger()->debug(std :: string("CatalogServer to handle CatSharedLibraryRequest to get shared library for typeID=") + std :: to_string(typeID));

                bool res = getFunctionality <CatalogServer> ().getSharedLibrary (typeID, (*putResultHere), errMsg);

                if (!res) {
                    const UseTemporaryAllocationBlock tempBlock{1024};
                    Handle <Vector <char>> response = makeObject <Vector <char>> ();
                    res = sendUsingMe->sendObject (response, errMsg);
                } else {
                    cout << "On Catalog Server bytes returned " << (*putResultHere).size () << endl;
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


        // handle a request to obtain a copy of a userd-defined data type stored in a shared library along with
        // its metadata (stored as a serialized CatalogUserTypeMetadata object)
        forMe.registerHandler (CatSharedLibraryByNameRequest_TYPEID, make_shared <SimpleRequestHandler <CatSharedLibraryByNameRequest>> (
            [&] (Handle <CatSharedLibraryByNameRequest> request, PDBCommunicatorPtr sendUsingMe) {

                string typeName = request->getTypeLibraryName ();
                int16_t typeId = request->getTypeLibraryId();


                // in practice, we can do better than simply locking the whole catalog, but good enough for now...
                const LockGuard guard{workingMutex};
                cout << "Triggering Handler CatalogServer CatSharedLibraryByNameRequest for typeName=" << typeName << " and typeId=" << typeId << std :: endl;
                // ask the catalog serer for the shared library
                // added by Jia to test a length error bug
                vector <char> * putResultHere = new vector<char>();
                bool res = false;
                string returnedBytes;
                std :: string errMsg;


                getLogger()->debug(std :: string("CatalogServer to handle CatSharedLibraryByNameRequest to get shared library for typeName=") + typeName + std :: string(" and typeId=") + std :: to_string(typeId));

                if(this->isMasterCatalogServer == true) {
                    // Allocates 128Mb for sending .so libraries
                    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 128};

                    Handle <CatalogUserTypeMetadata> response = makeObject<CatalogUserTypeMetadata>();
                    //Handle <CatalogUserTypeMetadata> responseTwo = makeObject<CatalogUserTypeMetadata>();

                    cout << "    Invoking getSharedLibrary(typeName) from CatalogServer Handler b/c this is Master Catalog " << endl;

                    // if the type is not registered in the Master Catalog just return
                    if (allTypeCodes.count (typeId) == 0) {

                        const UseTemporaryAllocationBlock tempBlock{1024};
                        // Creates an empty Object just to send the response to caller
                        Handle <CatalogUserTypeMetadata> notFoundResponse = makeObject<CatalogUserTypeMetadata>();
                        String newItemID("-1");
                        notFoundResponse->setObjectId(newItemID);

                        res = sendUsingMe->sendObject (notFoundResponse, errMsg);

                    } else {

                        //resolves typeName given the typeId
                        typeName = allTypeCodes[typeId];
                        cout << "Resolved typeName " << typeName << "  for typeId=" << typeId << std :: endl;

                        // the type was found in the catalog, retrieve metadata and bytes
                        res = getFunctionality <CatalogServer> ().getSharedLibraryByName (typeId,
                                                                                          typeName,
                                                                                          (*putResultHere),
                                                                                          response,
                                                                                          returnedBytes,
                                                                                          errMsg);

                        cout << "    Bytes returned YES isMaster: " << returnedBytes.size() << endl;

                        std :: cout << "typeId=" << response->getObjectID().c_str() << std :: endl;
                        std :: cout << "ItemName=" << response->getItemName().c_str() << std :: endl;
                        std :: cout << "ItemKey=" << response->getItemKey().c_str() << std :: endl;
                        response->setLibraryBytes(returnedBytes);
                        /*
                        JiaNote: I comment below lines because response and reponseTwo are in the same allocation block after calling deepCopyToCurrentAllocationBlock in retrievesDynamicLibrary
                        String _retBytes(returnedBytes);
                        *responseTwo = *response;
                        String newItemID(response->getObjectID());
                        responseTwo->setObjectId(newItemID);
                        String newItemName(response->getItemName());
                        responseTwo->setItemName(newItemName);
                        String newItemKey(response->getItemKey());
                        responseTwo->setItemKey(newItemKey);
                        responseTwo->setLibraryBytes(_retBytes);
                        */
                        cout << "Object Id isMaster: " << response->getObjectID().c_str() << " | " << response->getItemKey() << " | " << response->getItemName() << endl;
                        if (!res) {
                            //const UseTemporaryAllocationBlock tempBlock{1024};
            //                Handle <Vector <char>> response = makeObject <Vector <char>> ();
                            //res = sendUsingMe->sendObject (responseTwo, errMsg);
                            res = sendUsingMe->sendObject (response, errMsg);
                        } else {
                            cout << "     Sending metadata and bytes to caller!" << endl;
                            //res = sendUsingMe->sendObject (responseTwo, errMsg);
                            res = sendUsingMe->sendObject (response, errMsg);
                        }

                    }
                } else {
                    //JiaNote: It is possible this request is from a backend process, in that case, it is possible that frontend catalog server already has that shared library file
                    if (allTypeCodes.count (typeId) == 0) {
                        // Allocates 124Mb for sending .so libraries
                        const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 124};

                        Handle <CatalogUserTypeMetadata> response = makeObject<CatalogUserTypeMetadata>();

                        cout << "    Connecting to the Remote Catalog Server via Catalog Client" << endl;
                        cout << "    Invoking CatalogClient.getSharedLibraryByName(typeName) from CatalogServer b/c this is Local Catalog " << endl;
                        // otherwise connect to remote master catalog server and make call

                        // uses a dummyObjectFile since this is just making a remote call to the Catalog Master Server
                        // and what matters is the returned bytes.
                        string dummyObjectFile = string("temp.so");

                        res = catalogClientConnectionToMasterCatalogServer.getSharedLibraryByName(typeId,
                                                                                              typeName,
                                                                                              dummyObjectFile,
                                                                                              (*putResultHere),
                                                                                              response,
                                                                                              returnedBytes,
                                                                                              errMsg);
                        //std :: cout << "       response->getObjectID()=" << response->getObjectID().c_str() << std :: endl;
                        cout << "     Bytes returned NOT isMaster: " << returnedBytes.size() << endl;

                        // if the library was successfully retrieved, go ahead and resolve vtable fixing
                        // in the local catalog
                        if (res == true) {
                            // resolves vtable fixing on the local catalog, given the library and metadata
                            // retrieved from the remote Master Catalog
                            res = getFunctionality <CatalogServer> ().addObjectType (*putResultHere, errMsg);
                        }

                        if (!res) {
                            cout << "     before sending response Vtable not fixed!!!!!!" << endl;

                            cout << errMsg << endl;
                            const UseTemporaryAllocationBlock tempBlock{1024};

                            Handle <CatalogUserTypeMetadata> notFoundResponse = makeObject<CatalogUserTypeMetadata>();
                            String newItemID("-1");
                            notFoundResponse->setObjectId(newItemID);

                            res = sendUsingMe->sendObject (notFoundResponse, errMsg);

                       } else {
                            cout << "     before sending response Vtable fixed!!!!" << endl;
                            const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 128};
                            Handle <CatalogUserTypeMetadata> responseTwo = makeObject<CatalogUserTypeMetadata>();

                    //JiaNote, response fields can not be correctly set in following function:
                    /*
                     *             res = catalogClientConnectionToMasterCatalogServer.getSharedLibraryByName(typeId,
                                                                                          typeName,
                                                                                          dummyObjectFile,
                                                                                          (*putResultHere),
                                                                                          response,
                                                                                          returnedBytes,
                                                                                          errMsg);
                     */

                           //Therefore below code is refactored a bit

                           String _retBytes(returnedBytes);
                           char objectIDCharArray[50];
                           sprintf(objectIDCharArray, "%d", typeId);
                           String newItemID(objectIDCharArray);
                           responseTwo->setObjectId(newItemID);
                           responseTwo->setLibraryBytes(_retBytes);
                           String newTypeName(typeName);
                           responseTwo->setItemName(newTypeName);
                           responseTwo->setItemKey(newTypeName);
                           res = sendUsingMe->sendObject (responseTwo, errMsg);
                       }
                    } else {
                       //JiaNote: I already have the shared library file, because the catalog client may come from a backend process
                       //JiaNote: I copied following code from Master Catalog code path in this handler
                       // Allocates 124Mb for sending .so libraries
                       const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 128};

                       Handle <CatalogUserTypeMetadata> response = makeObject<CatalogUserTypeMetadata>();

                       typeName = allTypeCodes[typeId];
                       std :: cout << "Resolved typeName" << typeName << " for typeId=" << typeId << std :: endl;
                       // the type was found in the catalog, retrieve metadata and bytes
                       res = getFunctionality <CatalogServer> ().getSharedLibraryByName (typeId,
                                                                                          typeName,
                                                                                          (*putResultHere),
                                                                                          response,
                                                                                          returnedBytes,
                                                                                          errMsg);

                       cout << "    Bytes returned No isMaster: " << returnedBytes.size() << endl;


                       response->setLibraryBytes(returnedBytes);

                       cout << "Object Id isLocal: " << response->getObjectID().c_str() << " | " << response->getItemKey() << " | " << response->getItemName() << endl;
                        if (!res) {
                        const UseTemporaryAllocationBlock tempBlock{1024};
            //                Handle <Vector <char>> response = makeObject <Vector <char>> ();
                            res = sendUsingMe->sendObject (response, errMsg);
                        } else {
                            cout << "     Sending metadata and bytes to caller!" << endl;
                            res = sendUsingMe->sendObject (response, errMsg);
                        }

                    }


                }

                cout << " Num bytes in putResultHere " << (*putResultHere).size() << endl;

                delete putResultHere;

                return make_pair (res, errMsg);
            }
        ));

        // handle a request to get the string corresponding to the name of an object type
        forMe.registerHandler (CatSetObjectTypeRequest_TYPEID, make_shared <SimpleRequestHandler <CatSetObjectTypeRequest>> (
            [&] (Handle <CatSetObjectTypeRequest> request, PDBCommunicatorPtr sendUsingMe) {

                // in practice, we can do better than simply locking the whole catalog, but good enough for now...
                const LockGuard guard{workingMutex};
                const UseTemporaryAllocationBlock block{1024*1024};
                // ask the catalog server for the type ID and then the name of the type
                int16_t typeID = getFunctionality <CatalogServer> ().getObjectType (request->getDatabaseName (), request->getSetName ());
                            std :: cout << "typeID for Set with dbName=" << request->getDatabaseName() << " and setName=" << request->getSetName() << " is " << typeID << std :: endl;
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
                const UseTemporaryAllocationBlock block{1024*1024};
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
                const UseTemporaryAllocationBlock block{1024*1024};
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


        // handle a request to delete a database
        forMe.registerHandler (CatDeleteDatabaseRequest_TYPEID, make_shared <SimpleRequestHandler <CatDeleteDatabaseRequest>> (
            [&] (Handle <CatDeleteDatabaseRequest> request, PDBCommunicatorPtr sendUsingMe) {

                // in practice, we can do better than simply locking the whole catalog, but good enough for now...
                const LockGuard guard{workingMutex};
                const UseTemporaryAllocationBlock block{1024*1024};
                // ask the catalog server for the type ID and then the name of the type
                std :: string errMsg;
                bool res = getFunctionality <CatalogServer> ().deleteDatabase (request->dbToDelete (), errMsg);

                // make the response
                const UseTemporaryAllocationBlock tempBlock{1024};
                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

                // return the result
                res = sendUsingMe->sendObject (response, errMsg);
                return make_pair (res, errMsg);
            }
        ));

        forMe.registerHandler (CatDeleteSetRequest_TYPEID, make_shared <SimpleRequestHandler <CatDeleteSetRequest>> (
            [&] (Handle <CatDeleteSetRequest> request, PDBCommunicatorPtr sendUsingMe) {

                // in practice, we can do better than simply locking the whole catalog, but good enough for now...
                const LockGuard guard{workingMutex};
                const UseTemporaryAllocationBlock block{1024*1024};
                // ask the catalog server for the type ID and then the name of the type
                std :: string errMsg;
                auto info = request->whichSet ();
                bool res = getFunctionality <CatalogServer> ().deleteSet (info.first, info.second, errMsg);

                // make the response
                const UseTemporaryAllocationBlock tempBlock{1024};
                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

                // return the result
                res = sendUsingMe->sendObject (response, errMsg);
                return make_pair (res, errMsg);
            }
        ));

        // handle a request to add a node to a database
        forMe.registerHandler (CatAddNodeToDatabaseRequest_TYPEID, make_shared <SimpleRequestHandler <CatAddNodeToDatabaseRequest>> (
            [&] (Handle <CatAddNodeToDatabaseRequest> request, PDBCommunicatorPtr sendUsingMe) {

                // in practice, we can do better than simply locking the whole catalog, but good enough for now...
                const LockGuard guard{workingMutex};
                const UseTemporaryAllocationBlock block{1024*1024};
                // ask the catalog server for the type ID and then the name of the type
                std :: string errMsg;
                bool res = getFunctionality <CatalogServer> ().addNodeToDB (request->nodeToAdd (), request->whichDB(), errMsg);

                // make the response
                const UseTemporaryAllocationBlock tempBlock{1024};
                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

                // return the result
                res = sendUsingMe->sendObject (response, errMsg);
                return make_pair (res, errMsg);
            }
        ));

        forMe.registerHandler (CatAddNodeToSetRequest_TYPEID, make_shared <SimpleRequestHandler <CatAddNodeToSetRequest>> (
            [&] (Handle <CatAddNodeToSetRequest> request, PDBCommunicatorPtr sendUsingMe) {

                // in practice, we can do better than simply locking the whole catalog, but good enough for now...
                const LockGuard guard{workingMutex};
                const UseTemporaryAllocationBlock block{1024*1024};
                // ask the catalog server for the type ID and then the name of the type
                std :: string errMsg;
                bool res = getFunctionality <CatalogServer> ().addNodeToSet (request->nodeToAdd(), request->whichDB(), request->whichSet(), errMsg);

                // make the response
                const UseTemporaryAllocationBlock tempBlock{1024};
                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

                // return the result
                res = sendUsingMe->sendObject (response, errMsg);
                return make_pair (res, errMsg);
            }
        ));

        // handle a request to remove a node from a database
        forMe.registerHandler (CatRemoveNodeFromDatabaseRequest_TYPEID, make_shared <SimpleRequestHandler <CatRemoveNodeFromDatabaseRequest>> (
            [&] (Handle <CatRemoveNodeFromDatabaseRequest> request, PDBCommunicatorPtr sendUsingMe) {

                // in practice, we can do better than simply locking the whole catalog, but good enough for now...
                const LockGuard guard{workingMutex};
                const UseTemporaryAllocationBlock block{1024*1024};
                // ask the catalog server for the type ID and then the name of the type
                std :: string errMsg;
                bool res = getFunctionality <CatalogServer> ().removeNodeFromDB (request->nodeToRemove (), request->whichDB(), errMsg);

                // make the response
                const UseTemporaryAllocationBlock tempBlock{1024};
                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

                // return the result
                res = sendUsingMe->sendObject (response, errMsg);
                return make_pair (res, errMsg);
            }
        ));

        forMe.registerHandler (CatRemoveNodeFromSetRequest_TYPEID, make_shared <SimpleRequestHandler <CatRemoveNodeFromSetRequest>> (
            [&] (Handle <CatRemoveNodeFromSetRequest> request, PDBCommunicatorPtr sendUsingMe) {

                // in practice, we can do better than simply locking the whole catalog, but good enough for now...
                const LockGuard guard{workingMutex};
                const UseTemporaryAllocationBlock block{1024*1024};
                // ask the catalog server for the type ID and then the name of the type
                std :: string errMsg;
                auto info = request->whichSet ();
                bool res = getFunctionality <CatalogServer> ().removeNodeFromSet (request->nodeToRemove(), request->whichDB(), request->whichSet(), errMsg);

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
                const UseTemporaryAllocationBlock block{1024*1024};
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

        // Handle to close the SQLite DB Handler
        forMe.registerHandler (CatalogCloseSQLiteDBHandler_TYPEID, make_shared <SimpleRequestHandler <CatalogCloseSQLiteDBHandler>> (
            [&] (Handle <CatalogCloseSQLiteDBHandler> request, PDBCommunicatorPtr sendUsingMe) {

                std :: string errMsg;
                cout << "--->Testing CatalogCloseSQLiteDBHandler handler " << endl;
                const UseTemporaryAllocationBlock block{1024*1024};

                pdbCatalog->closeSQLiteHandler();

                // make the response
                const UseTemporaryAllocationBlock tempBlock{1024};
                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (true, errMsg);

                // return the result
                bool res = sendUsingMe->sendObject (response, errMsg);
                return make_pair (res, errMsg);
            }
        ));

    }


    std :: string CatalogServer :: searchForObjectTypeName (int16_t typeIdentifier) {

            std :: cout << "searchForObjectTypeName with typeIdentifier =" << typeIdentifier << std :: endl;
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

    bool CatalogServer :: getSharedLibrary (int16_t identifier, vector <char> &putResultHere, std :: string &errMsg) {

            //std :: cout << "CatalogServer getSharedLibrary: typeId=" << identifier << std :: endl;
        // first, make sure we have this identifier
        if (allTypeCodes.count (identifier) == 0) {
            errMsg = "CatalogServer::getSharedLibrary(): Error: didn't know the identifier you sent me";
                    std :: cout << errMsg << std :: endl;
                    getLogger()->error(errMsg);
            return false;
        }

        //TODO connect remotely and get the file

        //



        // now, read in the .so file, and put it in the vector
        std :: string whichFile = catalogDirectory + "/pdbCatalog/tmp_so_files/" + allTypeCodes[identifier] + ".so";
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

    bool CatalogServer :: getSharedLibraryByName (int16_t identifier, std :: string typeName, vector <char> &putResultHere, Handle <CatalogUserTypeMetadata> &itemMetadata, string &returnedBytes, std :: string &errMsg) {
        cout << " Catalog Server->inside get getSharedLibraryByName id for type " << typeName << endl;

            //std :: cout << "CatalogServer getSharedLibrary: typeId=" << identifier << std :: endl;
        // first, make sure we have this identifier
    //    if (allTypeCodes.count (identifier) == 0) {
    //        errMsg = "CatalogServer::getSharedLibrary(): Error: didn't know the identifier you sent me";
    //                std :: cout << errMsg << std :: endl;
    //                getLogger()->error(errMsg);
    //        return false;
    //    }

        // TODO debug these lines
        int metadataCategory = (int)PDBCatalogMsgType::CatalogPDBRegisteredObject;
        // gets type id
        string id = pdbCatalog->itemName2ItemId(metadataCategory, typeName);
        cout << " id " << id << endl;

        string soFileBytes;
        string typeOfObject = "data_types";

        // retrieves metadata and library bytes from the catalog
        bool res = retrieveUserDefinedTypeMetadata(typeName,
                itemMetadata,
                returnedBytes,
                errMsg);

        // the item was found
        if (res == true){
            cout << "Metadata returned at get SharedLibrary Id: " << itemMetadata->getItemId().c_str() << endl;
            cout << "Metadata returned at get SharedLibrary Key: " << itemMetadata->getItemKey().c_str() << endl;
            cout << "--pass" << endl;

        //    cout << "Size after returning " <<  itemMetadata->getLibraryBytes().size() << endl;

        //    string returnedBytes = string(itemMetadata->getLibraryBytes());
        //    char *buffer = new char[itemMetadata->getLibraryBytes().size()];
        //    memcpy (buffer, itemMetadata->getLibraryBytes().c_str(), itemMetadata->getLibraryBytes().size());

        //    returnedBytes = string(buffer,itemMetadata->getLibraryBytes().size());


            cout << "Bytes after string " << returnedBytes.size()<< endl;

        //    cout << "bytes size " << returnedBytes.length() << endl;

            cout << "bytes before putResultHere " << putResultHere.size() << endl;

            // copy bytes to output param
            std::copy(returnedBytes.begin(), returnedBytes.end(), std::back_inserter(putResultHere));
        } else {
            cout << "Item with key " << typeName <<  " was not found!" << endl;
        }

        return res;
    }

    int16_t CatalogServer :: getObjectType (std :: string databaseName, std :: string setName) {
        //cout << "getObjectType make_pair " << databaseName << " " << setName << endl;
        if (setTypes.count (make_pair (databaseName, setName)) == 0)
            return -1;

        return setTypes[make_pair (databaseName, setName)];
    }

    int16_t CatalogServer :: addObjectType (vector <char> &soFile, string &errMsg) {

        // and add the new .so file
        string tempFile = catalogDirectory + "/pdbCatalog/tmp_so_files/temp.so";
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

       cout << "open function: " << getName << endl;
        if ((dlsym_error = dlerror())) {
            errMsg = "Error, can't load function getObjectTypeName in the shared library. " + string(dlsym_error) + '\n';
            cout << errMsg << endl;
            return -1;
        }
        cout << "all ok" << endl;

        // now, get the type name and write the appropriate file
        string typeName (myFunc ());
        dlclose (so_handle);
        //rename file
        string newName = catalogDirectory + "/pdbCatalog/tmp_so_files/" + typeName + ".so";
        int result = rename( tempFile.c_str() , newName.c_str());
        if ( result == 0 ) {
            cout << "Successfully renaming file " << newName << endl;
        } else {
            errMsg = "Renaming temp file failed " + newName + "\n";
            return -1;
        }

        // add the new type name, if we don't already have it
        if (allTypeNames.count (typeName) == 0) {
            cout << "Fixing vtable ptr for type " << typeName << " with metadata retrieved from remote Catalog Server." << endl;
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

            cout << "before creating object" << endl;
            //allocates 128Mb to register .so libraries

            //JiaNote: we should use temporary allocation block in functions that may be invoked by other handlers
            //makeObjectAllocatorBlock (1024 * 1024 * 128, true);
            const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 128};
            Handle<CatalogUserTypeMetadata> objectMetadata = makeObject<CatalogUserTypeMetadata>();
            cout << "before calling " << endl;

            pdbCatalog->registerUserDefinedObject(objectMetadata, std::string(soFile.begin(), soFile.end()), typeName, catalogDirectory + "/pdbCatalog/tmp_so_files/" + typeName + ".so", "data_types", errMsg);

            return typeCode;
        } else
            return allTypeNames [typeName];
    }

    bool CatalogServer :: deleteSet (std :: string databaseName, std :: string setName, std :: string &errMsg) {

        // allocate memory temporarily
        // TODO change this later
    //    makeObjectAllocatorBlock (1024 * 1024 * 128, true);

        string setUniqueId = databaseName + "." + setName;
        cout << "Deleting set " << setUniqueId << endl;

        if (isDatabaseRegistered(databaseName) ==  false){
            errMsg = "Database does not exist.\n";
            cout << errMsg << endl;
            return false;
        }

        // make sure that set exists
        if (isSetRegistered(databaseName, setName) ==  false){
            errMsg = "Set doesn't exist " + databaseName + "." + setName;
            cout << errMsg << endl;

            return false;
        }

        Handle<CatalogSetMetadata> metadataObject = makeObject<CatalogSetMetadata>();
        int catalogType = PDBCatalogMsgType::CatalogPDBSet;

        // TODO: Why not just look up the metadata from the catalog?
        // creates Strings
        string _setName(databaseName + "." + setName);
        String setKeyCatalog = String(_setName);
        String setNameCatalog = String(setName);
        String dbName(databaseName);
        String setId = String(pdbCatalog->itemName2ItemId(catalogType, _setName));

        // populates object metadata
        metadataObject->setItemId(setId);
        metadataObject->setItemKey(setKeyCatalog);
        metadataObject->setItemName(setNameCatalog);
        metadataObject->setDBName(dbName);

        // deletes metadata in sqlite

        pdbCatalog->deleteMetadataInCatalog( metadataObject, catalogType, errMsg);

        // prepares object to update database entry in sqlite
        catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
        Handle<CatalogDatabaseMetadata> dbMetadataObject = makeObject<CatalogDatabaseMetadata>();

        Handle<Vector<CatalogDatabaseMetadata>> vectorResultItems = makeObject<Vector<CatalogDatabaseMetadata>>();

        if(pdbCatalog->getMetadataFromCatalog(false, databaseName,vectorResultItems,errMsg,catalogType) == false)
            cout << errMsg<< endl;

        for (int i=0; i < (*vectorResultItems).size(); i++){
            if ((*vectorResultItems)[i].getItemKey().c_str()==databaseName) *dbMetadataObject = (*vectorResultItems)[i];
        }

        (*dbMetadataObject).deleteSet(setNameCatalog);

        // updates the corresponding database metadata
        if (!pdbCatalog->updateMetadataInCatalog(dbMetadataObject, catalogType, errMsg)) {
            return false;
        }

        // after it updated the database metadata in the local catalog, iterate over all nodes,
        // make connections and broadcast the objects
        if (isMasterCatalogServer){

            // TODO: Should we explicitly delete the data or just perform data overwrites?

            // map to capture the results of broadcasting the Set insertion
            map<string, pair <bool, string>> updateResults;

            // first, broadcasts the metadata of the removed set to all local copies of the catalog
            // in the cluster, inserting the new item
            Handle<CatDeleteSetRequest> setToRemove = makeObject<CatDeleteSetRequest>(databaseName, setName);
            broadcastCatalogDelete (setToRemove, updateResults, errMsg);

            for (auto &item : updateResults){
                cout << "Set Metadata in node IP: " << item.first << ((item.second.first == true) ? " updated correctly!" : " couldn't be updated due to error: ")
                     << item.second.second << endl;
            }

            // map to capture the results of broadcasting the DB update
            map<string, pair <bool, string>> updateSetResults;


            // second, broadcasts the metadata of the DB to which this set has been removed from,
            // updating all local copies of the catalog in the cluster
            broadcastCatalogUpdate (dbMetadataObject, updateSetResults, errMsg);

            for (auto &item : updateSetResults){
                cout << "DB Metadata in node IP: " << item.first << ((item.second.first == true) ? "updated correctly!" : "couldn't be updated due to error: ")
                     << item.second.second << endl;
            }
            cout << "******************* deleteSet step completed!!!!!!!" << endl;
        } else {
            cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
        }

        return true;
    }

    bool CatalogServer :: addSet (int16_t typeIdentifier, std :: string databaseName, std :: string setName, std :: string &errMsg) {
        auto begin = std :: chrono :: high_resolution_clock :: now();

        // make sure we are only adding to an existing database
        if (isDatabaseRegistered(databaseName) == false){
            errMsg = "Database does not exist.\n";
            cout << errMsg << endl;
            return false;
        }

        // make sure that set does not exist
        string setUniqueId = databaseName + "." + setName;
        if (isSetRegistered(databaseName, setName) ==  true){
            errMsg = "Set already exists.\n";
            cout << errMsg << endl;
            return false;

        }

        // make sure that type code exists, if we get one that is not built in
        if (typeIdentifier >= 8192 && allTypeCodes.count (typeIdentifier) == 0) {
            errMsg = "Type code does not exist.\n";
                    cout << errMsg << "TypeId="<< typeIdentifier << endl;
            return false;
        }

        cout << "...... Calling CatalogServer :: addSet" << endl;

        //  // and add the set's type
        setTypes [make_pair (databaseName, setName)] = typeIdentifier;
        //JiaNote: commented below line, because allTypeCodes only contains registered type, but doesn't contain built-in type
        //String typeName(allTypeCodes[typeIdentifier]);

        //JiaNote: added below code to replace above line so that built-in type and registered type can both get translated
        string typeNameStr = searchForObjectTypeName (typeIdentifier);
        std :: cout << "Got typeName=" << typeNameStr << std :: endl;
        if (typeNameStr == "") {
            errMsg = "TypeName doesn not exist";
            return false;
        }
        String typeName(typeNameStr);

        auto afterChecks = std :: chrono :: high_resolution_clock :: now();

            std :: cout << "TypeID for Set with dbName=" << databaseName << " and setName=" << setName << " is " << typeIdentifier << std :: endl;
        //TODO this might change depending on what metadata
        Handle<CatalogSetMetadata> metadataObject = makeObject<CatalogSetMetadata>();
        // TODO *****************New Metadata*****************
        CatalogStandardSetMetadata metadataItem = CatalogStandardSetMetadata();
        // populates object metadata
        metadataItem.setItemKey(setUniqueId);
        metadataItem.setItemName(setName);

        int catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
        string dbId = pdbCatalog->itemName2ItemId(catalogType, databaseName);

        metadataItem.setDBId(dbId);
        metadataItem.setDBName(databaseName);

        catalogType = PDBCatalogMsgType::CatalogPDBRegisteredObject;

        //JiaNote: commented below line to use the typeIdentifier passed in as parameter.
        //std :: string itemId = pdbCatalog->itemName2ItemId(catalogType, allTypeCodes[typeIdentifier]);

        //JiaNote: added below code to replace the above line
        string typeId = std :: to_string(typeIdentifier);

        metadataItem.setTypeId(typeId);
        metadataItem.setTypeName(typeNameStr);
        //*****************New metadata*****************


        // creates Strings
        String setKeyCatalog = String(setUniqueId);
        String setNameCatalog = String(setName);
        String dbName(databaseName);

        // populates object metadata
        metadataObject->setItemKey(setKeyCatalog);
        metadataObject->setItemName(setNameCatalog);

        catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
        String _dbId = String(pdbCatalog->itemName2ItemId(catalogType, databaseName));

        metadataObject->setDBId(_dbId);
        metadataObject->setDBName(dbName);

        catalogType = PDBCatalogMsgType::CatalogPDBRegisteredObject;

        //JiaNote: commented below line to use the typeIdentifier passed in as parameter.
        //std :: string itemId = pdbCatalog->itemName2ItemId(catalogType, allTypeCodes[typeIdentifier]);

        //JiaNote: added below code to replace the above line
        String _typeId = String(std :: to_string(typeIdentifier));

        metadataObject->setTypeId(_typeId);
        metadataObject->setTypeName(typeName);

        catalogType = PDBCatalogMsgType::CatalogPDBSet;

        auto beforeCallAddUpdate = std :: chrono :: high_resolution_clock :: now();

        // stores metadata in sqlite
        if (isSetRegistered(dbName, setName) ==  false){
           pdbCatalog->addMetadataToCatalog(metadataObject, metadataItem, catalogType, errMsg);
        }else{
            pdbCatalog->updateMetadataInCatalog(metadataObject, catalogType, errMsg);
        }
        auto afterCallAddUpdate = std :: chrono :: high_resolution_clock :: now();


        // prepares data for the DB metadata
        catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
        Handle<CatalogDatabaseMetadata> dbMetadataObject = makeObject<CatalogDatabaseMetadata>();

        Handle<Vector<CatalogDatabaseMetadata>> vectorResultItems = makeObject<Vector<CatalogDatabaseMetadata>>();

        if(pdbCatalog->getMetadataFromCatalog(false, databaseName,vectorResultItems,errMsg,catalogType) == false)
            cout << errMsg<< endl;

        for (int i=0; i < (*vectorResultItems).size(); i++){
            if ((*vectorResultItems)[i].getItemKey().c_str()==databaseName) *dbMetadataObject = (*vectorResultItems)[i];
        }

        auto aftergetMetadataFromCatalog = std :: chrono :: high_resolution_clock :: now();

        (*dbMetadataObject).addSet(setNameCatalog);
        (*dbMetadataObject).addType(typeName);

        // updates the corresponding database metadata
        catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
        // if database exists update its metadata
        if (isDatabaseRegistered(databaseName) == true){
           if (pdbCatalog->updateMetadataInCatalog(dbMetadataObject, catalogType, errMsg)==true) cout << "DB Update Set metadata OK" << endl;
           else{
               cout << "DB Update metadata Set Error: " << errMsg << endl;
               return false;
           }

        }else{
            return false;
        }

        auto afterUpdateMetadata = std :: chrono :: high_resolution_clock :: now();
        auto broadCast1 = afterUpdateMetadata;
        auto broadCast2 = afterUpdateMetadata;


        // after it registered the database metadata in the local catalog, iterate over all nodes,
        // make connections and broadcast the objects
        if (isMasterCatalogServer){
            // map to capture the results of broadcasting the Set insertion
            map<string, pair <bool, string>> updateResults;

            // first, broadcasts the metadata of the new set to all local copies of the catalog
            // in the cluster, inserting the new item
            broadcastCatalogUpdate (metadataObject, updateResults, errMsg);
            broadCast1 = std :: chrono :: high_resolution_clock :: now();

            for (auto &item : updateResults){
                cout << "Set Metadata broadcasted to node IP: " << item.first << ((item.second.first == true) ? " updated correctly!" : " couldn't be updated due to error: ")
                     << item.second.second << endl;
            }

            // map to capture the results of broadcasting the DB update
            map<string, pair <bool, string>> updateSetResults;

            // second, broadcasts the metadata of the DB to which this set has been added,
            // updating all local copies of the catalog in the cluster
            broadcastCatalogUpdate (dbMetadataObject, updateSetResults, errMsg);
            broadCast2 = std :: chrono :: high_resolution_clock :: now();

            for (auto &item : updateSetResults){
                cout << "DB Metadata broadcasted to node IP: " << item.first << ((item.second.first == true) ? " updated correctly!" : " couldn't be updated due to error: ")
                     << item.second.second << endl;
            }
            cout << "******************* addSet step completed!!!!!!!" << endl;
        } else{
            cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
        }

        auto end = std :: chrono :: high_resolution_clock :: now();

        std::cout << "Time Duration for check registration:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(afterChecks-begin).count() << " secs." << std::endl;
        std::cout << "Time Duration for Setting Metadata values:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(beforeCallAddUpdate-afterChecks).count() << " secs." << std::endl;
        std::cout << "Time Duration for addMetadataToCatalog SET:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(afterCallAddUpdate-beforeCallAddUpdate).count() << " secs." << std::endl;
        std::cout << "Time Duration for getMetadataFromCatalog call:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(aftergetMetadataFromCatalog-afterCallAddUpdate).count() << " secs." << std::endl;
        std::cout << "Time Duration for Updte DB Metadata:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(afterUpdateMetadata-aftergetMetadataFromCatalog).count() << " secs." << std::endl;
        std::cout << "Time Duration for broadcastCatalogUpdate SET:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(broadCast1-afterUpdateMetadata).count() << " secs." << std::endl;
        std::cout << "Time Duration for broadcastCatalogUpdate DB:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(broadCast2-broadCast1).count() << " secs." << std::endl;
        std::cout << "------>Time Duration to AddSet\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(end-begin).count() << " secs." << std::endl;

        // TODO, remove it, just used for debugging
    //    printCatalog("");

        return true;
    }


    bool CatalogServer :: addDatabase (std :: string databaseName, std :: string &errMsg) {

        // don't add a database that is alredy there
        if (isDatabaseRegistered(databaseName) == true){
            errMsg = "Database name already exists.\n";
            return false;
        }

        vector <string> empty;

        cout << "...... Calling CatalogServer :: addDatabase" << endl;

        //allocates 24Mb to process metadata info
    //    makeObjectAllocatorBlock (1024 * 1024 * 24, true);

        int catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
        Handle<CatalogDatabaseMetadata> metadataObject = makeObject<CatalogDatabaseMetadata>();

        // TODO *****************New Metadata*****************
        CatalogStandardDatabaseMetadata metadataItem = CatalogStandardDatabaseMetadata();
        // populates object metadata
        metadataItem.setItemName(databaseName);
        //*****************New metadata*****************

        String dbName = String(databaseName);
        metadataObject->setItemName(dbName);

        // stores metadata in sqlite
        if (isDatabaseRegistered(databaseName) == false){
           pdbCatalog->addMetadataToCatalog(metadataObject, metadataItem, catalogType, errMsg);
        }else{
            pdbCatalog->updateMetadataInCatalog(metadataObject, catalogType, errMsg);
        }

        // after it registered the database metadata in the local catalog, iterate over all nodes,
        // make connections and broadcast the objects
        if (isMasterCatalogServer){
            // get the results of each broadcast
            map<string, pair <bool, string>> updateResults;
            errMsg = "";

            broadcastCatalogUpdate (metadataObject, updateResults, errMsg);

            for (auto &item : updateResults){
                // adds node info to database metadata
                cout << "DB metadata broadcasted to node IP: " << item.first << ((item.second.first == true) ? " updated correctly!" : " couldn't be updated due to error: ")
                     << item.second.second << endl;
            }
            cout << "******************* addDatabase step completed!!!!!!!" << endl;
        }
        else{
            cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
        }

        //TODO
        return true;
    }


    bool CatalogServer :: deleteDatabase (std :: string databaseName, std :: string &errMsg) {

        if (isDatabaseRegistered(databaseName) == false) {
            errMsg = "Database does not exist.\n";
            cout << errMsg << endl;
            return false;
        }

        int catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
        Handle <CatalogDatabaseMetadata> dbMetadataObject = makeObject<CatalogDatabaseMetadata>();
        Handle <Vector<CatalogDatabaseMetadata>> vectorResultItems = makeObject < Vector < CatalogDatabaseMetadata >> ();

        if (pdbCatalog->getMetadataFromCatalog(false, databaseName, vectorResultItems, errMsg, catalogType) == false) {
            errMsg = "Database does not exist.\n";
            cout << errMsg << endl;
            return false;
        }

        if ((*vectorResultItems).size() == 0 ) {
            errMsg = "Database does not exist.\n";
            cout << errMsg << endl;
            return false;
        }
        *dbMetadataObject = (*vectorResultItems)[0];

        // Delete all the sets in the database
        for (int i = 0; i < dbMetadataObject->getListOfSets()->size(); i++) {
            if (!deleteSet(databaseName, (* dbMetadataObject->getListOfSets())[i], errMsg)) {
                errMsg = "Failed to delete set ";
                errMsg += (* dbMetadataObject->getListOfSets())[i].c_str();
            }
        }

        // after it registered the database metadata in the local catalog, iterate over all nodes,
        // make connections and broadcast the objects
        if (isMasterCatalogServer){
            // get the results of each broadcast
            map<string, pair <bool, string>> updateResults;
            errMsg = "";

            Handle<CatDeleteDatabaseRequest> databaseToRemove = makeObject<CatDeleteDatabaseRequest>(databaseName);
            broadcastCatalogDelete (databaseToRemove, updateResults, errMsg);

            for (auto &item : updateResults){
                // adds node info to database metadata
                cout << "DB metadata broadcasted to node IP: " << item.first
                        << ((item.second.first == true) ? " deleted correctly!" : " couldn't be deleted due to error: ")
                        << item.second.second << endl;
            }
        }
        else{
            cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
        }

        return pdbCatalog->deleteMetadataInCatalog( dbMetadataObject, catalogType, errMsg);
    }


    CatalogServer :: ~CatalogServer () {
        pthread_mutex_destroy(&workingMutex);
    }

    CatalogServer :: CatalogServer (std :: string catalogDirectoryIn, bool isMasterCatalogServer, string masterIPValue, int masterPortValue) {
        auto begin = std :: chrono :: high_resolution_clock :: now();

        masterIP = masterIPValue;
        masterPort = masterPortValue;

        //TODO remove hard-coded
    //    string masterPort = "10.134.96.150";
        catalogClientConnectionToMasterCatalogServer = CatalogClient(masterPort, masterIP, make_shared <pdb :: PDBLogger> ("clientCatalogToServerLog"));

        // allocates 64Mb for Catalog related metadata
        //TODO some of these containers will be removed, here just for testing
        //JiaNote: to use temporary allocation block in constructors of server functionalities
        //pdb::makeObjectAllocatorBlock (1024 * 1024 * 128, true);
        const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 128};
        _allNodesInCluster = makeObject<Vector<CatalogNodeMetadata>>();
        _setTypes = makeObject<Vector<CatalogSetMetadata>>();;
        _allDatabases = makeObject<Vector<CatalogDatabaseMetadata>>();
        _udfsValues = makeObject<Vector<CatalogUserTypeMetadata>>();

        // by default this is a non-master catalog server, otherwise call the setIsMasterCatalogServer()
        // method after an instance has been created in order to change this flag.

        this->isMasterCatalogServer = isMasterCatalogServer;

        catalogDirectory = catalogDirectoryIn;
        cout << "Catalog Server ctor is Master Catalog= " << this->isMasterCatalogServer << endl;

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
        if(pdbCatalog->getMetadataFromCatalog(false, emptyString,
                                              _udfsValues,
                                              errMsg, PDBCatalogMsgType::CatalogPDBRegisteredObject) == false)
            cout << errMsg<< endl;

        for (int i=0; i < (*_udfsValues).size(); i++){
            string _typeName = (*_udfsValues)[i].getItemKey().c_str();
            int16_t _typeId = (int16_t)atoi((*_udfsValues)[i].getObjectID().c_str());

            allTypeNames[_typeName] = _typeId;
            allTypeCodes[_typeId] = _typeName;

        }

        // retrieves metadata for databases from sqlite storage and loads them into memory
        if(pdbCatalog->getMetadataFromCatalog(false, emptyString,_allDatabases, errMsg, PDBCatalogMsgType::CatalogPDBDatabase) == false)
            cout << errMsg<< endl;

        // get the list of databases
    //    vector <string> databaseNames;

        for (int i=0; i < (*_allDatabases).size(); i++){

            string _dbName = (*_allDatabases)[i].getItemKey().c_str();

    //        databaseNames.push_back(_dbName);
            for (int j=0; j < (*(*_allDatabases)[i].getListOfSets()).size(); j++){

                string _setName = (*(*_allDatabases)[i].getListOfSets())[j].c_str();
                string _typeName = (*(*_allDatabases)[i].getListOfTypes())[j].c_str();

                cout << "Database " << _dbName << " has set " << _setName << " and type " << _typeName << endl;
                // populates information about databases
    //            allDatabases [_dbName].push_back(_setName);

                // populates information about types and sets for a given database
                cout << "ADDDDDDDing type= " << _typeName << " db= " << _dbName << " _set=" << _setName << " typeId= " << (int16_t)std::atoi(pdbCatalog->getUserDefinedTypesList()[_typeName].getObjectID().c_str()) << endl;
                setTypes [make_pair (_dbName, _setName)] = (int16_t)std::atoi(pdbCatalog->getUserDefinedTypesList()[_typeName].getObjectID().c_str());

            }
        }

        // retrieves metadata for nodes in the cluster from sqlite storage and loads them into memory
        if(pdbCatalog->getMetadataFromCatalog(false, emptyString,_allNodesInCluster,errMsg,PDBCatalogMsgType::CatalogPDBNode) == false)
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
        auto end = std :: chrono :: high_resolution_clock :: now();

        cout << "Catalog Metadata successfully loaded!" << endl;
        std::cout << "--------->Populate CatalogServer Metadata : " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(end-begin).count() << " secs." << std::endl;

    }

    //TODO review and debug/clean these new catalog-related methods
    bool CatalogServer :: printCatalog (string item) {

        pdbCatalog->getModifiedMetadata(item);

        return true;
    }

    bool CatalogServer :: addNodeMetadata (Handle<CatalogNodeMetadata> &nodeMetadata, std :: string &errMsg) {

        // adds the port to the node IP address
        string _nodeIP = nodeMetadata->getNodeIP().c_str();
    string nodeAddress = _nodeIP + ":" + to_string(nodeMetadata->getNodePort());

        // don't add a node that is already registered
    //    if(std::find(allNodesInCluster.begin(), allNodesInCluster.end(), nodAddress) != allNodesInCluster.end()){
    //JIANOTE: We need to support running multiple instances with different ports on the same Ip
    if (isNodeRegistered(nodeAddress) == true) {
        errMsg = "NodeAddress " + nodeAddress + " is already registered.\n";
            return false;
        }


        // add the node info to container
    allNodesInCluster.push_back (nodeAddress);

        int metadataCategory = PDBCatalogMsgType::CatalogPDBNode;
        Handle<CatalogNodeMetadata> metadataObject = makeObject<CatalogNodeMetadata>();
        CatalogStandardNodeMetadata metadataItem = CatalogStandardNodeMetadata();


        *metadataObject = *nodeMetadata;

        pdbCatalog->addMetadataToCatalog(metadataObject, metadataItem, metadataCategory, errMsg);

        // after it registered the node in the local catalog, iterate over all nodes,
        // make connections and broadcast the objects
        if (isMasterCatalogServer){
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
    //    if(allDatabases.find(dbName) != allDatabases.end()){
        if (isDatabaseRegistered(dbName) == true){
            errMsg = "Db name: " + dbName + " is already registered.\n";
            return false;
        }

        vector<string> sets;
        // add the node info to container
    //    allDatabases.insert (make_pair(dbName, sets));

        // temporary allocator
        //const UseTemporaryAllocationBlock tempBlock{4096};

        int metadataCategory = PDBCatalogMsgType::CatalogPDBDatabase;
        Handle<CatalogDatabaseMetadata> metadataObject = makeObject<CatalogDatabaseMetadata>();

        CatalogStandardDatabaseMetadata metadataItem = CatalogStandardDatabaseMetadata();

        *metadataObject = *dbMetadata;

        pdbCatalog->addMetadataToCatalog(metadataObject, metadataItem, metadataCategory, errMsg);

        // after it registered the node in the local catalog, iterate over all nodes,
        // make connections and broadcast the objects
        if (isMasterCatalogServer){
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

    bool CatalogServer :: updateDatabaseMetadata (Handle<CatalogDatabaseMetadata> &dbMetadata, std :: string &errMsg) {

        // adds the port to the node IP address
        string dbName = dbMetadata->getItemName().c_str();

        vector<string> sets;

        int metadataCategory = PDBCatalogMsgType::CatalogPDBDatabase;
        Handle<CatalogDatabaseMetadata> metadataObject = makeObject<CatalogDatabaseMetadata>();
        *metadataObject = *dbMetadata;

        pdbCatalog->updateMetadataInCatalog(metadataObject, metadataCategory, errMsg);

        // after it registered the node in the local catalog, iterate over all nodes,
        // make connections and broadcast the objects
        if (isMasterCatalogServer){
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

    bool CatalogServer :: addSetMetadata (Handle<CatalogSetMetadata> &setMetadata, std :: string &errMsg) {
        auto begin = std :: chrono :: high_resolution_clock :: now();

        // gets the set name
        string setName = string(setMetadata->getItemName().c_str());
        // gets the database name
        string dbName = string(setMetadata->getDBName().c_str());
        // gets the type Id
        int16_t typeId = (int16_t)atoi((*setMetadata).getObjectTypeId().c_str());

        // don't add a set that is already registered
    //    if(allDatabases.find(setName) != allDatabases.end()){
        if (isSetRegistered(dbName, setName) == true){
            errMsg = "Set name: " + dbName + "." + setName + " is already registered.\n";
            return false;
        }

        // add the node info to container
        // TODO get the values from the catalog instead!!!
        // change the 1 in the last param
        cout << "inserting set-----------------> dbName= " << dbName << " setName " << setName << " id " << typeId <<endl;
        setTypes.insert (make_pair(make_pair(dbName, setName), typeId));

        int metadataCategory = PDBCatalogMsgType::CatalogPDBSet;
        Handle<CatalogSetMetadata> metadataObject = makeObject<CatalogSetMetadata>();
        CatalogStandardSetMetadata metadataItem = CatalogStandardSetMetadata();

        *metadataObject = *setMetadata;

        auto beforeaddMetadataToCatalog = std :: chrono :: high_resolution_clock :: now();

        cout << "Adding set metadata for set " << setName << endl;
        pdbCatalog->addMetadataToCatalog(metadataObject, metadataItem, metadataCategory, errMsg);

        auto afteraddMetadataToCatalog = std :: chrono :: high_resolution_clock :: now();
        auto beforebroadcastCatalogUpdate = afteraddMetadataToCatalog;
        auto afterbroadcastCatalogUpdate = afteraddMetadataToCatalog;
        // after it registered the set in the local catalog, iterate over all nodes,
        // make connections and broadcast the objects
        if (isMasterCatalogServer){
            // get the results of each broadcast
            map<string, pair <bool, string>> updateResults;
            errMsg = "";
            beforebroadcastCatalogUpdate = std :: chrono :: high_resolution_clock :: now();

            broadcastCatalogUpdate (metadataObject, updateResults, errMsg);
            afterbroadcastCatalogUpdate = std :: chrono :: high_resolution_clock :: now();

            for (auto &item : updateResults){
                cout << "Node IP: " << item.first << ((item.second.first == true) ? " updated correctly!" : " couldn't be updated due to error: ")
                     << item.second.second << endl;
            }
        } else {
            cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
        }
        auto beforeReturn = std :: chrono :: high_resolution_clock :: now();

        std::cout << "Time Duration for check and copy:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(beforeaddMetadataToCatalog-begin).count() << " secs." << std::endl;
        std::cout << "Time Duration for adding metadata to catalog:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(afteraddMetadataToCatalog - beforeaddMetadataToCatalog).count() << " secs." << std::endl;
        std::cout << "Time Duration for check isMasterCatalog:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(beforebroadcastCatalogUpdate - afteraddMetadataToCatalog).count() << " secs." << std::endl;
        std::cout << "Time Duration for broadcast update:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(afterbroadcastCatalogUpdate - beforebroadcastCatalogUpdate).count() << " secs." << std::endl;
        std::cout << "Time Duration before return:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(beforeReturn - afterbroadcastCatalogUpdate).count() << " secs." << std::endl;
        std::cout << "------>Time Duration to Complete addSetMetadata\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(beforeReturn - begin).count() << " secs." << std::endl;

        //TODO
        return true;
    }

    //TODO implement these methods
    // Adds the IP of a node to a given set
    bool CatalogServer :: addNodeToSet (std :: string nodeIP, std :: string databaseName, std :: string setName, std :: string &errMsg){
        auto begin = std :: chrono :: high_resolution_clock :: now();

        // make sure we are only adding to an existing database
    //    if (allDatabases.count (databaseName) == 0) {
        if (isDatabaseRegistered(databaseName) == false){
            errMsg = "Database does not exist.\n";
            return false;
        }

        if (isSetRegistered(databaseName, setName) == false){
            errMsg = "Set does not exists.\n";
            return false;
        }

        auto afterRegisteredCheck = std :: chrono :: high_resolution_clock :: now();


    //    vector <string> &setList = allDatabases[databaseName];
    //
    //    // make sure that set does not exist
    //    for (string s : setList) {
    //        if (s == setName) {
    //            errMsg = "Set already exists.\n";
    //            return false;
    //        }
    //    }

        cout << "...... Calling CatalogServer :: addNodeToSet" << endl;

        // prepares data for the DB metadata
        int catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
        Handle<CatalogDatabaseMetadata> dbMetadataObject = makeObject<CatalogDatabaseMetadata>();

        Handle<Vector<CatalogDatabaseMetadata>> vectorResultItems = makeObject<Vector<CatalogDatabaseMetadata>>();

        if(pdbCatalog->getMetadataFromCatalog(false, databaseName,vectorResultItems,errMsg,catalogType) == false)
            cout << errMsg<< endl;

        for (int i=0; i < (*vectorResultItems).size(); i++){
            if ((*vectorResultItems)[i].getItemKey().c_str()==databaseName) *dbMetadataObject = (*vectorResultItems)[i];
        }

        String setNameCatalog(setName);
        String nodeID(nodeIP);

        (*dbMetadataObject).addSetToMap(setNameCatalog, nodeID);

        // updates the corresponding database metadata
        catalogType = PDBCatalogMsgType::CatalogPDBDatabase;

        auto afteraddSetToMap = std :: chrono :: high_resolution_clock :: now();

        // if database exists update its metadata
        if (isDatabaseRegistered(databaseName) == true){
            cout << ".......... Invoking updateMetadataInCatalog key: " << databaseName << endl;
           if (pdbCatalog->updateMetadataInCatalog(dbMetadataObject, catalogType, errMsg)==true) cout << "DB Update Set metadata OK" << endl;
           else{
               cout << "DB Update metadata Set Error: " << errMsg << endl;
               return false;
           }

        }else{
            return false;
        }

        auto afterUpdateDB = std :: chrono :: high_resolution_clock :: now();

        // after it registered the database metadata in the local catalog, iterate over all nodes,
        // make connections and broadcast the objects
        if (isMasterCatalogServer){
            cout << "About to broadcast addition of node to set in the cluster: " << endl;

            // map to capture the results of broadcasting the DB update
            map<string, pair <bool, string>> updateSetResults;

            // second, broadcasts the metadata of the DB to which this set has been added,
            // updating all local copies of the catalog in the cluster
            if (broadcastCatalogUpdate (dbMetadataObject, updateSetResults, errMsg)){
                cout << " Broadcasting DB updated Ok. " << endl;
            } else {
                cout << " Error broadcasting DB update." << endl;
            }
            for (auto &item : updateSetResults){
                cout << "Node IP: " << item.first << ((item.second.first == true) ? " updated correctly!" : " couldn't be updated due to error: ")
                     << item.second.second << endl;
            }

        }
        else{
            cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
        }

        auto afterBroadcast = std :: chrono :: high_resolution_clock :: now();


        // TODO, remove it, just used for debugging
    //    printCatalog("");

        auto addNodeToSet = std :: chrono :: high_resolution_clock :: now();

        std::cout << "Time Duration for check registration:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(afterRegisteredCheck-begin).count() << " secs." << std::endl;
        std::cout << "Time Duration for adding set to map:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(afteraddSetToMap - afterRegisteredCheck).count() << " secs." << std::endl;
        std::cout << "Time Duration for updating db Metadata:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(afterUpdateDB - afteraddSetToMap).count() << " secs." << std::endl;
        std::cout << "Time Duration for broadcast update:\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(afterBroadcast - afterUpdateDB).count() << " secs." << std::endl;
        std::cout << "------>Time Duration to Complete addNodeToSet\t " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(addNodeToSet - begin).count() << " secs." << std::endl;

        std::cout << std::endl;

        return true;
    }

    bool CatalogServer :: addNodeToDB (std :: string nodeIP, std :: string databaseName, std :: string &errMsg){
        // make sure we are only adding to an existing database
    //    if (allDatabases.count (databaseName) == 0) {
        if (isDatabaseRegistered(databaseName) == false){
            errMsg = "Database does not exist.\n";
            return false;
        }

        cout << "...... Calling CatalogServer :: addNodeToSet" << endl;

        // prepares data for the DB metadata
        int catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
        Handle<CatalogDatabaseMetadata> dbMetadataObject = makeObject<CatalogDatabaseMetadata>();

        Handle<Vector<CatalogDatabaseMetadata>> vectorResultItems = makeObject<Vector<CatalogDatabaseMetadata>>();

        if(pdbCatalog->getMetadataFromCatalog(false, databaseName,vectorResultItems,errMsg,catalogType) == false)
            cout << errMsg<< endl;

        for (int i=0; i < (*vectorResultItems).size(); i++){
            if ((*vectorResultItems)[i].getItemKey().c_str()==databaseName) *dbMetadataObject = (*vectorResultItems)[i];
        }

        String nodeID(nodeIP);
        String dbName(databaseName);
        (*dbMetadataObject).addNodeToMap(nodeID, dbName);

        // updates the corresponding database metadata
        catalogType = PDBCatalogMsgType::CatalogPDBDatabase;

        // if database exists update its metadata
        if (isDatabaseRegistered(databaseName) == true){
            cout << ".......... Invoking updateMetadataInCatalog key: " << databaseName << endl;
           if (pdbCatalog->updateMetadataInCatalog(dbMetadataObject, catalogType, errMsg)==true) cout << "DB Update Set metadata OK" << endl;
           else{
               cout << "DB Update metadata Set Error: " << errMsg << endl;
               return false;
           }

        }else{
            return false;
        }

        // after it registered the database metadata in the local catalog, iterate over all nodes,
        // make connections and broadcast the objects
        if (isMasterCatalogServer){
            cout << "About to broadcast addition of node to set in the cluster: " << endl;

            // map to capture the results of broadcasting the DB update
            map<string, pair <bool, string>> updateSetResults;

            // second, broadcasts the metadata of the DB to which this set has been added,
            // updating all local copies of the catalog in the cluster
            if (broadcastCatalogUpdate (dbMetadataObject, updateSetResults, errMsg)){
                cout << " Broadcasting DB updated Ok. " << endl;
            } else {
                cout << " Error broadcasting DB update." << endl;
            }
            for (auto &item : updateSetResults){
                cout << "Node IP: " << item.first << ((item.second.first == true) ? " updated correctly!" : " couldn't be updated due to error: ")
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

    bool CatalogServer :: removeNodeFromSet (std :: string nodeIP, std :: string databaseName, std :: string setName, std :: string &errMsg){

        // TODO: Under construction, do not use

        string setUniqueId = databaseName + "." + setName;

        if (!isDatabaseRegistered(databaseName)){
            errMsg = "Database does not exist.\n";
            cout << errMsg << endl;
            return false;
        }

        // make sure that set exists
        if (!isSetRegistered(databaseName, setName)){
            errMsg = "Set doesn't exist.\n";
            cout << errMsg << endl;
            return false;
        }

        int catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
        Handle<CatalogDatabaseMetadata> dbMetadataObject = makeObject<CatalogDatabaseMetadata>();

        Handle<Vector<CatalogDatabaseMetadata>> vectorResultItems = makeObject<Vector<CatalogDatabaseMetadata>>();

        if(!pdbCatalog->getMetadataFromCatalog(false, databaseName,vectorResultItems,errMsg,catalogType)) {
            cout << errMsg << endl;
            return false;
        }

        if (vectorResultItems->size() != 1) {
            errMsg = "Could not find database " + databaseName;
            cout << errMsg << endl;
            return false;
        }

        *dbMetadataObject = (*vectorResultItems)[0];

        dbMetadataObject->removeNodeFromSet(nodeIP, setName);

        if (pdbCatalog->updateMetadataInCatalog(dbMetadataObject, catalogType, errMsg)) {
            cout << "DB Update Set metadata OK" << endl;
        } else{
            cout << "DB Update metadata Set Error: " << errMsg << endl;
            return false;
        }
        // after it registered the database metadata in the local catalog, iterate over all nodes,
        // make connections and broadcast the objects
        if (isMasterCatalogServer){
            // map to capture the results of broadcasting the Set insertion
            map<string, pair <bool, string>> updateResults;

            // first, broadcasts the metadata of the new set to all local copies of the catalog
            // in the cluster, inserting the new item
            broadcastCatalogUpdate (dbMetadataObject, updateResults, errMsg);

            for (auto &item : updateResults){
                cout << "Set Metadata broadcasted to node IP: " << item.first <<
                        (item.second.first ? " updated correctly!" : " couldn't be updated due to error: ")
                        << item.second.second << endl;
            }

            // map to capture the results of broadcasting the DB update
            map<string, pair <bool, string>> updateSetResults;

            // second, broadcasts the metadata of the DB to which this set has been added,
            // updating all local copies of the catalog in the cluster
            broadcastCatalogUpdate (dbMetadataObject, updateSetResults, errMsg);

            for (auto &item : updateSetResults){
                cout << "DB Metadata broadcasted to node IP: " << item.first <<
                        (item.second.first ? " updated correctly!" : " couldn't be updated due to error: ")
                        << item.second.second << endl;
            }
        } else{
            cout << "This is not Master Catalog Node, thus metadata was only registered locally!" << endl;
        }
        return true;
    }

    bool CatalogServer :: removeNodeFromDB (std :: string nodeIP, std :: string databaseName, std :: string &errMsg){
        errMsg = "Remove node from db not implemented";
        return false;
    }

    template <class Type>
    bool CatalogServer :: broadcastCatalogUpdate (Handle<Type> metadataToSend,
                                                  map <string, pair<bool, string>> &broadcastResults,
                                                  string &errMsg) {
        PDBLoggerPtr catalogLogger = make_shared<PDBLogger>("distCatalogLogger");

        auto beforeLoop = std :: chrono :: high_resolution_clock :: now();

        for (auto &item : pdbCatalog->getListOfNodesInCluster()){

            string nodeAddress = string(item.second.getNodeIP().c_str()) + ":" + to_string(item.second.getNodePort());
            string nodeIP = item.second.getNodeIP().c_str();
            int nodePort = item.second.getNodePort();
            bool res = false;

            auto beforeConn = std :: chrono :: high_resolution_clock :: now();

            CatalogClient clusterCatalogClient = CatalogClient(nodePort, nodeIP, catalogLogger);

            auto afterConn = std :: chrono :: high_resolution_clock :: now();

            std::cout << "Time Duration for create catalog client:\t " <<
                std::chrono::duration_cast<std::chrono::duration<float>>(afterConn-beforeConn).count() << " secs." << std::endl;

            //TODO new mechanism for identifying the master node not based on the name!
            if (string(item.second.getNodeType().c_str()).compare("master") !=0){

                auto beforeRegGeneric = std :: chrono :: high_resolution_clock :: now();

                // sends the request to a node in the cluster
                res = clusterCatalogClient.registerGenericMetadata (metadataToSend, errMsg);

                auto afterRegGeneric = std :: chrono :: high_resolution_clock :: now();

                std::cout << "Time Duration for registerGenericMetadata call " << " to node " << nodeAddress << " \t" <<
                    std::chrono::duration_cast<std::chrono::duration<float>>(afterRegGeneric-beforeRegGeneric).count() << " secs." << std::endl;

                // adds the result of the update
                broadcastResults.insert(make_pair(nodeIP, make_pair(res , errMsg)));

            }

        }
        auto afterLoop = std :: chrono :: high_resolution_clock :: now();
        std::cout << "------>Time Duration to complete broadcastCatalogUpdate\t" << " " <<
            std::chrono::duration_cast<std::chrono::duration<float>>(afterLoop-beforeLoop).count() << " secs." << std::endl;

        return true;
    }

} // namespace

template <class Type>
bool CatalogServer :: broadcastCatalogDelete (Handle<Type> metadataToSend,
                                              map <string, pair<bool, string>> &broadcastResults,
                                              string &errMsg) {
    PDBLoggerPtr catalogLogger = make_shared<PDBLogger>("distCatalogLogger");

    for (auto &item : pdbCatalog->getListOfNodesInCluster()){

        string nodeAddress = string(item.second.getNodeIP().c_str()) + ":" + to_string(item.second.getNodePort());
        string nodeIP = item.second.getNodeIP().c_str();
        int nodePort = item.second.getNodePort();
        bool res = false;

        CatalogClient clusterCatalogClient = CatalogClient(nodePort, nodeIP, catalogLogger);

        //TODO new mechanism for identifying the master node not based on the name!
        if (string(item.second.getNodeType().c_str()).compare("master") !=0){

            // sends the request to a node in the cluster
            res = clusterCatalogClient.deleteGenericMetadata (metadataToSend, errMsg);

            // adds the result of the update
            broadcastResults.insert(make_pair(nodeIP, make_pair(res , errMsg)));

        } else {

            cout << "Don't broadcast to " << nodeAddress << " because it has the master catalog." << endl;

        }

    }

    return true;
}

bool CatalogServer :: retrieveUserDefinedTypeMetadata(string typeName, Handle<CatalogUserTypeMetadata> &itemMetadata, string &soFileBytes, string &errMsg){

    string returnedBytes;
    // TODO this is needed to differentiate between data_types and metrics
    string typeOfObject = "data_types";

    bool res = pdbCatalog->retrievesDynamicLibrary(typeName,
                                               typeOfObject,
                                               itemMetadata,
                                               returnedBytes,
                                               soFileBytes,
                                               errMsg);

    return res;
}


bool CatalogServer :: isDatabaseRegistered(string dbName){
    int catalogType = PDBCatalogMsgType::CatalogPDBDatabase;
    string result("");

    return pdbCatalog->keyIsFound(catalogType, dbName, result);
}

bool CatalogServer :: isSetRegistered(string dbName, string setName){
    int catalogType = PDBCatalogMsgType::CatalogPDBSet;
    string result("");
    string setUniqueId = dbName + "." + setName;
    return pdbCatalog->keyIsFound(catalogType, setUniqueId, result);

}

bool CatalogServer :: isNodeRegistered(string nodeIP){
    int catalogType = PDBCatalogMsgType::CatalogPDBNode;
    string result("");

    return pdbCatalog->keyIsFound(catalogType, nodeIP, result);
}


bool CatalogServer :: getIsMasterCatalogServer(){
    return isMasterCatalogServer;
}

void CatalogServer :: setIsMasterCatalogServer(bool isMasterCatalogServerIn){
    isMasterCatalogServer = isMasterCatalogServerIn;
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
