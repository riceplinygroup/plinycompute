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
/*
 * File:   DataProxy.h
 * Author: Jia
 *
 * Created on November 28, 2015, 11:29 PM
 */

#ifndef DATAPROXY_H
#define DATAPROXY_H

#include "DataTypes.h"
#include "PDBPage.h"
#include "PDBCommunicator.h"
#include "SharedMem.h"
#include "PageScanner.h"

#include <memory>
using namespace std;
class DataProxy;
typedef shared_ptr<DataProxy> DataProxyPtr;

/*
 * The DataProxy class will use a BackEndCommunicator instance to interact with Storage to do all
 *stuffs like
 * addTempSet, removeTempSet, addTempPage, pinTempPage, unpinTempPage, pinUserPage, unpinUserPage,
 *getScanner, and closeCleaner.
 * Because multiple threads can not share one communicator, a data proxy instance can only be
 *owned/accessed by one thread.
 **/
class DataProxy {
public:
    DataProxy(NodeID nodeId,
              pdb::PDBCommunicatorPtr communicator,
              SharedMemPtr shm,
              pdb::PDBLoggerPtr logger);
    ~DataProxy();

    /**
     * Add a temporary set to store intermediate data with setName specified.
     * The storage system will allocate SetID to the temporary set.
     * If successful, return true, otherwise (like setName exists), return false.
     */
    bool addTempSet(string setName, SetID& setId, bool needMem = true, int numTries = 0);

    /**
     * Remove a temp set with the specified SetID.
     * If successful, return true, otherwise (like setId doesn't exists), return false.
     */
    bool removeTempSet(SetID setId, bool needMem = true, int numTries = 0);

    /**
     * Add a page to the temporary set specified by the given SetID.
     * The storage system will allocate PageID to the page.
     * If successful, return true, otherwise (like setId doesn't exists), return false.
     * Immediately, the page will be pinned at storage, and is ready for read/write in memory.
     */
    bool addTempPage(SetID setId, PDBPagePtr& page, bool needMem = true, int numTries = 0);

    /**
     * Add a page to the set specified by the given DbId, TypeId, and SetID.
     * The storage system will allocate PageID to the page.
     * If successful, return true, otherwise (like setId doesn't exists), return false.
     * Immediately, the page will be pinned at storage, and is ready for read/write in memory.
     */
    bool addUserPage(DatabaseID dbId,
                     UserTypeID typeId,
                     SetID setId,
                     PDBPagePtr& page,
                     bool needMem = true,
                     int numTries = 0);

    /**
     * Pin a page in the temporary set specified by the given SetID and PageID to the storage,
     * so that it can get ready for read/write in memory.
     * If successful, return true, otherwise (like setId or pageId does not exist), return false.
     */
    bool pinTempPage(
        SetID setId, PageID pageId, PDBPagePtr& page, bool needMem = true, int numTries = 0);

    /**
     * UnPin a page in the temporary set specified by the given SetID and PageID from memory,
     * so that it can be flushed to disk if it is in input buffer, or can be evicted if it is in
     * cache.
     * If successful, return true, otherwise (like setId or pageId does not exist), return false.
     */
    bool unpinTempPage(SetID setId, PDBPagePtr page, bool needMem = true, int numTries = 0);

    /**
     * Pin a page in the user set specified by the given DatabaseID, UserTypeID, SetID and PageID to
     * the storage,
     * so that it can get ready for read/write in memory.
     * If successful, return true, otherwise (like dbId, typeId, setId or pageId does not exist),
     * return false.
     */
    bool pinUserPage(NodeID nodeId,
                     DatabaseID dbId,
                     UserTypeID typeId,
                     SetID setId,
                     PageID pageId,
                     PDBPagePtr& page,
                     bool needMem = true,
                     int numTries = 0);

    /**
     * Copy bytes to a page in localset, it returns true if successful.
     */
    bool pinBytes(DatabaseID dbId,
                  UserTypeID typeId,
                  SetID setId,
                  size_t sizeOfBytes,
                  void* bytes,
                  bool needMem = true,
                  int numTries = 0);


    /**
     * UnPin a page in the user set specified by the given DatabaseID, UserTypeID, SetID and PageID
     * from memory,
     * so that it can be flushed to disk if it is in input buffer, or can be evicted if it is in
     * cache.
     * If successful, return true, otherwise (like dbId, typeId, setId or pageId does not exist),
     * return false.
     */
    bool unpinUserPage(NodeID nodeId,
                       DatabaseID dbId,
                       UserTypeID typeId,
                       SetID setId,
                       PDBPagePtr page,
                       bool needMem = true,
                       int numTries = 0);

    /**
     * Create a PageScanner instance given the specified thread number.
     * Return a smart pointer pointing at the created PageScanner instance.
     */
    PageScannerPtr getScanner(int numThreads);


private:
    pdb::PDBCommunicatorPtr communicator;
    SharedMemPtr shm;
    pdb::PDBLoggerPtr logger;
    NodeID nodeId;
};
#endif /* DATAPROXY_H */
