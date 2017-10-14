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
 * VTableMap.h
 *
 *  Created on: Dec 11, 2015
 *      Author: carlos
 */

#include "Allocator.h"

#ifndef PDBCATALOG_VTABLEMAP_H_
#define PDBCATALOG_VTABLEMAP_H_

#include <iostream>
#include "PDBLogger.h"
#include <string>
#include <vector>
#include <map>
#include <algorithm>

// this is the special type ID used for any type that the system does not know about
#define TYPE_NOT_RECOGNIZED 8191

namespace pdb {

class CatalogClient;

/**
 *   VTableMap is a helper class for maintaining information about registered
 *   user-defined types and metrics in an instance of PDB. Entries are
 *   stored as VTableMapEntry objects in two std::maps 1) indexed by objectID,
 *   and 2) indexed by objectName.
 *   Clients of this class will access this information using a handler to
 *   the catalog.
 */

class VTableMap {

public:
    // constructor/destructor
    VTableMap();
    ~VTableMap();

    static void setLogger(PDBLoggerPtr myLoggerIn);

    // Returns the type ID of a user-defined object, given the object name
    static int16_t getIDByName(std::string objectName, bool withLock = true);

    // returns the vTablePtr for the corresponding tpye identifier (or a nullptr if the
    // type ID was not recognized)
    static void* getVTablePtr(int16_t objectTypeID);

    // look up the vtable using the given catalog
    static void* getVTablePtrUsingCatalog(int16_t objectTypeID);

    // print out the contents of the vTableMap
    static void listVtableEntries();
    static void listVtableLabels();

    // use the catalog to look up the type ID for an object
    static int16_t lookupTypeNameInCatalog(std::string objectTypeName);

    // use to lookup a built-in type... return a -1 if not found
    static int16_t lookupBuiltInType(std::string objectTypeName);

    // use to lookup a built-in type... return a "" if not found
    static std::string lookupBuiltInType(int16_t);

    // returns the number of built-in objects
    static int totalBuiltInObjects();

    // sets the catalog client for the vtable
    static void setCatalogClient(CatalogClient* catalog);

    // gets the catalog client for the vtable
    static CatalogClient* getCatalogClient();

private:
    // a map containing the typeIDs indexed by object name---a -1 for the typeID means that
    // we have previously looked for the ID and not been able to find it
    std::map<std::string, int16_t> objectTypeNamesList;

    // the list of all registered object types; the position in the list implies the type ID
    std::vector<void*> allVTables;

    // this is a pointer to the catalog client that we are using to access the catalog
    CatalogClient* catalog;

    // and a pointer to the logger
    PDBLoggerPtr logger;

    // so that we are thread safe
    pthread_mutex_t myLock;

    // holds all of the so handles
    std::vector<void*> so_handles;
};

extern VTableMap* theVTable;

} /* namespace pdb */

#include "VTableMap.cc"

#endif /* PDBCATALOG_VTABLEMAP_H_ */
