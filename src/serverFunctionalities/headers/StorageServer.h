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

#ifndef STORAGE_SERVER_H
#define STORAGE_SERVER_H

#include "ServerFunctionality.h"
#include "PDBServer.h"
#include "Record.h"
#include <vector>
#include "PDBVector.h"
#include "MyDB_BufferManager.h"

namespace pdb {

class StorageServer : public ServerFunctionality {

public:

	// creates a storage server, putting all data in the indicated directory.
	// The page size and the number of buffer pages is as indicated.
	StorageServer (std :: string storageDir, size_t pageSize, size_t numPages);

	// takes all of the currently buffered records for the given database/set pair,
	// creates at most one page of data with them, and then writes that page to storage
	void writeBackRecords (pair <std :: string, std :: string> databaseAndSet) ;

	// this allocates a new page at the end of the indicated database/set combo
	MyDB_PageHandle getNewPage (pair <std :: string, std :: string> databaseAndSet);

	// gets the specified page; returns a nullptr if we have asked for a page past the
	// end of the file
	MyDB_PageHandle getPage (pair <std :: string, std :: string> databaseAndSet, size_t pageNum);

	// returns a table object referencing the given database/set pair
	MyDB_TablePtr getTable (pair <std :: string, std :: string> databaseAndSet);

	// from the ServerFunctionality interface... registers the StorageServer's 
	// single handler, which accepts a vector of records and stores it
	void registerHandlers (PDBServer &forMe) override;

	// stores a record---we'll keep buffering records until we get enough of them that
	// we can put them together into a page.  The return value is the total size of
	// all of the records that we are buffering for this database and set
	size_t bufferRecord (pair <std :: string, std :: string> databaseAndSet, Record <Vector <Handle <Object>>> *addMe);

	// gets access to the buffer manager, which is the interface through which all of 
	// the data storage and access is performed
	MyDB_BufferManagerPtr getBufferManager ();
	
	// deletes a set from disk
	bool deleteSet (pair <std :: string, std :: string> databaseAndSet);

	// destructor
	~StorageServer ();

private:

	// this stores the set of all records that we are buffering
	std :: map <pair <std :: string, std :: string>, std :: vector <Record <Vector <Handle <Object>>> *>> allRecords;

	// this stores the total sizes of all lists of records that we are buffering
	std :: map <pair <std :: string, std :: string>, size_t> sizes;

	// this is the set of table pointers
	std :: map <pair <std :: string, std :: string>, MyDB_TablePtr> allTables;

	// allows us to access the data
	MyDB_BufferManagerPtr myBufferMgr;

	// the directory where data are stored
	std :: string storageDir;
};

}

#endif
