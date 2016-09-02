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

#ifndef STORAGE_SERVER_C
#define STORAGE_SERVER_C

#include "StorageServer.h"
#include "SimpleRequestResult.h"
#include "CatalogServer.h"
#include "StorageAddData.h"
#include "SimpleRequestHandler.h"
#include "Record.h"
#include "InterfaceFunctions.h"

namespace pdb {

size_t StorageServer :: bufferRecord (pair <std :: string, std :: string> databaseAndSet, Record <Vector <Handle <Object>>> *addMe) {
	if (allRecords.count (databaseAndSet) == 0) {
		std :: vector <Record <Vector <Handle <Object>>> *> records;
		records.push_back (addMe);
		allRecords[databaseAndSet] = records;
		sizes[databaseAndSet] = addMe->numBytes ();
	} else {
		allRecords[databaseAndSet].push_back (addMe);
		sizes[databaseAndSet] += addMe->numBytes ();
	} 
	return sizes[databaseAndSet];
}

MyDB_BufferManagerPtr StorageServer :: getBufferManager () {
	return myBufferMgr;
}

StorageServer :: StorageServer (std :: string storageDirIn, size_t pageSize, size_t numPages) {
	storageDir = storageDirIn;
	myBufferMgr = make_shared <MyDB_BufferManager> (pageSize, numPages, storageDir + "/tempFile");	
}

MyDB_TablePtr StorageServer :: getTable (pair <std :: string, std :: string> databaseAndSet) {

	if (allTables.count (databaseAndSet) == 0) {
		allTables[databaseAndSet] = make_shared <MyDB_Table> (databaseAndSet.first + "." + databaseAndSet.second, storageDir + "/" 
			+ databaseAndSet.first + "." + databaseAndSet.second);
	}

	return allTables[databaseAndSet];
}

StorageServer :: ~StorageServer () {

	for (auto &a : allRecords) {
		while (a.second.size () > 0)
			writeBackRecords (a.first);
	}
}

MyDB_PageHandle StorageServer :: getNewPage (pair <std :: string, std :: string> databaseAndSet) {

	// and get that page
	size_t pageNum = getFunctionality <CatalogServer> ().getNewPage (databaseAndSet.first, databaseAndSet.second);
	MyDB_TablePtr whichTable = getTable (databaseAndSet);
	return getBufferManager ()->getPinnedPage (whichTable, pageNum);
}

void StorageServer :: writeBackRecords (pair <std :: string, std :: string> databaseAndSet) {

	// get all of the records
	auto &allRecs = allRecords[databaseAndSet];

	// now, get a page to write to
	MyDB_PageHandle myPage = getNewPage (databaseAndSet);
						
	// now, copy everything over... do all allocations on the page
	MyDB_BufferManagerPtr myMgr = getBufferManager ();
	makeObjectAllocatorBlock (myPage->getBytes (), myMgr->getPageSize (), true);
	Handle <Vector <Handle <Object>>> data = makeObject <Vector <Handle <Object>>> ();

	// and move everything to the page
	while (allRecs.size () > 0) {
		auto &allObjects = *(allRecs[allRecs.size () - 1]->getRootObject ());
		int numObjectsInRecord = allObjects.size ();

		int pos = 0;
		try {
			for (pos = 0; pos < numObjectsInRecord; pos++)
				data->push_back (allObjects[pos]);

		} catch (NotEnoughSpace &n) {
						
			// put the extra objects tht we could not store back in the record
			void *myRAM = malloc (allRecs[allRecs.size () - 1]->numBytes ());
			makeObjectAllocatorBlock (myRAM, allRecs[allRecs.size () - 1]->numBytes (), true);
			Handle <Vector <Handle <Object>>> extraData = makeObject <Vector <Handle <Object>>> (allObjects.size () - pos);
			for (; pos < numObjectsInRecord; pos++) 
				extraData->push_back (allObjects[pos]);	

			// put the record back
			free (allRecs[allRecs.size () - 1]);
			allRecs[allRecs.size () - 1] = getRecord (extraData);	
			break;
		}

		// now kill this record
		free (allRecs[allRecs.size () - 1]);
		allRecs.pop_back ();
	}

	// now restore the object allocator
	makeObjectAllocatorBlock (PDBWorkerQueue :: defaultAllocatorBlockSize, true);

	// and now write the page to disk
	myPage->wroteBytes ();
	myPage->flush ();
	myPage->unpin ();

	// and rec-compute the total size of the buffered records
	sizes[databaseAndSet] = 0;
	for (auto *a : allRecs) {
		sizes[databaseAndSet] += a->numBytes ();
	}
	
}

void StorageServer :: registerHandlers (PDBServer &forMe) {

	// this handler accepts a request to store some data
	forMe.registerHandler (StorageAddData_TYPEID, make_shared <SimpleRequestHandler <StorageAddData>> (
		[&] (Handle <StorageAddData> request, PDBCommunicatorPtr sendUsingMe) {

			// first, check with the catalog to make sure that the given database, set, and type are correct
			int16_t typeID = getFunctionality <CatalogServer> ().getObjectType (request->getDatabase (), request->getSetName ());
			std :: string errMsg;
			bool everythingOK = true;
			if (typeID >= 0) {

				// if we made it here, the type is correct, as is the database and the set
				if (typeID == getFunctionality <CatalogServer> ().searchForObjectTypeName (request->getType ())) {
					
					// get the record
					size_t numBytes = sendUsingMe->getSizeOfNextObject ();
					void *readToHere = malloc (numBytes);
					Handle <Vector <Handle <Object>>> myData = 
						sendUsingMe->getNextObject <Vector <Handle <Object>>> (readToHere, everythingOK, errMsg);

					if (everythingOK) {	
						Record <Vector <Handle <Object>>> *ramForRecord = getRecord (myData);
					
						// at this point, we have performed the serialization, so remember the record
						auto databaseAndSet = make_pair ((std :: string) request->getDatabase (),
        	                                        (std :: string) request->getSetName ());
						getFunctionality <StorageServer> ().bufferRecord (databaseAndSet, ramForRecord); 
	
						// if we have enough space to fill up a page, do it
						size_t limit = getFunctionality <StorageServer> ().getBufferManager ()->getPageSize ();
						while (sizes[databaseAndSet] > limit) {
							getFunctionality <StorageServer> ().writeBackRecords (databaseAndSet);
						}
					}
				} else {
					errMsg = "Tried to add data of the wrong type to a database set.\n";
					everythingOK = false;
				}
			} else {
				errMsg = "Tried to add data to a set/database combination that does not exist.\n";
				everythingOK = false;
			}

			Handle <SimpleRequestResult> response = makeObjectOnTempAllocatorBlock <SimpleRequestResult> (1024, everythingOK, errMsg);

                        // return the result
                        bool res = sendUsingMe->sendObject (response, errMsg);
                        return make_pair (res, errMsg);
		}
	));
}

}

#endif
