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
#include "UseTemporaryAllocationBlock.h"
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
	size_t pageSize = myMgr->getPageSize ();

	// the position in the output vector
	int pos = 0;
	
	// the number of items in the current record we are processing
	int numObjectsInRecord;

	// the current size (in bytes) of records we need to process
	size_t numBytesToProcess = sizes[databaseAndSet];

	// now, keep looping until we run out of records to process (in which case, we'll break)
	while (true) {

		// all allocations will be done to the page
		const UseTemporaryAllocationBlock block{myPage->getBytes (), pageSize};
		Handle <Vector <Handle <Object>>> data = makeObject <Vector <Handle <Object>>> ();

		try {
			// while there are still pages
			while (allRecs.size () > 0) {

				std :: cout << "Processing a record!!\n";

				auto &allObjects = *(allRecs[allRecs.size () - 1]->getRootObject ());
				numObjectsInRecord = allObjects.size ();

				// put all of the data onto the page
				for (; pos < numObjectsInRecord; pos++)
					data->push_back (allObjects[pos]);
	
				// now kill this record
				numBytesToProcess -= allRecs[allRecs.size () - 1]->numBytes ();
				free (allRecs[allRecs.size () - 1]);
				allRecs.pop_back ();
				pos = 0;
			}

			// if we got here, all records have been processed

			std :: cout << "Write all of the bytes in the record.\n";
			myPage->wroteBytes ();
			myPage->flush ();
			myPage->unpin ();
			break;

		// put the extra objects tht we could not store back in the record
		} catch (NotEnoughSpace &n) {
						
			std :: cout << "Writing back a page!!\n";

			// write back the current page...
			myPage->wroteBytes ();
			myPage->flush ();
			myPage->unpin ();

			// there are two cases... in the first case, we can make another page out of this data, since we have enough records to do so
			if (numBytesToProcess + (((numObjectsInRecord - pos) / numObjectsInRecord) * allRecs[allRecs.size () - 1]->numBytes ()) > pageSize) {
				
				std :: cout << "Are still enough records for another page.\n";
				myPage = getNewPage (databaseAndSet);
				continue;

			// in this case, we have a small bit of data left
			} else {
					
				// create the vector to hold these guys
				void *myRAM = malloc (allRecs[allRecs.size () - 1]->numBytes ());
				const UseTemporaryAllocationBlock block{myRAM, allRecs[allRecs.size () - 1]->numBytes ()};
				Handle <Vector <Handle <Object>>> extraData = makeObject <Vector <Handle <Object>>> (numObjectsInRecord - pos);

				// write the objects to the vector
				auto &allObjects = *(allRecs[allRecs.size () - 1]->getRootObject ());
				for (; pos < numObjectsInRecord; pos++) {
					extraData->push_back (allObjects[pos]);	
				}
				std :: cout << "Putting the records back complete.\n";
	
				// destroy the record that we were copying from
				numBytesToProcess -= allRecs[allRecs.size () - 1]->numBytes ();
				free (allRecs[allRecs.size () - 1]);
	
				// and get the record that we copied to
				allRecs[allRecs.size () - 1] = getRecord (extraData);	
				numBytesToProcess += allRecs[allRecs.size () - 1]->numBytes ();
				break;
			}
		}
	}

	std :: cout << "Now all the records are back.\n";
	sizes[databaseAndSet] = numBytesToProcess;
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
					sendUsingMe->getNextObject <Vector <Handle <Object>>> (readToHere, everythingOK, errMsg);

					if (everythingOK) {	
						// at this point, we have performed the serialization, so remember the record
						auto databaseAndSet = make_pair ((std :: string) request->getDatabase (),
        	                                        (std :: string) request->getSetName ());
						getFunctionality <StorageServer> ().bufferRecord 
							(databaseAndSet, (Record <Vector <Handle <Object>>> *) readToHere); 
	
						// if we have enough space to fill up a page, do it
						std :: cout << "Got the data.\n";
						std :: cout << "Are " << sizes[databaseAndSet] << " bytes to write.\n";
						getFunctionality <StorageServer> ().writeBackRecords (databaseAndSet);
						std :: cout << "Done with write back.\n";
						std :: cout << "Are " << sizes[databaseAndSet] << " bytes left.\n";
					}
				} else {
					errMsg = "Tried to add data of the wrong type to a database set.\n";
					everythingOK = false;
				}
			} else {
				errMsg = "Tried to add data to a set/database combination that does not exist.\n";
				everythingOK = false;
			}

			const UseTemporaryAllocationBlock block{1024};
			Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (everythingOK, errMsg);

                        // return the result
                        bool res = sendUsingMe->sendObject (response, errMsg);
			std :: cout << "Sending response object.\n";
                        return make_pair (res, errMsg);
		}
	));
}

}

#endif
