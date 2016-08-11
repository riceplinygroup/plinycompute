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


/****************************************************
** COPYRIGHT 2016, Chris Jermaine, Rice University **
**                                                 **
** The MyDB Database System, COMP 530              **
** Note that this file contains SOLUTION CODE for  **
** A1.  You should not be looking at this file     **
** unless you have completed A1!                   **
****************************************************/


#ifndef BUFFER_MGR_H
#define BUFFER_MGR_H

#include "CheckLRU.h"
#include <map>
#include <memory>
#include "MyDB_Page.h"
#include "MyDB_PageHandle.h"
#include "MyDB_Table.h"
#include "PageCompare.h"
#include <queue>
#include "TableCompare.h"
#include <set>

using namespace std;

class MyDB_BufferManager;
typedef shared_ptr <MyDB_BufferManager> MyDB_BufferManagerPtr;

class MyDB_BufferManager {

public:

	// gets the i^th page in the table whichTable... note that if the page
	// is currently being used (that is, the page is current buffered) a handle 
	// to that already-buffered page should be returned
	MyDB_PageHandle getPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular 
	// table
	MyDB_PageHandle getPage ();

	// gets the i^th page in the table whichTable... the only difference 
	// between this method and getPage (whicTable, i) is that the page will be 
	// pinned in RAM; it cannot be written out to the file... note that in Chris'
	// implementation, a request for a pinned page that is made when the buffer
	// is ENTIRELY full of pinned pages will return a nullptr
	MyDB_PageHandle getPinnedPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page, like getPage (), except that this one is pinned
	MyDB_PageHandle getPinnedPage ();

	// un-pins the specified page
	void unpin (MyDB_PagePtr unpinMe);

	// creates an LRU buffer manager... params are as follows:
	// 1) the size of each page is pageSize 
	// 2) the number of pages managed by the buffer manager is numPages;
	// 3) temporary pages are written to the file tempFile
	MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile);
	
	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, and any temporary files need to be deleted
	~MyDB_BufferManager ();

	// returns the page size
	size_t getPageSize ();
	
private:

	// tells us the LRU number of each of the pages
	set <MyDB_PagePtr, CheckLRU> lastUsed;

	// list of ALL of the page objects that are currently in existence
	map <pair <MyDB_TablePtr, size_t>, MyDB_PagePtr, PageCompare> allPages;
	
	// lists the FDs for all of the files
	map <MyDB_TablePtr, int, TableCompare> fds;

	// all of the chunks of RAM that are currently not allocated
	vector <void *> availableRam;

	// all of the positions in the temporary file that are currently not in use
	priority_queue<size_t, vector<size_t>, greater<size_t>> availablePositions;

	// the page size
	size_t pageSize;

	// the time tick associated with the MRU page
	long lastTimeTick;

	// the last position in the temporary file
	size_t lastTempPos;

	// where we write the data
	string tempFile;

	// the number of buffer pages
	size_t numPages;

	// so that the page can access these private methods
	friend class MyDB_Page;
	friend class SortMergeJoin;

	// kick out the LRU page
	void kickOutPage ();

	// process an access to the given page
	void access (MyDB_PagePtr updateMe);

	// removes all traces of the page from the buffer manager
	void killPage (MyDB_PagePtr killMe);

};

#endif


