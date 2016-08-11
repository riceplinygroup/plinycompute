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


#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include <fcntl.h>
#include <iostream>
#include "MyDB_BufferManager.h"
#include "MyDB_Page.h"
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <utility>

using namespace std;

size_t MyDB_BufferManager :: getPageSize () {
	return pageSize;
}

MyDB_PageHandle MyDB_BufferManager :: getPage (MyDB_TablePtr whichTable, long i) {
		
	// open the file, if it is not open
	if (fds.count (whichTable) == 0) {
		int fd = open (whichTable->getStorageLoc ().c_str (), O_CREAT | O_RDWR, 0666);
		fds[whichTable] = fd;
	}

	// make sure we don't have a null table
	if (whichTable == nullptr) {
		cout << "Can't allocate a page with a null table!!\n";
		exit (1);
	}
	
	// next, see if the page is already in existence
	pair <MyDB_TablePtr, long> whichPage = make_pair (whichTable, i);
	if (allPages.count (whichPage) == 0) {

		// it is not there, so create a page
		//cout << "Didn't find it\n";
		MyDB_PagePtr returnVal = make_shared <MyDB_Page> (whichTable, i, *this);
		allPages [whichPage] = returnVal;
		return make_shared <MyDB_PageHandleBase> (returnVal);
	}

	// it is there, so return it
	return make_shared <MyDB_PageHandleBase> (allPages [whichPage]);
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {

	// open the file, if it is not open
	if (fds.count (nullptr) == 0) {
		int fd = open (tempFile.c_str (), O_TRUNC | O_CREAT | O_RDWR, 0666);
		fds[nullptr] = fd;
	}

	// check if we are extending the size of the temp file
	size_t pos;
	if (availablePositions.size () == 0) {
		pos = lastTempPos++;
	} else {
		pos = availablePositions.top ();
		availablePositions.pop ();
	}

	MyDB_PagePtr returnVal = make_shared <MyDB_Page> (nullptr, pos, *this);
	return make_shared <MyDB_PageHandleBase> (returnVal);
}

void MyDB_BufferManager :: kickOutPage () {
	
	// find the oldest page
	auto it = lastUsed.begin();
	auto page = *it;

	if (lastUsed.size () == 0)
		cout << "Bad: all buffer memory is exhaisted!";

	// make sure we don't have a null pointer
	if (page->bytes == nullptr) {
		cout << "Bad!! Kicking out a page with no RAM.";
		exit (1);
	}

	// write it back if necessary
	if (page->isDirty) {
		lseek (fds[page->myTable], page->pos * pageSize, SEEK_SET);
		write (fds[page->myTable], page->bytes, pageSize);
		page->isDirty = false;
	}

	// remove it
	lastUsed.erase (page);

	// remember its RAM
	availableRam.push_back (page->bytes);
	page->bytes = nullptr;

	// if this guy has no references, kill him
	if (page->refCount == 0)
		killPage (page);
}

void MyDB_BufferManager :: killPage (MyDB_PagePtr killMe) {
	
	//cout << "killing " << killMe->myTable << " " << killMe->pos << "\n";

	// if this is an anon page...
	if (killMe->myTable == nullptr) {

		// recycle him
		availablePositions.push (killMe->pos);
		if (killMe->bytes != nullptr) {
			availableRam.push_back (killMe->bytes);
		}

		// if he is in the LRU list, remove him
		if (lastUsed.count (killMe) != 0) {
			auto page = *(lastUsed.find (killMe));
			lastUsed.erase (page);
		}

	// if this is a pinned, non-anon page whose data is buffered it converts...
	} else if (lastUsed.count (killMe) == 0 && killMe->bytes != nullptr) {
		killMe->timeTick = ++lastTimeTick;
		lastUsed.insert (killMe);

	// this guy has no data, so just kill him
	} else if (killMe->bytes == nullptr) {
		pair <MyDB_TablePtr, long> whichPage = make_pair (killMe->myTable, killMe->pos);
		allPages.erase (whichPage);
	}
}

void MyDB_BufferManager :: access (MyDB_PagePtr updateMe) {
	
	// if this page was just accessed, get outta here
	if (updateMe->timeTick > lastTimeTick - (numPages / 2) && updateMe->bytes != nullptr) {
		return;
	}

	// first, see if it is currently in the LRU list; if it is, update it
	if (lastUsed.count (updateMe) == 1) {
		auto page = *(lastUsed.find (updateMe));
		lastUsed.erase (page);
		updateMe->timeTick = ++lastTimeTick;
		lastUsed.insert (updateMe);

	// here, we don't have the bytes...
	} else if (updateMe->bytes == nullptr) {
		
		// not in the LRU list means that we don't have its contents buffered
		// see if there is space
		if (availableRam.size () == 0)
			kickOutPage ();

		// if there is no space, we cannot do anything
		if (availableRam.size () == 0) {
			cout << "Can't get any RAM to read a page!!\n";
			exit (1);
		}

		// get some RAM for the page
		updateMe->bytes = availableRam[availableRam.size () - 1]; 
		updateMe->numBytes = pageSize;
		availableRam.pop_back ();

		// and read it
		lseek (fds[updateMe->myTable], updateMe->pos * pageSize, SEEK_SET);
		read (fds[updateMe->myTable], updateMe->bytes, pageSize);
		//cout << "reading " << updateMe->myTable << " " << updateMe->pos << "\n";

		updateMe->timeTick = ++lastTimeTick;
		lastUsed.insert (updateMe);
	}
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (MyDB_TablePtr whichTable, long i) {

	// open the file, if it is not open
	if (fds.count (whichTable) == 0) {
		int fd = open (whichTable->getStorageLoc ().c_str (), O_CREAT | O_RDWR, 0666);
		fds[whichTable] = fd;
	}

	// make sure we don't have a null table
	if (whichTable == nullptr) {
		cout << "Can't allocate a page with a null table!!\n";
		exit (1);
	}

	// first, see if the page is there in the buffer
	pair <MyDB_TablePtr, long> whichPage = make_pair (whichTable, i);
	MyDB_PagePtr returnVal;

	// see if we already know him
	if (allPages.count (whichPage) == 0) {

		//cout << "could not find pinned page\n";
		// in this case, we do not
		returnVal = make_shared <MyDB_Page> (whichTable, i, *this);
		allPages [whichPage] = returnVal;
		//cout << "are " << allPages.size () << " entries.\n";

	// in this case, we do
	} else {

		// get him out of the LRU list if he is there
		//cout << "found pinned page\n";
		returnVal = allPages [whichPage];
		if (lastUsed.count (returnVal) != 0) {
			auto page = *(lastUsed.find (returnVal));
	       		lastUsed.erase (page);
		}
	}

	// see if we need to get his data
	if (returnVal->bytes == nullptr) {

		// see if there is space to make a pinned page
		if (availableRam.size () == 0)
			kickOutPage ();

		// if there is no space, we cannot do anything
		if (availableRam.size () == 0) 
			return nullptr;

		// set up the return val
		returnVal->bytes = availableRam[availableRam.size () - 1];
		returnVal->numBytes = pageSize;
		availableRam.pop_back ();

		// and read it
		lseek (fds[returnVal->myTable], returnVal->pos * pageSize, SEEK_SET);
		read (fds[returnVal->myTable], returnVal->bytes, pageSize);

	}	

	// get outta here
	return make_shared <MyDB_PageHandleBase> (returnVal);
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {

	// see if there is space to make a pinned page
	if (availableRam.size () == 0)
		kickOutPage ();

	// if there is no space, we cannot do anything
	if (availableRam.size () == 0) 
		return nullptr;

	// get a page to return
	MyDB_PageHandle returnVal = getPage ();
	returnVal->page->bytes = availableRam[availableRam.size () - 1];
	returnVal->page->numBytes = pageSize;
	availableRam.pop_back ();

	// and get outta here
	return returnVal;
}

void MyDB_BufferManager :: unpin (MyDB_PagePtr unpinMe) {
	unpinMe->timeTick = ++lastTimeTick;
	lastUsed.insert (unpinMe);
}

MyDB_BufferManager :: MyDB_BufferManager (size_t pageSizeIn, size_t numPagesIn, string tempFileIn) {

	// remember the inputs
	pageSize = pageSizeIn;

	// this is the location where we write temp pages
	tempFile = tempFileIn;

	// start at time tick zero
	lastTimeTick = 0;

	// position in temp file
	lastTempPos = 0;

	// the number of pages
	numPages = numPagesIn;

	// create all of the RAM
	for (size_t i = 0; i < numPages; i++) {
		availableRam.push_back (malloc (pageSizeIn));
	}	
}

MyDB_BufferManager :: ~MyDB_BufferManager () {
	
	//cout << "\n";
	//cout << allPages.size () << "\n";
	for (auto page : allPages) {

		if (page.second->bytes != nullptr) {

			// write it back if necessary
			if (page.second->isDirty) {
				lseek (fds[page.second->myTable], page.second->pos * pageSize, SEEK_SET);
				write (fds[page.second->myTable], page.second->bytes, pageSize);
			}

			free (page.second->bytes);
			page.second->bytes = nullptr;
		}
		//cout << "writing back " << page.second->myTable << " " << page.second->pos << "\n";
	}

	// delete the rest of the RAM
	for (auto ram : availableRam) {
		free (ram);
	}

	// finally, close the files
	for (auto fd : fds) {
		close (fd.second);
	}

	unlink (tempFile.c_str ());
}


#endif


