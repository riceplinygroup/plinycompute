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
 * StoragePagePinned.h
 *
 *  Created on: Feb 29, 2016
 *      Author: Jia
 */

#ifndef SRC_BUILTINPDBOBJECTS_HEADERS_PAGEPINNED_H_
#define SRC_BUILTINPDBOBJECTS_HEADERS_PAGEPINNED_H_

#include <cstddef>

#include "Object.h"
#include "DataTypes.h"

//  PRELOAD %StoragePagePinned%

namespace pdb {
// this object type is sent from the server to backend to tell it a page is pinned for it.
class StoragePagePinned : public pdb :: Object {


public:

	StoragePagePinned () {}
	~StoragePagePinned () {}

	//get/set morePagesToLoad, if it is set to false, the other side knows that the receive loop should be ended
	bool getMorePagesToLoad() { return this->morePagesToLoad; }
	void setMorePagesToLoad(bool morePagesToLoad) { this->morePagesToLoad = morePagesToLoad; }

        //get/set nodeId
	NodeID getNodeID() {return this->nodeId;}
	void setNodeID(NodeID nodeId) {this->nodeId = nodeId;}

        //get/set databaseId
	DatabaseID getDatabaseID() {return this->dbId;}
	void setDatabaseID(DatabaseID dbId) {this->dbId = dbId;}

	//get/set userTypeId
	UserTypeID getUserTypeID() {return this->userTypeId;}
	void setUserTypeID(UserTypeID typeId) {this->userTypeId = typeId;}

	//get/set setId
	SetID getSetID() {return this->setId;}
	void setSetID(SetID setId) {this->setId = setId; }

	//get/set pageId
	PageID getPageID() {return this->pageId;}
	void setPageID(PageID pageId) {this->pageId = pageId;}

	//get/set pageSize
	size_t getPageSize() {return this->pageSize;}
	void setPageSize(size_t pageSize) {this->pageSize = pageSize;}


	//get/set page offset in shared memory pool
	size_t getSharedMemOffset() {return this->sharedMemOffset;}
	void setSharedMemOffset(size_t offset) {this->sharedMemOffset = offset;}


private:
	bool morePagesToLoad;
	NodeID nodeId;
	DatabaseID dbId;
	UserTypeID userTypeId;
	SetID setId;
	PageID pageId;
	size_t pageSize;
	size_t sharedMemOffset;

};

}

#endif /* SRC_BUILTINPDBOBJECTS_HEADERS_PAGEPINNED_H_ */
