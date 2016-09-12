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
 * StoragePinPage.h
 *
 *  Created on: Feb 29, 2016
 *      Author: Jia
 */

#ifndef SRC_BUILTINPDBOBJECTS_HEADERS_PINPAGE_H_
#define SRC_BUILTINPDBOBJECTS_HEADERS_PINPAGE_H_

#include "Object.h"
#include "DataTypes.h"

//  PRELOAD %StoragePinPage%

namespace pdb {
// this object type is sent to the server to tell it to pin a page
class StoragePinPage : public pdb :: Object {


public:

	StoragePinPage () {}

	~StoragePinPage () {}

        StoragePinPage (bool wasNewPage, NodeID nodeId, DatabaseID dbId, UserTypeID userTypeId, SetID setId, PageID pageId) : wasNewPage(wasNewPage), nodeId(nodeId), dbId(dbId), userTypeId(userTypeId), setId(setId), pageId(pageId) {}

	//
	bool getWasNewPage() {return this->wasNewPage;}

	void setWasNewPage(bool wasNewPage) {this->wasNewPage = wasNewPage;}

	NodeID getNodeID() {return this->nodeId;}

	void setNodeID (NodeID nodeId) {this->nodeId = nodeId;}

	DatabaseID getDatabaseID() {return this->dbId;}

	void setDatabaseID(DatabaseID dbId) {this->dbId = dbId;}

	UserTypeID getUserTypeID() {return this->userTypeId;}

	void setUserTypeID(UserTypeID typeId) {this->userTypeId = typeId;}

	SetID getSetID() {return this->setId;}

	void setSetID(SetID setId) {this->setId = setId; }

	PageID getPageID() {return this->pageId; }

	void setPageID(PageID pageId) {this->pageId = pageId;}


private:
	bool wasNewPage;
	NodeID nodeId;
	DatabaseID dbId;
	UserTypeID userTypeId;
	SetID setId;
	PageID pageId;


};

}

#endif /* SRC_BUILTINPDBOBJECTS_HEADERS_PINPAGE_H_ */
