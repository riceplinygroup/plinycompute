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
 * StorageBytesPinned.h
 *
 *  Created on: Feb 29, 2016
 *      Author: Jia
 */

#ifndef SRC_BUILTINPDBOBJECTS_HEADERS_BYTESPINNED_H_
#define SRC_BUILTINPDBOBJECTS_HEADERS_BYTESPINNED_H_

#include <cstddef>

#include "Object.h"
#include "DataTypes.h"

//  PRELOAD %StorageBytesPinned%

namespace pdb {
// this object type is sent from the server to backend to tell it a page is pinned for it.
class StorageBytesPinned : public pdb :: Object {


public:

	StorageBytesPinned () {}
	~StorageBytesPinned () {}


	//get/set sizeOfBytes
	size_t getSizeOfBytes() {return this->sizeOfBytes;}
	void setSizeOfBytes(size_t sizeOfBytes) {this->sizeOfBytes = sizeOfBytes;}


	//get/set page offset in shared memory pool
	size_t getSharedMemOffset() {return this->sharedMemOffset;}
	void setSharedMemOffset(size_t offset) {this->sharedMemOffset = offset;}

        ENABLE_DEEP_COPY

private:
	size_t sizeOfBytes;
	size_t sharedMemOffset;

};

}

#endif /* SRC_BUILTINPDBOBJECTS_HEADERS_BYTESPINNED_H_ */
