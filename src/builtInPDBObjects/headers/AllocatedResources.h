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

#ifndef ALLOCATED_RESOURCES_H
#define ALLOCATED_RESOURCES_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"
#include "ResourceInfo.h"
#include "PDBVector.h"

// PRELOAD %AllocatedResources%

namespace pdb {

// encapsulates a response for resource requests
class AllocatedResources  : public Object {

public:

	AllocatedResources () {}
	~AllocatedResources () {}

	AllocatedResources (Handle<Vector<Handle<ResourceInfo>>> resources) : resources (resources) {}

	Handle<Vector<Handle<ResourceInfo>>> getResources () {
		return resources;
	}

        void print() {
                for (int i = 0; i < resources->size(); i++) {
                     std::cout << i << ":" << "CPU number=" << (*resources)[i]->getNumCores() << ", memory size=" << (*resources)[i]->getMemSize() << std :: endl;
                }

        }

	ENABLE_DEEP_COPY

private:

        Handle<Vector<Handle<ResourceInfo>>> resources;
};

}

#endif
