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

#ifndef SERVER_FUNCT_H
#define SERVER_FUNCT_H

#include "PDBServer.h"

namespace pdb {

// this pure virtual class encapsulates some particular server functionality (catalog client,
// catalog server, storage server, etc.).  
class ServerFunctionality {

public:

	// registers any particular handlers that this server needs
	virtual void registerHandlers (PDBServer &forMe) = 0;

        // added by Jia, it will be invoked when PDBServer is to be shutdown
        virtual void cleanup() {}
 
	// access a particular functionality on the attached server
	template <class Functionality>
	Functionality &getFunctionality () {
		return parent->getFunctionality <Functionality> ();
	}
	
	// remember the server this is attached to
	void recordServer (PDBServer &recordMe) {
		parent = &recordMe;
	}

	PDBWorkerPtr getWorker () {
		return parent->getWorkerQueue ()->getWorker ();
	}

private:

	PDBServer *parent;	
};

}

#endif
