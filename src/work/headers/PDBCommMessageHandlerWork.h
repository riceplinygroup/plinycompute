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
//
// Created by barnett on 6/16/16.
//

#ifndef PDB_PDBWORK_PDBCOMMMESSAGEHANDLERWORK_H
#define PDB_PDBWORK_PDBCOMMMESSAGEHANDLERWORK_H

#include <PDBCommWork.h>
#include <AllocationStrategy.h>
#include <ObjectAndAllocator.h>

namespace pdb {
namespace detail {


class PDBCommMessageHandlerWork: public PDBCommWork {
	virtual void handleMessageReceived(PDBBuzzerPtr callerBuzzer, Handle<Object> message, shared_ptr<ObjectBlockAllocator> allocatorOfMessage) = 0;

	void execute(PDBBuzzerPtr callerBuzzer) {

		PDBCommunicatorPtr myCommunicator = this->getCommunicator();
		if (myCommunicator == nullptr) {
			callerBuzzer->buzz(PDBAlarm::GenericError);
			return;
		}

		shared_ptr<pdb::SafeResult<pdb::detail::ObjectAndAllocator<pdb::Object>>>messageAndAllocator = myCommunicator->getNextObject();

		messageAndAllocator->apply([&] (pdb::detail::ObjectAndAllocator<pdb::Object> objectAndAllocator) {
			handleMessageReceived(callerBuzzer, objectAndAllocator.Obj, objectAndAllocator.Allocator);
		}, [&] (string errorMessage) {
			// todo: what to do if not successful?
				callerBuzzer->buzz(PDBAlarm::GenericError);
			});

	}

protected:

	virtual size_t getRequestedAdditionalAllocatorSpaceSize() {
		return 0;
	}

};

}
}

#endif //PDB_PDBWORK_PDBCOMMMESSAGEHANDLERWORK_H
