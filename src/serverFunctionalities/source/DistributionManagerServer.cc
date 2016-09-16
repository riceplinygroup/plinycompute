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
#include <cstddef>
#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

#include "UseTemporaryAllocationBlock.h"
#include "SimpleRequestHandler.h"
#include "BuiltInObjectTypeIDs.h"

#include "SimpleRequestResult.h"
#include "DistributionManagerServer.h"
#include "NodeInfo.h"
#include "Ack.h"
#include "ExecuteQueryOnSingleHost.h"

namespace pdb {

DistributionManagerServer::DistributionManagerServer() {
	// make the distribution manager instance.
	this->distributionManager = make_shared<PDBDistributionManager>();
}

DistributionManagerServer::~DistributionManagerServer() {
}

void DistributionManagerServer::registerHandlers(PDBServer &forMe) {

	forMe.registerHandler(NodeInfo_TYPEID, make_shared<SimpleRequestHandler<NodeInfo>>([&] (Handle <NodeInfo> request, PDBCommunicatorPtr sendUsingMe) {

		//TODO: There is no errMsg that I can forward. Check if we need to forward some errMsg.
			std :: string errMsg;

			// get the pointer to the distribution manager instance
			PDBDistributionManagerPtr myDM = getFunctionality <DistributionManagerServer>().getDistributionManager();
			std::string hostname = request->getHostName();

			// update
			//TODO: result is unused so far.
			bool resulstFromUpdate = myDM->addOrUpdateNodes(forMe.getLogger(), hostname);
			bool wasError;

			//TODO
			if(resulstFromUpdate)
			wasError=true;
			else
			wasError=true;

			//TODO : I have no fail case, so I send always true with no errMsg.
			// return the result
			return make_pair (wasError, errMsg);
		}));

}

PDBDistributionManagerPtr DistributionManagerServer::getDistributionManager() {
	return this->distributionManager;
}


}
