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
#include <iostream>     // std::cout
//#include <iterator>     // std::iterator, std::input_iterator_tag
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
#include "GetListOfNodes.h"
#include "ListOfNodes.h"
#include "Ack.h"
#include "ExecuteQueryOnSingleHost.h"
#include "InterfaceFunctions.h"
#include "UseTemporaryAllocationBlock.h"

namespace pdb {

DistributionManagerServer::DistributionManagerServer(PDBDistributionManagerPtr distributionManagerIn) {
	distributionManager = distributionManagerIn;
}

DistributionManagerServer::~DistributionManagerServer() {
	this->logToMe = getWorker()->getLogger();
}

void DistributionManagerServer::registerHandlers(PDBServer &forMe) {

	// A handler for Heart Beat Operation - Nodes send NodeInfo
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

	forMe.registerHandler(GetListOfNodes_TYPEID, make_shared<SimpleRequestHandler<GetListOfNodes>>([&] (Handle <GetListOfNodes> request, PDBCommunicatorPtr sendUsingMe) {
		std :: string errMsg;

		// get the pointer to the distribution manager instance
			PDBDistributionManagerPtr myDM = getFunctionality <DistributionManagerServer>().getDistributionManager();
			unordered_map<string, long> myList= myDM->getUpNodesOfCluster();
			bool res;

			try {
				// make the vector
				makeObjectAllocatorBlock (1024 * 24, true);

				Handle <Vector <String>> hostNames = makeObject <Vector <String>> ();
				if(hostNames==nullptr) {
					getLogger()->error("DistributionManagerServer::registerHandlers - CouldNotMakeObjectHostNames" );
					errMsg+="CouldNotMakeObjectHostNames";
					return  make_pair (false, errMsg);
				}

				for(auto &ent1 : myList) {
					// ent1.first is the first key
					string tmpHostName=ent1.first;
					hostNames->push_back(tmpHostName);
				}

				//  Make the list of nodes
				makeObjectAllocatorBlock(1024 * 48, true);
				Handle<ListOfNodes> response = makeObject<ListOfNodes>();

				if(response==nullptr) {

					getLogger()->error("DistributionManagerServer::registerHandlers - CouldNotMakeObjectListOfNodes" );
					errMsg+="CouldNotMakeObjectListOfNodes";
					return  make_pair (false, errMsg);
				}

				response->setHostNames(hostNames);
				// return the result
				res = sendUsingMe->sendObject (response, errMsg);

			} catch (NotEnoughSpace &e) {

				res=false;
				errMsg+="NotEnoughSpace";
				getLogger()->error("DistributionManagerServer::registerHandlers - NotEnoughSpace to make objects" );
				return  make_pair (false, errMsg);
			}

			return make_pair (res, errMsg);
		}));

}

void DistributionManagerServer::setDistributionManager(PDBDistributionManagerPtr distributionManagerIn) {
	distributionManager = distributionManagerIn;

}

PDBDistributionManagerPtr DistributionManagerServer::getDistributionManager() {
	return this->distributionManager;
}

PDBLoggerPtr DistributionManagerServer::getLogger() {
	return this->logToMe;
}
}
