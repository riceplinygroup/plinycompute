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
#ifndef HEART_BEAT_HANDLER_CC
#define HEART_BEAT_HANDLER_CC

#include <iostream>

#include "HeartBeatHandler.h"
#include "NodeInfo.h"
#include "Handle.h"
#include "Ack.h"
#include "UseTemporaryAllocationBlock.h"
#include "PDBCommunicator.h"
#include "ServerFunctionality.h"

pdb::HeartBeatHandler::HeartBeatHandler(string fNameIn, PDBDistributionManagerPtr distribution) {
	this->fName = fNameIn;
	this->distribution = distribution;
}

pdb::HeartBeatHandler::~HeartBeatHandler() {
}

pdb::PDBCommWorkPtr pdb::HeartBeatHandler::clone() {
	return make_shared<HeartBeatHandler>(fName, distribution);
}

void pdb::HeartBeatHandler::execute(PDBBuzzerPtr callerBuzzer) {

	std::string errMsg;
	bool success;

	PDBCommunicatorPtr myCommunicator = getCommunicator();
	size_t numBytes = myCommunicator->getSizeOfNextObject();
	UseTemporaryAllocationBlock myBlock {numBytes};
	Handle<NodeInfo> msg = myCommunicator->getNextObject<NodeInfo>(success, errMsg);

	if (!success) {
		getLogger()->error("HeartBeatHandler: " + errMsg);
		callerBuzzer->buzz(PDBAlarm::HeartBeatError);
		return;
	}

	string hostName = (string) msg->getHostName();

	getLogger()->trace("Heart Beat From Host: " + hostName +":" +to_string(msg->getPort()));

	this->distribution->addOrUpdateNodes(getLogger(), hostName);

	callerBuzzer->buzz(PDBAlarm::WorkAllDone);

}

#endif
