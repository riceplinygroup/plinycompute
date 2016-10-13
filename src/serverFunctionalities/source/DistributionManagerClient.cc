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
 * DistributionManagerClient.cc
 *
 *  Created on: Sep 12, 2016
 *      Author: kia
 */

#ifndef DISTRIBUTION_MANAGER_CLIENT_CC
#define DISTRIBUTION_MANAGER_CLIENT_CC

#include <chrono>
#include <thread>
#include <iostream>

#include "DistributionManagerClient.h"
#include "NodeInfo.h"
#include "InterfaceFunctions.h"

#include "ExecuteQueryOnSingleHost.h"
#include "UseTemporaryAllocationBlock.h"
#include "GetListOfNodes.h"

namespace pdb {

DistributionManagerClient::DistributionManagerClient(PDBLoggerPtr loggerIn) {
	logger = loggerIn;
}

DistributionManagerClient::DistributionManagerClient(pdb::String hostnameIn, int portIn, PDBLoggerPtr loggerIn) {
	logger = loggerIn;
	port = portIn;
	hostname = hostnameIn;
}
;

DistributionManagerClient::~DistributionManagerClient() {
}

void DistributionManagerClient::registerHandlers(PDBServer &forMe) { /* no handlers for a DistributionManager client!! */
}

void DistributionManagerClient::sendHeartBeat(string &masterHostName, int masterNodePort, bool &wasError, string& errMsg) {

	std::chrono::seconds interval(2);  // 2 seconds


	try {
		makeObjectAllocatorBlock(1024 * 24, true);
		Handle<NodeInfo> m_nodeInfo = makeObject<NodeInfo>();

		m_nodeInfo->setHostName(hostname);
		m_nodeInfo->setPort(port);

		//TODO: this is temporary to check the heart beat functionality and implement the timer inside PDBServer.
		while (true) {

			// First build a new connection to the Server
			PDBCommunicator myCommunicator;
			if (myCommunicator.connectToInternetServer(logger, masterNodePort, masterHostName, errMsg)) {
				logger->error("[DistributionManagerClient] - Error when connecting to server: " + errMsg);

				// try to connect again after awhile.
			} else {

				// send  the object ovber
				if (!myCommunicator.sendObject(m_nodeInfo, errMsg)) {
					logger->error("[DistributionManagerClient] - HeartBeat Client: Sending nodeInfo object: " + errMsg);
					// try to connect and send the object again.
				}
			}
			// sleep for the time interval and send it again.
			std::this_thread::sleep_for(interval);
		}

	} catch (NotEnoughSpace &e) {
		logger->error("[DistributionManagerClient] - Not enough memory");
	}

}

Handle<ListOfNodes> DistributionManagerClient::getCurrentNodes(string &masterHostName, int masterNodePort, bool &wasError, string& errMsg) {
	// First build a new connection to the Server
	PDBCommunicator myCommunicator;
	if (myCommunicator.connectToInternetServer(logger, masterNodePort, masterHostName, errMsg)) {
		logger->error("[DistributionManagerClient] - Error when connecting to server: " + errMsg);
		wasError = true;
		return nullptr;
	}
	makeObjectAllocatorBlock(1024, true);

	try {
		Handle<GetListOfNodes> requestToGetListOfNodes = makeObject<GetListOfNodes>();

		// send  the object over
		if (!myCommunicator.sendObject(requestToGetListOfNodes, errMsg)) {
			logger->error("[DistributionManagerClient] - HeartBeat Client: Sending nodeInfo object: " + errMsg);
			// try to connect and send the object again.
			wasError = true;
			return nullptr;
		}

	} catch (NotEnoughSpace &e) {
		logger->error("[DistributionManagerClient] - Not enough memory");
	}

	// Get the response from the server.
	bool success;
	Handle<ListOfNodes> response = myCommunicator.getNextObject<ListOfNodes>(success, errMsg);
	if (!success) {
		logger->error("[DistributionManagerClient] - getCurrentNodes Error when connecting to server: " + errMsg);
		return nullptr;
	}

	return response;

}

//bool DistributionManagerClient::shutDownServer(std::string &errMsg) {
//
//	return simpleRequest<ShutDown, SimpleRequestResult, bool>(logger, port, address, false, 1024, [&] (Handle <SimpleRequestResult> result) {
//		if (result != nullptr) {
//			if (!result->getRes ().first) {
//				errMsg = "Error shutting down server: " + result->getRes ().second;
//				myLogger->error ("Error shutting down server: " + result->getRes ().second);
//				return false;
//			}
//			return true;
//		}
//		errMsg = "Error getting type name: got nothing back from catalog";
//		return false;});
//}

Handle<QueryPermitResponse> DistributionManagerClient::sendQueryPermitt(string &hostName, int masterNodePort, pdb::Handle<QueryPermit> m_queryPermit, bool &wasError, string& errMsg) {

	// First build a new connection to the Server
	PDBCommunicator myCommunicator;

	if (myCommunicator.connectToInternetServer(logger, masterNodePort, hostName, errMsg)) {
		logger->error("Error when connecting to server: " + errMsg);
		wasError = true;
		return nullptr;
	}

	// send QueryPermit object over the socket
	if (!myCommunicator.sendObject(m_queryPermit, errMsg)) {
		logger->error("sendQueryPermitt Client: Sending QueryPermit object: " + errMsg);
		wasError = true;
		return nullptr;
	}

	bool success;

	pdb::Handle<QueryPermitResponse> response = myCommunicator.getNextObject<QueryPermitResponse>(success, errMsg);

	if (!success) {
		logger->error("sendQueryPermitt Error when connecting to server: " + errMsg);
		return nullptr;
	}

	logger->trace("Got back From Server Query ID : " + string(response->getQueryId()));

	return response;
}

Handle<Ack> DistributionManagerClient::sendQueryDone(string &hostName, int masterNodePort, Handle<QueryDone> m_queryDone, bool &wasError, string& errMsg) {

	// First build a new connection to the Server
	PDBCommunicator myCommunicator;

	if (myCommunicator.connectToInternetServer(logger, masterNodePort, hostName, errMsg)) {
		logger->error("Error when connecting to server: " + errMsg);
		wasError = true;
		return nullptr;
	}

	// send QueryPermit object over the socket
	if (!myCommunicator.sendObject(m_queryDone, errMsg)) {
		logger->error("sendQueryDone Client: Sending QueryPermit object: " + errMsg);
		wasError = true;
		return nullptr;
	}

	bool success;
	pdb::Handle<Ack> response = myCommunicator.getNextObject<Ack>(success, errMsg);

	if (!success) {
		logger->error("PDBDistributionManagerClient -sendQueryDone no ack received: " + errMsg);
		return nullptr;
	}

	logger->trace("Got back From Server Query ID : " + string(response->getInfo()));
	return response;

}

Handle<Ack> DistributionManagerClient::sendGetPlaceOfQueryPlanner(string &masterNodeHostName, int masterNodePort, Handle<PlaceOfQueryPlanner> m_PlaceOfQueryPlanner, bool &wasError, string& errMsg) {

	// First build a new connection to the Server
	PDBCommunicator myCommunicator;

	if (myCommunicator.connectToInternetServer(logger, masterNodePort, masterNodeHostName, errMsg)) {
		logger->error("Error when connecting to server: " + errMsg);
		wasError = true;
		return nullptr;
	}

	// send QueryPermit object over the socket
	if (!myCommunicator.sendObject(m_PlaceOfQueryPlanner, errMsg)) {
		logger->error("sendQueryDone Client: Sending QueryPermit object: " + errMsg);
		wasError = true;
		return nullptr;
	}

	bool success;
	// get the next object
	pdb::Handle<Ack> response = myCommunicator.getNextObject<Ack>(success, errMsg);

	if (!success) {
		logger->error("ERROR sendQueryPermitt: Uh oh.  The type is not what I expected!!\n");
	}

	logger->trace("sendGetPlaceOfQueryPlanner: Got back From Server Query ID : " + string(response->getInfo()));

	return response;
}

}

#endif
