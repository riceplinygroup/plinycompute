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
 * PDBDistributionManagerClient.cc
 *
 *  Created on: Mar 17, 2016
 *      Author: Kia
 */

#include "PDBDistributionManagerClient.h"

#include "PDBString.h"
#include "BuiltInObjectTypeIDs.h"

void pdb::PDBDistributionManagerClient::sendHeartBeat(string &masterHostName, int masterNodePort, pdb::Handle<NodeInfo> m_nodeInfo, PDBLoggerPtr logger, bool &wasError, string& errMsg) {

	// First build a new connection to the Server
	PDBCommunicator myCommunicator;
	if (myCommunicator.connectToInternetServer(logger, masterNodePort, masterHostName, errMsg)) {
		logger->writeLn("Error when connecting to server: " + errMsg);
		wasError = true;
		return;
	}

	// send it
	if (!myCommunicator.sendObject(m_nodeInfo, errMsg)) {
		logger->writeLn("HeartBeat Client: Sending nodeInfo object: " + errMsg);
		wasError = true;
		return;
	}

}

pdb::Handle<pdb::QueryPermitResponse> pdb::PDBDistributionManagerClient::sendQueryPermitt(string& hostname, int masterNodePort, pdb::Handle<QueryPermit> m_queryPermit, PDBLoggerPtr logger, bool& wasError,
		string& errMsg) {

	// First build a new connection to the Server
	PDBCommunicator myCommunicator;

	if (myCommunicator.connectToInternetServer(logger, masterNodePort, hostname, errMsg)) {
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

pdb::Handle<pdb::Ack> pdb::PDBDistributionManagerClient::sendQueryDone(string& hostname, int masterNodePort, pdb::Handle<QueryDone> m_queryDone, PDBLoggerPtr logger, bool& wasError, string& errMsg) {

	// First build a new connection to the Server
	PDBCommunicator myCommunicator;

	if (myCommunicator.connectToInternetServer(logger, masterNodePort, hostname, errMsg)) {
		logger->writeLn("Error when connecting to server: " + errMsg);
		wasError = true;
		return nullptr;
	}

	// send QueryPermit object over the socket
	if (!myCommunicator.sendObject(m_queryDone, errMsg)) {
		logger->writeLn("sendQueryDone Client: Sending QueryPermit object: " + errMsg);
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

pdb::Handle<pdb::Ack> pdb::PDBDistributionManagerClient::sendGetPlaceOfQueryPlanner(string &hostName, int masterNodePort, pdb::Handle<PlaceOfQueryPlanner> m_PlaceOfQueryPlanner, PDBLoggerPtr logger, bool &wasError,
		string& errMsg) {

	// First build a new connection to the Server
	PDBCommunicator myCommunicator;

	if (myCommunicator.connectToInternetServer(logger, masterNodePort, hostName, errMsg)) {
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

	logger->info("sendGetPlaceOfQueryPlanner: Got back From Server Query ID : " + string(response->getInfo()));

	return response;
}
