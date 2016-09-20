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
 * PDBDistributionManager.cc
 *
 *  Created on: Nov 30, 2015
 *      Author: kia
 */
#ifndef PDB_DISTRIBUTION_MANAGER_CC
#define	PDB_DISTRIBUTION_MANAGER_CC

#include "PDBDistributionManager.h"

#include <stdio.h>
#include <time.h>

#include "PDBLogger.h"

#include <iostream>
#include <vector>
#include <string>

#include <signal.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <map>
#include <stdio.h>
#include <time.h>
#include <chrono>
#include <uuid/uuid.h>


#include "UseTemporaryAllocationBlock.h"

using namespace std;

namespace pdb {

PDBDistributionManager::PDBDistributionManager() {
	this->heartBeatCounter = 0;
}

PDBDistributionManager::~PDBDistributionManager() {
}

bool PDBDistributionManager::addOrUpdateNodes(PDBLoggerPtr myLoggerIn, string& nodeID) {

	std::chrono::time_point<std::chrono::system_clock> p;
	p = std::chrono::system_clock::now();

	this->heartBeatCounter++;
	long timeCounter = std::chrono::duration_cast<std::chrono::nanoseconds>(p.time_since_epoch()).count();

	// if we have 20 heart beat messages from any servers then we activate the cleaning process.
	if (this->heartBeatCounter >= 20) {
		// iterate over the list of nodes and remove old ones.
		for (auto myPair = nodesOfCluster.begin(); myPair != nodesOfCluster.end();) {
			// if a node does not send heart beat messages then remove it from the list. If we don't get heart beat more than 10sec.
			if ((timeCounter - myPair->second) > 10000000000) {
				string m_nodehostname = myPair->first;
				myLoggerIn->writeLn("PDBDistributionManager: Not responding node to remove " + m_nodehostname + " No. of Nodes in Cluster " + to_string(nodesOfCluster.size()));
				myPair = nodesOfCluster.erase(myPair);
			} else
				++myPair;

		}
		this->heartBeatCounter = 0;
	}

	if (this->nodesOfCluster.count(nodeID) == 0) {
		// insert the nodeID and timestamp
		this->nodesOfCluster[nodeID] = timeCounter;
		myLoggerIn -> info ("PDBDistributionManager:  Node  with ID " + nodeID + " added.  No. of Nodes in Cluster " + to_string(nodesOfCluster.size()));
		return false;
	} else {
		// insert the nodeID and timestamp
		this->nodesOfCluster[nodeID] = timeCounter;
		myLoggerIn -> info ("PDBDistributionManager: Node with ID " + nodeID + " updated.   No. of Nodes in Cluster " + to_string(nodesOfCluster.size()));
		return true;
	}

}

/**
 * This method provides permission to run a query on the PDB cluster.
 * It generates a GUID as query ID as a string and returns it to the query executor.
 *
 */
string PDBDistributionManager::getPermitToRunQuery(PDBLoggerPtr myLoggerIn) {

//TODO: we need to know when to admit permission to run queries and when not.

	uuid_t uuid;

	// generate
	uuid_generate(uuid);

	char uuid_str[37];      // ex. "1b4e28ba-2fa1-11d2-883f-0016d3cca427" + "\0"
	uuid_unparse_lower(uuid, uuid_str);

	// check the time now and store the time of permitting the query.
	std::chrono::time_point<std::chrono::system_clock> p;
	p = std::chrono::system_clock::now();
	long timeCounter = std::chrono::duration_cast<std::chrono::nanoseconds>(p.time_since_epoch()).count();

	string uuidString = uuid_str;

	// store time of running this query.
	runningQueries[uuidString] = timeCounter;

	myLoggerIn->trace("PDBDistributionManager: permitted running a new query with GUID " + uuidString + "  at time " + to_string(timeCounter) + " No. of Queries: " + to_string(runningQueries.size()));

	return uuid_str;
}

/**
 * This is called when a query is done.
 * Returns 1 if it can find the query in memory and removes 0 when it has no data about this query.
 */
int PDBDistributionManager::queryIsDone(string& queryID, PDBLoggerPtr logToMe) {

	if (runningQueries.find(queryID) != runningQueries.end()) {
		map<string, long>::iterator tmp = runningQueries.find(queryID);
		// erase it from the map
		runningQueries.erase(tmp);
		logToMe->trace("PDBDistributionManager: running query with " + queryID + " is done and erased from memory." + " No. of Queries: " + to_string(runningQueries.size()));
		return 1;
	} else {
		logToMe->error("PDBDistributionManager: running query with " + queryID + " could not be found." + " No. of Queries: " + to_string(runningQueries.size()));
		return 0;
	}

}

}
#endif

