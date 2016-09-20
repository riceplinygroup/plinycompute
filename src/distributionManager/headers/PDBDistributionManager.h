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
 * PDBDistributionManager.h

 *
 *  Created on: Nov, 2015
 *      Author: Kia Teymourian
 */

#ifndef PDB_DISTRIBUTION_MANAGER_H
#define PDB_DISTRIBUTION_MANAGER_H


#include <string>
#include <iostream>
#include <unordered_map>
#include <map>

#include <cassert>


#include "PDBLogger.h"
#include "Ack.h"
#include "PDBString.h"
#include "PDBVector.h"
#include "QueryBase.h"


namespace pdb {

/**
 * This class represents the distribution manager of PDB cluster system.
 */


class PDBDistributionManager;
using namespace std;

// A smart pointer for PDBDistributionManager
typedef shared_ptr<PDBDistributionManager> PDBDistributionManagerPtr;


class PDBDistributionManager {

public:

	PDBDistributionManager();

	~PDBDistributionManager();

	// This method adds or update a node.
	// If the node is seen for the first time it adds it to memory with the current time and returns 0
	// If the node already exists it updates the timestamp and returns 1.
	bool addOrUpdateNodes(PDBLoggerPtr myLoggerIn, string& nodeID);

	//TODO: return the one of the nodes from the list.
	string getSingleNode();

    // Query executer nodes have to ask for permission to run queries on the cluster before they run it.
	// A Query ID is a global unique identifier for the query.
	string getPermitToRunQuery(PDBLoggerPtr myLoggerIn);

	// When the query executer is done with running query it has to notify Distribution Manager.
	// It has to send the same queryID and receive an Ack.
	int queryIsDone(string& queryID, PDBLoggerPtr myLoggerIn);

	unordered_map<string, long>& getUpNodesOfCluster() {
		return nodesOfCluster;
	}

	int getNoOfNodes() {
			return nodesOfCluster.size();
	}

	void setNodesOfCluster(const unordered_map<string, long>& nodesOfCluster) {
		this->nodesOfCluster = nodesOfCluster;
	}

	string& getQueryPlannerPlace() {
		return queryPlannerPlace;
	}

	void setQueryPlannerPlace(string& queryPlannerPlace) {
		this->queryPlannerPlace = queryPlannerPlace;
	}

private:

	// stores the heart beat data that we are collecting form all worker nodes in the cluster.
	unordered_map<string, long> nodesOfCluster;

	// This is a counter to count heart beat messages and active the cleaning process after we got specific number of messages.
	// This is just to do not start the cleaning process immediately after cluster bootstrapping.
	int heartBeatCounter;

	// it stores the location of the query planner in the cluster.
	string queryPlannerPlace;

	// stores currently running queries and their start time.
	// we store the start time of query execution to be able to clean the list when one of the executors goes down and does not response any more.
	map<string, long> runningQueries;

    //////Mutex/////////////////
    pthread_mutex_t writeLock;

};
}
#endif /* DISTRIBUTION_HEADERS_PDBDISTRIBUTIONMANAGER_H_ */
