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
#ifndef PIPELINE_NETWORK_H
#define PIPELINE_NETWORK_H

//by Jia, Sept 2016

#include <vector>
#include "PipelineNode.h"
#include "DataTypes.h"
#include "PDBLogger.h"
#include "Configuration.h"
#include "SharedMem.h"

namespace pdb {

// this class encapsulates the pipeline network
class PipelineNetwork {

private:

    // all source nodes
    std :: vector<PipelineNodePtr> * sourceNodes;

    // all nodes in this network
    std :: unordered_map<OperatorID, PipelineNodePtr> * allNodes;

    // Job stage id
    JobStageID stageId;

    // batch size
    size_t batchSize;

    // number of threads
    int numThreads;

    // node id
    NodeID nodeId;

    // logger
    PDBLoggerPtr logger;

    // configuration
    ConfigurationPtr conf;

    // shared memory
    SharedMemPtr shm;

public:

    //destructor
    ~PipelineNetwork();

    //constructor
    PipelineNetwork(SharedMemPtr shm, PDBLoggerPtr logger, ConfigurationPtr conf, NodeID nodeId, JobStageID id, size_t batchSize, int numThreads);

    //return the job stage id
    JobStageID getStageId();

    //return all source nodes
    std :: vector<PipelineNodePtr> getSourceNodes();

    //append a node to the pipeline network
    bool appendNode(OperatorID parentId, PipelineNodePtr node);

    //append a source node to the pipeline network
    void appendSourceNode(PipelineNodePtr node); 

    //return the number of threads that are required to run the pipeline network
    int getNumThreads();

    //return the number of sources of the pipeline network
    int getNumSources();

    //running all sources in the pipeline (in one thread)
    void runAllSources();

    //running the i-the source (in one thread)
    //so you can multiple sources in different threads
    void runSource (int i);

};





}





#endif
