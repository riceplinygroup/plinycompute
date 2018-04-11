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
#ifndef PIPELINE_STAGE_H
#define PIPELINE_STAGE_H


#include "PDBVector.h"
#include "SimpleRequest.h"
#include "SimpleRequestResult.h"
#include "SimpleSendDataRequest.h"
#include "DataTypes.h"
#include "PDBLogger.h"
#include "Configuration.h"
#include "SharedMem.h"
#include "TupleSetJobStage.h"
#include "HermesExecutionServer.h"
#include "PartitionedHashSet.h"
#include "SetSpecifier.h"
#include "DataPacket.h"
#include <vector>
#include <memory>
#include <unordered_map>
#include <algorithm>
#include <ctime>
#include <cstdlib>

namespace pdb {


class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;


// this class encapsulates the pipeline stage, which we call TupleSetJobStage
// A TupleSetJobStage can have multiple different sources and different sinks

// Supported sources:
// -- VectorTupleSetIterator
// -- MapTupleSetIterator
// -- JoinTupleSetIterator

// Supported sinks:
// -- ShuffleSink for aggregation
// -- BroadcastSink for broadcast join
// -- HashPartitionSink for hash partitioned join
// -- HashPartitionVectorSink for partitioning vectors
// -- UserSetSink for data materialization 

class PipelineStage {

private:
    // Job stage
    Handle<TupleSetJobStage> jobStage;

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

    // operator id
    OperatorID id;

    // vector of nodeId for shuffling
    std::vector<int> nodeIds;


public:
    // destructor
    ~PipelineStage();

    // constructor
    PipelineStage(Handle<TupleSetJobStage> stage,
                  SharedMemPtr shm,
                  PDBLoggerPtr logger,
                  ConfigurationPtr conf,
                  NodeID nodeId,
                  size_t batchSize,
                  int numThreads);

    // store shuffle data
    bool storeShuffleData(Handle<Vector<Handle<Object>>> data,
                          std::string databaseName,
                          std::string setName,
                          std::string address,
                          int port,
                          std::string& errMsg);
    bool storeCompressedShuffleData(char* bytes,
                                    size_t numBytes,
                                    std::string databaseName,
                                    std::string setName,
                                    std::string address,
                                    int port,
                                    std::string& errMsg);
    // send Shuffle data
    bool sendData(PDBCommunicatorPtr conn,
                  void* bytes,
                  size_t size,
                  std::string databaseName,
                  std::string setName,
                  std::string& errMsg);

    // tuning the backend circular buffer size
    size_t getBackendCircularBufferSize(bool& success, std::string& errMsg);

    // get iterators to scan a user set
    std::vector<PageCircularBufferIteratorPtr> getUserSetIterators(HermesExecutionServer* server,
                                                                   int numThreads,
                                                                   bool& success,
                                                                   std::string& errMsg);

    // get bufferss to scan a user set in a sharing way, so that each iterator is linked to a buffer
    // and will scan all pages
    void feedSharedBuffers(HermesExecutionServer* server,
                           std::vector<PageCircularBufferPtr>& sourceBuffers,
                           int numPartitions,
                           int& counter,
                           PDBBuzzerPtr tempBuzzer,
                           bool& success,
                           std::string& errMsg);

    // create proxy
    DataProxyPtr createProxy(int i, pthread_mutex_t connection_mutex, std::string& errMsg);

    // execute pipeline
    void executePipelineWork(int i,
                             SetSpecifierPtr outputSet,
                             std::vector<PageCircularBufferIteratorPtr>& iterators,
                             PartitionedHashSetPtr hashSet,
                             DataProxyPtr proxy,
                             std::vector<PageCircularBufferPtr>& sinkBuffers,
                             HermesExecutionServer* server,
                             std::string& errMsg);

    // return the root job stage corresponding to the pipeline
    Handle<TupleSetJobStage>& getJobStage();

    // return the number of threads that are required to run the pipeline network
    int getNumThreads();

    // run the pipeline stage
    void runPipeline(HermesExecutionServer* server,
                     std::vector<PageCircularBufferPtr> combinerBuffers,
                     SetSpecifierPtr outputSet);

    // run a pipeline without combining
    void runPipeline(HermesExecutionServer* server);

    // run a pipeline with combiner queue
    void runPipelineWithShuffleSink(HermesExecutionServer* server);

    // run a pipeline with shuffle buffers
    void runPipelineWithBroadcastSink(HermesExecutionServer* server);

    // run a pipeline with hash partitioning for JoinMaps
    void runPipelineWithHashPartitionSink(HermesExecutionServer* server);


};
}


#endif
