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
#ifndef FRONTEND_SERVER_CC
#define FRONTEND_SERVER_CC

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
#include "PDBDebug.h"
#include "FrontendQueryTestServer.h"
#include "SimpleRequestHandler.h"
#include "BuiltInObjectTypeIDs.h"
#include "SimpleRequestResult.h"
#include "QueryBase.h"
#include "ExecuteQuery.h"
#include "InterfaceFunctions.h"
#include "GenericWork.h"
#include "DeleteSet.h"
#include "CatalogServer.h"
#include "SetScan.h"
#include "KeepGoing.h"
#include "DoneWithResult.h"
#include "PangeaStorageServer.h"
#include "TupleSetJobStage.h"
#include "AggregationJobStage.h"
#include "BroadcastJoinBuildHTJobStage.h"
#include "HashPartitionedJoinBuildHTJobStage.h"
#include <snappy.h>

namespace pdb {

FrontendQueryTestServer::FrontendQueryTestServer() {

  isStandalone = true;
  createOutputSet = true;
}

FrontendQueryTestServer::FrontendQueryTestServer(bool isStandalone, bool createOutputSet) {

  this->isStandalone = isStandalone;
  this->createOutputSet = createOutputSet;
}

FrontendQueryTestServer::~FrontendQueryTestServer() {}

void FrontendQueryTestServer::registerHandlers(PDBServer &forMe) {

  // to handle a request to execute a job stage for building hash tables for hash partition join
  forMe.registerHandler(
      HashPartitionedJoinBuildHTJobStage_TYPEID,
      make_shared<SimpleRequestHandler<HashPartitionedJoinBuildHTJobStage>>([&](
          Handle<HashPartitionedJoinBuildHTJobStage> request, PDBCommunicatorPtr sendUsingMe) {
        std::string errMsg;
        bool success;
        PDB_COUT << "Frontend got a request for HashPartitionedJoinBuildHTJobStage"
                 << std::endl;
        request->print();
#ifdef EANBLE_LARGE_GRAPH
        makeObjectAllocatorBlock(256 * 1024 * 1024, true);
#else
        makeObjectAllocatorBlock(32 * 1024 * 1024, true);
#endif
#ifdef PROFILING
        std::string out = getAllocator().printInactiveBlocks();
            std::cout << "HashPartitionedJoinBuildHTJobStage: print inactive blocks:" << std::endl;
            std::cout << out << std::endl;
#endif
        PDBCommunicatorPtr communicatorToBackend = make_shared<PDBCommunicator>();
        if (communicatorToBackend->connectToLocalServer(
            getFunctionality<PangeaStorageServer>().getLogger(),
            getFunctionality<PangeaStorageServer>().getPathToBackEndServer(),
            errMsg)) {
          std::cout << errMsg << std::endl;
          return std::make_pair(false, errMsg);
        }
        PDB_COUT << "Frontend connected to backend" << std::endl;

        Handle<HashPartitionedJoinBuildHTJobStage> newRequest =
            deepCopyToCurrentAllocationBlock<HashPartitionedJoinBuildHTJobStage>(request);
        PDB_COUT << "Created HashPartitionedJoinBuildHTJobStage object for forwarding"
                 << std::endl;

        // check input set
        // input set
        // restructure the input information
        std::string inDatabaseName = request->getSourceContext()->getDatabase();
        std::string inSetName = request->getSourceContext()->getSetName();
        Handle<SetIdentifier> sourceContext =
            makeObject<SetIdentifier>(inDatabaseName, inSetName);
        PDB_COUT << "Created SetIdentifier object for input" << std::endl;
        SetPtr inputSet = getFunctionality<PangeaStorageServer>().getSet(
            std::pair<std::string, std::string>(inDatabaseName, inSetName));
        if (inputSet == nullptr) {
          PDB_COUT << "FrontendQueryTestServer: input set doesn't exist in this machine"
                   << std::endl;
          // TODO: move data from other servers
          // temporarily, we simply return;
          // now, we send back the result
          Handle<SetIdentifier> result = makeObject<SetIdentifier>(inDatabaseName, inSetName);
          result->setNumPages(0);
          result->setPageSize(0);
          PDB_COUT << "Query is done without data. " << std::endl;
          // return the results
          if (!sendUsingMe->sendObject(result, errMsg)) {
            return std::make_pair(false, errMsg);
          }
          return std::make_pair(true, std::string("execution complete"));
        } else {
          inputSet->unpinBufferPage();
          getFunctionality<PangeaStorageServer>().cleanup();
        }
        sourceContext->setDatabaseId(inputSet->getDbID());
        sourceContext->setTypeId(inputSet->getTypeID());
        sourceContext->setSetId(inputSet->getSetID());
        sourceContext->setPageSize(inputSet->getPageSize());
        newRequest->setSourceContext(sourceContext);
        std::cout << "HashPartitioned data set size: " << inputSet->getNumPages() << " pages"
                  << std::endl;
        newRequest->setNumPages(inputSet->getNumPages());
        std::cout << "Input is set with setName=" << inSetName
                  << ", setId=" << inputSet->getSetID() << std::endl;


        // forward the request
        newRequest->print();

        if (inputSet->getNumPages() == 0) {
          std::cout << "WARNING: repartitioned data size is 0" << std::endl;
        }

        if (!communicatorToBackend->sendObject(newRequest, errMsg)) {
          std::cout << errMsg << std::endl;
          errMsg = std::string("can't send message to backend: ") + errMsg;
          success = false;
        } else {
          PDB_COUT << "Frontend sent request to backend" << std::endl;
          // wait for backend to finish.
          communicatorToBackend->getNextObject<SimpleRequestResult>(success, errMsg);
          if (!success) {
            std::cout << "Error waiting for backend to finish this job stage. " << errMsg
                      << std::endl;
            errMsg = std::string("backend failure: ") + errMsg;
          }
        }

        // forward result
        // now, we send back the result
        Handle<SetIdentifier> result = makeObject<SetIdentifier>(inDatabaseName, inSetName);
        result->setNumPages(inputSet->getNumPages());
        result->setPageSize(inputSet->getPageSize());
        if (success == true) {
          PDB_COUT << "Stage is done. " << std::endl;
          errMsg = std::string("execution complete");
        } else {
          std::cout << "Stage failed at server" << std::endl;
        }
        // return the results
        if (!sendUsingMe->sendObject(result, errMsg)) {
          return std::make_pair(false, errMsg);
        }
        if (success == false) {
          // TODO:restart backend
        }
        return std::make_pair(success, errMsg);
      }));

  // to handle a request to execute a job stage for building hash table for broadcast join
  forMe.registerHandler(
      BroadcastJoinBuildHTJobStage_TYPEID,
      make_shared<SimpleRequestHandler<BroadcastJoinBuildHTJobStage>>([&](
                                                                          Handle<BroadcastJoinBuildHTJobStage> request, PDBCommunicatorPtr sendUsingMe) {

                                                                        std::string errMsg;
                                                                        bool success;
                                                                        PDB_COUT << "Frontend got a request for BroadcastJoinBuildHTJobStage" << std::endl;
                                                                        request->print();
#ifdef ENABLE_LARGE_GRAPH
                                                                        makeObjectAllocatorBlock(256 * 1024 * 1024, true);
#else
                                                                        makeObjectAllocatorBlock(32 * 1024 * 1024, true);
#endif
#ifdef PROFILING
                                                                        std::string out = getAllocator().printInactiveBlocks();
            std::cout << "BroadcastJoinBuildHTJobStage: print inactive blocks:" << std::endl;
            std::cout << out << std::endl;
#endif
                                                                        PDBCommunicatorPtr communicatorToBackend = make_shared<PDBCommunicator>();
                                                                        if (communicatorToBackend->connectToLocalServer(
                                                                            getFunctionality<PangeaStorageServer>().getLogger(),
                                                                            getFunctionality<PangeaStorageServer>().getPathToBackEndServer(),
                                                                            errMsg)) {
                                                                          std::cout << errMsg << std::endl;
                                                                          return std::make_pair(false, errMsg);
                                                                        }
                                                                        PDB_COUT << "Frontend connected to backend" << std::endl;

                                                                        Handle<BroadcastJoinBuildHTJobStage> newRequest =
                                                                            deepCopyToCurrentAllocationBlock<BroadcastJoinBuildHTJobStage>(request);
                                                                        PDB_COUT << "Created BroadcastJoinBuildHTJobStage object for forwarding" << std::endl;

                                                                        // check input set
                                                                        // input set
                                                                        // restructure the input information
                                                                        std::string inDatabaseName = request->getSourceContext()->getDatabase();
                                                                        std::string inSetName = request->getSourceContext()->getSetName();
                                                                        Handle<SetIdentifier> sourceContext =
                                                                            makeObject<SetIdentifier>(inDatabaseName, inSetName);
                                                                        PDB_COUT << "Created SetIdentifier object for input" << std::endl;
                                                                        SetPtr inputSet = getFunctionality<PangeaStorageServer>().getSet(
                                                                            std::pair<std::string, std::string>(inDatabaseName, inSetName));
                                                                        if (inputSet == nullptr) {
                                                                          PDB_COUT << "FrontendQueryTestServer: input set doesn't exist in this machine"
                                                                                   << std::endl;
                                                                          // TODO: move data from other servers
                                                                          // temporarily, we simply return;
                                                                          // now, we send back the result
                                                                          Handle<SetIdentifier> result = makeObject<SetIdentifier>(inDatabaseName, inSetName);
                                                                          result->setNumPages(0);
                                                                          result->setPageSize(0);
                                                                          PDB_COUT << "Query is done without data. " << std::endl;
                                                                          // return the results
                                                                          if (!sendUsingMe->sendObject(result, errMsg)) {
                                                                            return std::make_pair(false, errMsg);
                                                                          }
                                                                          return std::make_pair(true, std::string("execution complete"));

                                                                        } else {
                                                                          inputSet->unpinBufferPage();
                                                                          getFunctionality<PangeaStorageServer>().cleanup();
                                                                        }
                                                                        sourceContext->setDatabaseId(inputSet->getDbID());
                                                                        sourceContext->setTypeId(inputSet->getTypeID());
                                                                        sourceContext->setSetId(inputSet->getSetID());
                                                                        sourceContext->setPageSize(inputSet->getPageSize());
                                                                        newRequest->setSourceContext(sourceContext);
                                                                        std::cout << "Broadcasted data set size: " << inputSet->getNumPages() << " pages"
                                                                                  << std::endl;
                                                                        newRequest->setNumPages(inputSet->getNumPages());
                                                                        PDB_COUT << "Input is set with setName=" << inSetName
                                                                                 << ", setId=" << inputSet->getSetID() << std::endl;


                                                                        // forward the request
                                                                        newRequest->print();

                                                                        if (inputSet->getNumPages() != 0) {

                                                                          if (!communicatorToBackend->sendObject(newRequest, errMsg)) {
                                                                            std::cout << errMsg << std::endl;
                                                                            errMsg = std::string("can't send message to backend: ") + errMsg;
                                                                            success = false;
                                                                          } else {
                                                                            PDB_COUT << "Frontend sent request to backend" << std::endl;
                                                                            // wait for backend to finish.
                                                                            communicatorToBackend->getNextObject<SimpleRequestResult>(success, errMsg);
                                                                            if (!success) {
                                                                              std::cout << "Error waiting for backend to finish this job stage. "
                                                                                        << errMsg << std::endl;
                                                                              errMsg = std::string("backend failure: ") + errMsg;
                                                                            }
                                                                          }
                                                                        } else {

                                                                          success = false;
                                                                          errMsg = std::string("Error: broadcasted data size is 0");
                                                                          std::cout << errMsg << std::endl;
                                                                        }

                                                                        // forward result
                                                                        // now, we send back the result
                                                                        Handle<SetIdentifier> result = makeObject<SetIdentifier>(inDatabaseName, inSetName);
                                                                        result->setNumPages(inputSet->getNumPages());
                                                                        result->setPageSize(inputSet->getPageSize());
                                                                        if (success == true) {
                                                                          PDB_COUT << "Stage is done. " << std::endl;
                                                                          errMsg = std::string("execution complete");
                                                                        } else {
                                                                          std::cout << "Stage failed at server" << std::endl;
                                                                        }
                                                                        // return the results
                                                                        if (!sendUsingMe->sendObject(result, errMsg)) {
                                                                          return std::make_pair(false, errMsg);
                                                                        }
                                                                        if (success == false) {
                                                                          // TODO:restart backend
                                                                        }
                                                                        return std::make_pair(success, errMsg);

                                                                      }

      ));


  // to handle a request to execute an aggregation stage
  forMe.registerHandler(
      AggregationJobStage_TYPEID,
      make_shared<SimpleRequestHandler<AggregationJobStage>>([&](
          Handle<AggregationJobStage> request, PDBCommunicatorPtr sendUsingMe) {
        std::string errMsg;
        bool success;
        PDB_COUT << "Frontend got a request for AggregationJobStage" << std::endl;
        request->print();
#ifdef ENABLE_LARGE_GRAPH
        makeObjectAllocatorBlock(256 * 1024 * 1024, true);
#else
        makeObjectAllocatorBlock(32 * 1024 * 1024, true);
#endif
#ifdef PROFILING
        std::string out = getAllocator().printInactiveBlocks();
            std::cout << "AggregationJobStage: print inactive blocks:" << std::endl;
            std::cout << out << std::endl;
#endif
        PDBCommunicatorPtr communicatorToBackend = make_shared<PDBCommunicator>();
        if (communicatorToBackend->connectToLocalServer(
            getFunctionality<PangeaStorageServer>().getLogger(),
            getFunctionality<PangeaStorageServer>().getPathToBackEndServer(),
            errMsg)) {
          std::cout << errMsg << std::endl;
          return std::make_pair(false, errMsg);
        }
        PDB_COUT << "Frontend connected to backend" << std::endl;
        Handle<AggregationJobStage> newRequest =
            makeObject<AggregationJobStage>(request->getStageId(),
                                            request->needsToMaterializeAggOut(),
                                            request->getAggComputation(),
                                            request->getNumNodePartitions());
        PDB_COUT << "Created AggregationJobStage object for forwarding" << std::endl;


        // input set
        // restructure the input information
        std::string inDatabaseName = request->getSourceContext()->getDatabase();
        std::string inSetName = request->getSourceContext()->getSetName();
        Handle<SetIdentifier> sourceContext =
            makeObject<SetIdentifier>(inDatabaseName, inSetName);
        PDB_COUT << "Created SetIdentifier object for input" << std::endl;
        SetPtr inputSet = getFunctionality<PangeaStorageServer>().getSet(
            std::pair<std::string, std::string>(inDatabaseName, inSetName));
        if (inputSet == nullptr) {
          PDB_COUT << "FrontendQueryTestServer: input set doesn't exist in this machine"
                   << std::endl;
          // TODO: move data from other servers
          // temporarily, we simply return;
          // now, we send back the result
          Handle<SetIdentifier> result =
              makeObject<SetIdentifier>(request->getSinkContext()->getDatabase(),
                                        request->getSinkContext()->getSetName());
          result->setNumPages(0);
          result->setPageSize(
              getFunctionality<PangeaStorageServer>().getConf()->getPageSize());
          PDB_COUT << "Query is done without data. " << std::endl;
          // return the results
          if (!sendUsingMe->sendObject(result, errMsg)) {
            return std::make_pair(false, errMsg);
          }
          return std::make_pair(true, std::string("execution complete"));

        } else {
          getFunctionality<PangeaStorageServer>().cleanup(false);
          PDB_COUT << "input set size=" << inputSet->getNumPages() << std::endl;
        }
        sourceContext->setDatabaseId(inputSet->getDbID());
        sourceContext->setTypeId(inputSet->getTypeID());
        sourceContext->setSetId(inputSet->getSetID());
        sourceContext->setPageSize(inputSet->getPageSize());
        newRequest->setSourceContext(sourceContext);
        PDB_COUT << "Input is set with setName=" << inSetName
                 << ", setId=" << inputSet->getSetID() << std::endl;


        // output set
        std::string outDatabaseName = request->getSinkContext()->getDatabase();
        std::string outSetName = request->getSinkContext()->getSetName();
        SetType outSetType = request->getSinkContext()->getSetType();
        bool isAggResult = request->getSinkContext()->isAggregationResult();
        success = true;
        // add the output set
        // check whether output set exists
        std::pair<std::string, std::string> outDatabaseAndSet =
            std::make_pair(outDatabaseName, outSetName);
        SetPtr outputSet = getFunctionality<PangeaStorageServer>().getSet(outDatabaseAndSet);
        if ((outputSet == nullptr) && (outSetType != PartitionedHashSetType)) {
          success = getFunctionality<PangeaStorageServer>().addSet(
              outDatabaseName, request->getOutputTypeName(), outSetName);
          outputSet = getFunctionality<PangeaStorageServer>().getSet(outDatabaseAndSet);
          PDB_COUT << "Output set is created in storage" << std::endl;
        }

        if (success == true) {
          newRequest->setOutputTypeName(request->getOutputTypeName());
          Handle<SetIdentifier> sinkContext =
              makeObject<SetIdentifier>(outDatabaseName, outSetName, outSetType, isAggResult);
          if (outSetType != PartitionedHashSetType) {
            sinkContext->setDatabaseId(outputSet->getDbID());
            sinkContext->setTypeId(outputSet->getTypeID());
            sinkContext->setSetId(outputSet->getSetID());
            sinkContext->setPageSize(outputSet->getPageSize());
          }
          newRequest->setSinkContext(sinkContext);
        } else {
          Handle<SetIdentifier> result =
              makeObject<SetIdentifier>(outDatabaseName, outSetName);
          result->setNumPages(0);
          result->setPageSize(0);
          PDB_COUT << "Query failed: not able to create output set. " << std::endl;
          // return the results
          if (!sendUsingMe->sendObject(result, errMsg)) {
            return std::make_pair(false, errMsg);
          }
          return std::make_pair(true,
                                std::string("Query failed: not able to create output set"));
        }

        newRequest->setJobId(request->getJobId());
        newRequest->setTotalMemoryOnThisNode(request->getTotalMemoryOnThisNode());
        // forward the request
        newRequest->print();

        if (!communicatorToBackend->sendObject(newRequest, errMsg)) {
          std::cout << errMsg << std::endl;
          errMsg = std::string("can't send message to backend: ") + errMsg;
          success = false;
        } else {
          PDB_COUT << "Frontend sent request to backend" << std::endl;
          // wait for backend to finish.
          communicatorToBackend->getNextObject<SimpleRequestResult>(success, errMsg);
          if (!success) {
            std::cout << "Error waiting for backend to finish this job stage. " << errMsg
                      << std::endl;
            errMsg = std::string("backend failure: ") + errMsg;
          }
        }

        // forward result
        // now, we send back the result

        Handle<SetIdentifier> result = makeObject<SetIdentifier>(outDatabaseName, outSetName);
        if (outSetType != PartitionedHashSetType) {
          result->setNumPages(outputSet->getNumPages());
          result->setPageSize(outputSet->getPageSize());
        } else {
          // if output is not materialized to user set, we roughly estimate the output using
          // the input.
          result->setNumPages(inputSet->getNumPages());
          result->setPageSize(inputSet->getPageSize());
        }
        if (success == true) {
          PDB_COUT << "Stage is done. " << std::endl;
          errMsg = std::string("execution complete");
        } else {
          std::cout << "Stage failed at server" << std::endl;
        }
        // return the results
        if (!sendUsingMe->sendObject(result, errMsg)) {
          return std::make_pair(false, errMsg);
        }
        if (success == false) {
          // TODO:restart backend
        }
        return std::make_pair(success, errMsg);

      }));


  // to handle a request to execute a tupleset pipeline stage
  forMe.registerHandler(
      TupleSetJobStage_TYPEID,
      make_shared<SimpleRequestHandler<TupleSetJobStage>>([&](Handle<TupleSetJobStage> request,
                                                              PDBCommunicatorPtr sendUsingMe) {
        std::string errMsg;
        bool success;
        PDB_COUT << "Frontend got a request for TupleSetJobStage" << std::endl;
        request->print();
#ifdef ENABLE_LARGE_GRAPH
        makeObjectAllocatorBlock(256 * 1024 * 1024, true);
#else
        makeObjectAllocatorBlock(32 * 1024 * 1024, true);
#endif
#ifdef PROFILING
        std::string out = getAllocator().printInactiveBlocks();
            std::cout << "TupleSetJobStage: print inactive blocks:" << std::endl;
            std::cout << out << std::endl;
#endif
        PDBCommunicatorPtr communicatorToBackend = make_shared<PDBCommunicator>();
        if (communicatorToBackend->connectToLocalServer(
            getFunctionality<PangeaStorageServer>().getLogger(),
            getFunctionality<PangeaStorageServer>().getPathToBackEndServer(),
            errMsg)) {
          std::cout << errMsg << std::endl;
          return std::make_pair(false, errMsg);
        }
        PDB_COUT << "Frontend connected to backend" << std::endl;
        Handle<TupleSetJobStage> newRequest =
            deepCopyToCurrentAllocationBlock<TupleSetJobStage>(request);

        PDB_COUT << "Created TupleSetJobStage object for forwarding" << std::endl;
        std::string inDatabaseName = request->getSourceContext()->getDatabase();
        std::string inSetName = request->getSourceContext()->getSetName();
        if (request->isInputAggHashOut() == false) {
          // restructure the input information
          Handle<SetIdentifier> sourceContext =
              makeObject<SetIdentifier>(inDatabaseName, inSetName);
          PDB_COUT << "Created SetIdentifier object for input" << std::endl;
          SetPtr inputSet = getFunctionality<PangeaStorageServer>().getSet(
              std::pair<std::string, std::string>(inDatabaseName, inSetName));
          if (inputSet == nullptr) {
            PDB_COUT << "FrontendQueryTestServer: input set doesn't exist in this machine"
                     << std::endl;
            // TODO: move data from other servers
            // temporarily, we simply return;
            // now, we send back the result
            Handle<SetIdentifier> result =
                makeObject<SetIdentifier>(request->getSinkContext()->getDatabase(),
                                          request->getSinkContext()->getSetName());
            result->setNumPages(0);
            result->setPageSize(
                getFunctionality<PangeaStorageServer>().getConf()->getPageSize());
            PDB_COUT << "Stage is done without input. " << std::endl;
            // return the results
            if (!sendUsingMe->sendObject(result, errMsg)) {
              return std::make_pair(false, errMsg);
            }
            return std::make_pair(true, std::string("execution complete"));

          } else {
            inputSet->unpinBufferPage();
            getFunctionality<PangeaStorageServer>().cleanup(false);
          }
          std::cout << "number of pages in set " << inSetName << " is "
                    << inputSet->getNumPages() << std::endl;
          if (inputSet->getNumPages() == 0) {
            PDB_COUT << "FrontendQueryTestServer: input set doesn't have any pages in this machine"
                     << std::endl;
            // TODO: move data from other servers
            // temporarily, we simply return;
            // now, we send back the result
            Handle<SetIdentifier> result =
                makeObject<SetIdentifier>(request->getSinkContext()->getDatabase(),
                                          request->getSinkContext()->getSetName());
            result->setNumPages(0);
            result->setPageSize(
                inputSet->getPageSize());
            PDB_COUT << "Stage is done without data. " << std::endl;
            // return the results
            if (!sendUsingMe->sendObject(result, errMsg)) {
              return std::make_pair(false, errMsg);
            }
            return std::make_pair(true, std::string("execution complete"));

          }
          sourceContext->setDatabaseId(inputSet->getDbID());
          sourceContext->setTypeId(inputSet->getTypeID());
          sourceContext->setSetId(inputSet->getSetID());
          sourceContext->setPageSize(inputSet->getPageSize());
          newRequest->setSourceContext(sourceContext);
          PDB_COUT << "Input is set with setName=" << inSetName
                   << ", setId=" << inputSet->getSetID() << std::endl;
        } else {
          PDB_COUT << "Input is hash table output from aggregation" << std::endl;
        }

        std::string outDatabaseName = request->getSinkContext()->getDatabase();
        std::string outSetName = request->getSinkContext()->getSetName();
        success = true;
        // add the output set
        // check whether output set exists
        std::pair<std::string, std::string> outDatabaseAndSet =
            std::make_pair(outDatabaseName, outSetName);
        SetPtr outputSet = getFunctionality<PangeaStorageServer>().getSet(outDatabaseAndSet);
        if (outputSet == nullptr) {
          success = getFunctionality<PangeaStorageServer>().addSet(
              outDatabaseName, request->getOutputTypeName(), outSetName);
          outputSet = getFunctionality<PangeaStorageServer>().getSet(outDatabaseAndSet);
          PDB_COUT << "Output set is created in storage with database=" << outDatabaseName
                   << ", set=" << outSetName << ", type=IntermediateData" << std::endl;
        }

        if (success == true) {
          Handle<SetIdentifier> sinkContext =
              makeObject<SetIdentifier>(outDatabaseName, outSetName);
          PDB_COUT << "Created SetIdentifier object for output with setName=" << outSetName
                   << ", setId=" << outputSet->getSetID() << std::endl;
          sinkContext->setDatabaseId(outputSet->getDbID());
          sinkContext->setTypeId(outputSet->getTypeID());
          sinkContext->setSetId(outputSet->getSetID());
          sinkContext->setPageSize(outputSet->getPageSize());
          newRequest->setSinkContext(sinkContext);
          newRequest->setOutputTypeName(request->getOutputTypeName());

        } else {
          Handle<SetIdentifier> result =
              makeObject<SetIdentifier>(outDatabaseName, outSetName);
          result->setNumPages(0);
          result->setPageSize(
              getFunctionality<PangeaStorageServer>().getConf()->getPageSize());
          PDB_COUT << "Stage failed: not able to create output set. " << std::endl;
          // return the results
          if (!sendUsingMe->sendObject(result, errMsg)) {
            return std::make_pair(false, errMsg);
          }
          return std::make_pair(true,
                                std::string("Query failed: not able to create output set"));
        }

        bool needsRemoveCombinerSet = false;
        SetPtr combinerSet = nullptr;
        std::string combinerDatabaseName;
        std::string combinerSetName;
        if (request->getCombinerContext() != nullptr) {
          combinerDatabaseName = request->getCombinerContext()->getDatabase();
          combinerSetName = request->getCombinerContext()->getSetName();
          success = true;
          // add the combiner set
          // check whether the combiner set exists
          std::pair<std::string, std::string> combinerDatabaseAndSet =
              std::make_pair(combinerDatabaseName, combinerSetName);
          combinerSet =
              getFunctionality<PangeaStorageServer>().getSet(combinerDatabaseAndSet);
          if (combinerSet == nullptr) {
            success = getFunctionality<PangeaStorageServer>().addSet(combinerDatabaseName,
                                                                     combinerSetName);
            combinerSet =
                getFunctionality<PangeaStorageServer>().getSet(combinerDatabaseAndSet);
            needsRemoveCombinerSet = true;
            PDB_COUT << "Combiner set is created in storage" << std::endl;
          }
        }

        if (success == true) {
          if (combinerSet != nullptr) {
            Handle<SetIdentifier> combinerContext =
                makeObject<SetIdentifier>(combinerDatabaseName, combinerSetName);
            PDB_COUT << "Created SetIdentifier object for combiner with setName="
                     << combinerSetName << ", setId=" << combinerSet->getSetID()
                     << std::endl;
            combinerContext->setDatabaseId(combinerSet->getDbID());
            combinerContext->setTypeId(combinerSet->getTypeID());
            combinerContext->setSetId(combinerSet->getSetID());
            combinerContext->setPageSize(combinerSet->getPageSize());
            newRequest->setCombinerContext(combinerContext);
          } else {
            newRequest->setCombinerContext(nullptr);
          }

          newRequest->print();

          if (!communicatorToBackend->sendObject(newRequest, errMsg)) {
            std::cout << errMsg << std::endl;
            errMsg = std::string("can't send message to backend: ") + errMsg;
            success = false;
          } else {
            PDB_COUT << "Frontend sent request to backend" << std::endl;
            // wait for backend to finish.
            communicatorToBackend->getNextObject<SimpleRequestResult>(success, errMsg);
            if (!success) {
              std::cout << "Error waiting for backend to finish this job stage. "
                        << errMsg << std::endl;
              errMsg = std::string("backend failure: ") + errMsg;
            }
          }
        }

        if (needsRemoveCombinerSet == true) {
          // remove combiner set
          getFunctionality<PangeaStorageServer>().removeSet(combinerDatabaseName,
                                                            combinerSetName);
        }

        // now, we send back the result
        Handle<SetIdentifier> result = makeObject<SetIdentifier>(outDatabaseName, outSetName);
        result->setNumPages(outputSet->getNumPages());
        result->setPageSize(outputSet->getPageSize());
        if (success == true) {
          PDB_COUT << "Stage is done. " << std::endl;
          errMsg = std::string("execution complete");
        } else {
          std::cout << "Stage failed at server" << std::endl;
        }
        // return the results
        if (!sendUsingMe->sendObject(result, errMsg)) {
          return std::make_pair(false, errMsg);
        }
        if (success == false) {
          // TODO:restart backend
        }
        return std::make_pair(success, errMsg);

      }));


  // handle a request to delete a file
  forMe.registerHandler(
      DeleteSet_TYPEID,
      make_shared<SimpleRequestHandler<DeleteSet>>([&](Handle<DeleteSet> request,
                                                       PDBCommunicatorPtr sendUsingMe) {

        const UseTemporaryAllocationBlock tempBlock{1024 * 128};
        {
          std::string errMsg;
          if  ((!getFunctionality<CatalogServer>().getCatalog()->removeSet(request->whichDatabase(), request->whichSet(), errMsg)) ||
               (!getFunctionality<PangeaStorageServer>().removeSet(request->whichDatabase(), request->whichSet()))) {

            Handle<SimpleRequestResult> result = makeObject<SimpleRequestResult>(false, std::string("error attempting to delete set: " + errMsg));
            if (!sendUsingMe->sendObject(result, errMsg)) {
              return std::make_pair(false, errMsg);
            }
          } else {
            Handle<SimpleRequestResult> result = makeObject<SimpleRequestResult>(
                true, std::string("successfully deleted set"));
            if (!sendUsingMe->sendObject(result, errMsg)) {
              return std::make_pair(false, errMsg);
            }
          }
          return std::make_pair(true, std::string("delete complete"));
        }
      }));

  // handle a request to iterate through a file
  forMe.registerHandler(
      SetScan_TYPEID,
      make_shared<SimpleRequestHandler<SetScan>>([&](Handle<SetScan> request,
                                                     PDBCommunicatorPtr sendUsingMe) {

        // for error handling
        std::string errMsg;

        // this is the number of pages
        std::string whichDatabase = request->getDatabase();
        std::string whichSet = request->getSetName();
        PDB_COUT << "we are now iterating set:" << whichSet << std::endl;
        // and keep looping while someone wants to get the output
        SetPtr loopingSet = getFunctionality<PangeaStorageServer>().getSet(
            std::make_pair(whichDatabase, whichSet));
        if (loopingSet == nullptr) {
          errMsg = "FATAL ERROR in handling SetScan request: set doesn't exist";
          std::cout << errMsg << std::endl;
          return std::make_pair(false, errMsg);
        } else {
          std::cout << "To scan set " << whichDatabase << ":" << whichSet <<
                    " with " << loopingSet->getNumPages() << " pages." << std::endl;
        }
        loopingSet->setPinned(true);
        vector<PageIteratorPtr> *pageIters = loopingSet->getIterators();
        // loop through all pages
        int numIterators = pageIters->size();
        for (int i = 0; i < numIterators; i++) {
          PageIteratorPtr iter = pageIters->at(i);
          while (iter->hasNext()) {
            PDBPagePtr nextPage = iter->next();
            // send the relevant page.
            if (nextPage != nullptr) {
              Record<Vector<Handle<Object>>> *myRec =
                  (Record<Vector<Handle<Object>>> *) (nextPage->getBytes());
              Handle<Vector<Handle<Object>>> inputVec = myRec->getRootObject();
              if (inputVec == nullptr) {
                std::cout << "no vector in this page" << std::endl;
                // to evict this page
                PageCachePtr cache = getFunctionality<PangeaStorageServer>().getCache();
                CacheKey key;
                key.dbId = nextPage->getDbID();
                key.typeId = nextPage->getTypeID();
                key.setId = nextPage->getSetID();
                key.pageId = nextPage->getPageID();
                cache->decPageRefCount(key);
#ifndef REMOVE_SET_WITH_EVICTION
                cache->evictPage(key);  // try to modify this to something like
                                                    // evictPageWithoutFlush() or clear set in the
                                                    // end.
#endif
                continue;
              }

              int vecSize = inputVec->size();
              if (vecSize != 0) {
                const UseTemporaryAllocationBlock tempBlock{2048};
#ifdef ENABLE_COMPRESSION
                char *newRecord = (char *) calloc(nextPage->getSize(), 1);
                myRec = getRecord(inputVec, newRecord, nextPage->getSize());
                char *compressedBytes =
                    new char[snappy::MaxCompressedLength(myRec->numBytes())];
                size_t compressedSize;
                snappy::RawCompress((char *) (myRec),
                                    myRec->numBytes(),
                                    compressedBytes,
                                    &compressedSize);
                std::cout << "Frontend=>Client: size before compression is "
                          << myRec->numBytes() << " and size after compression is "
                          << compressedSize << std::endl;
                sendUsingMe->sendBytes(compressedBytes, compressedSize, errMsg);

                delete[] compressedBytes;
                free(newRecord);
#else
                if (!sendUsingMe->sendBytes(
                                    nextPage->getBytes(), nextPage->getSize(), errMsg)) {
                                return std::make_pair(false, errMsg);
                            }
#endif
                // see whether or not the client wants to see more results
                bool success;
                if (sendUsingMe->getObjectTypeID() != DoneWithResult_TYPEID) {
                  Handle<KeepGoing> temp =
                      sendUsingMe->getNextObject<KeepGoing>(success, errMsg);
                  PDB_COUT << "Keep going" << std::endl;
                  if (!success)
                    return std::make_pair(false, errMsg);
                } else {
                  Handle<DoneWithResult> temp =
                      sendUsingMe->getNextObject<DoneWithResult>(success, errMsg);
                  PDB_COUT << "Done" << std::endl;
                  if (!success)
                    return std::make_pair(false, errMsg);
                  else
                    return std::make_pair(true, std::string("everything OK!"));
                }
              }
              // to evict this page
              PageCachePtr cache = getFunctionality<PangeaStorageServer>().getCache();
              CacheKey key;
              key.dbId = nextPage->getDbID();
              key.typeId = nextPage->getTypeID();
              key.setId = nextPage->getSetID();
              key.pageId = nextPage->getPageID();
              cache->decPageRefCount(key);
#ifndef REMOVE_SET_WITH_EVICTION
              cache->evictPage(key);  // try to modify this to something like
                                                // evictPageWithoutFlush() or clear set in the end.
#endif
            } else {
              PDB_COUT << "We've got a null page!!!" << std::endl;
            }
          }
        }
        loopingSet->setPinned(false);
        delete pageIters;
        // tell the caller we are done
        const UseTemporaryAllocationBlock tempBlock{1024};
        Handle<DoneWithResult> temp = makeObject<DoneWithResult>();
        if (!sendUsingMe->sendObject(temp, errMsg)) {
          return std::make_pair(false, "could not send done message: " + errMsg);
        }
        // we got to here means success!!  We processed the query, and got all of the results
        std::cout << "We have finished scanning this set" << std::endl;
        return std::make_pair(true, std::string("query completed!!"));
      }));
}

}

#endif
