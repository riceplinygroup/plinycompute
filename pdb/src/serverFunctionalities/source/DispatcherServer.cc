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

#ifndef DISPATCHER_SERVER_CC
#define DISPATCHER_SERVER_CC

#include "DispatcherServer.h"
#include "CatalogServer.h"
#include "PDBDebug.h"
#include "SimpleRequestHandler.h"
#include "SimpleRequestResult.h"
#include "SimpleSendBytesRequest.h"
#include "DispatcherAddData.h"
#include "BuiltInObjectTypeIDs.h"
#include "QuerySchedulerServer.h"
#include "Statistics.h"
#include "PartitionPolicyFactory.h"
#include "DispatcherRegisterPartitionPolicy.h"
#include <snappy.h>

namespace pdb {

DispatcherServer::DispatcherServer(PDBLoggerPtr &logger, shared_ptr<StatisticsDB> &statisticsDB) {

  // init
  this->logger = logger;
  this->statisticsDB = statisticsDB;
  this->storageNodes = pdb::makeObject<Vector<Handle<NodeDispatcherData>>>();
  this->partitionPolicies = std::map<std::pair<std::string, std::string>, PartitionPolicyPtr>();

  // there are 0 requests that we process when we start
  numRequestsInProcessing = 0;
}

void DispatcherServer::registerHandlers(PDBServer &forMe) {

  forMe.registerHandler(
      DispatcherAddData_TYPEID,
      make_shared<SimpleRequestHandler<DispatcherAddData>>([&](Handle<DispatcherAddData> request,
                                                               PDBCommunicatorPtr sendUsingMe) {
        // lock the mutex
        {
          // lock the mutex to access the @see numRequestsInProcessing
          std::unique_lock<std::mutex> lk(mutex);

          // wait till we have
          cv.wait(lk, [this] { return this->numRequestsInProcessing <= MAX_CONCURRENT_REQUESTS; });

          // update the number of requests
          numRequestsInProcessing += 1;
        }

        // grab the number of bytes
        size_t numBytes = sendUsingMe->getSizeOfNextObject();

        // depending on whether we are doing compression
        #ifdef ENABLE_COMPRESSION
          auto ret = dispatchCompressed(request, sendUsingMe);
        #else
          auto ret = dispatch(request, sendUsingMe);
        #endif

        // update stats
        {
          // lock the mutex to access the @see numRequestsInProcessing
          std::unique_lock<std::mutex> lk(mutex);

          StatisticsPtr stats = getFunctionality<QuerySchedulerServer>().getStats();
          if (stats == nullptr) {
            getFunctionality<QuerySchedulerServer>().collectStats();
            stats = getFunctionality<QuerySchedulerServer>().getStats();
          }

          size_t oldNumBytes = stats->getNumBytes(request->getDatabaseName(), request->getSetName());
          size_t newNumBytes = oldNumBytes + numBytes;
          stats->setNumBytes(request->getDatabaseName(), request->getSetName(), newNumBytes);

          // reduce the number of requests we are processing since we finished
          numRequestsInProcessing = numRequestsInProcessing - 1;

          // inform everybody who is waiting that we just finished
          cv.notify_all();
        }

        return ret;
      }));

  forMe.registerHandler(
      DispatcherRegisterPartitionPolicy_TYPEID,
      make_shared<SimpleRequestHandler<DispatcherRegisterPartitionPolicy>>(
          [&](Handle<DispatcherRegisterPartitionPolicy> request, PDBCommunicatorPtr sendUsingMe) {

            PDB_COUT << "Registering partition policy for set " << request->getSetName() << ":"
                     << request->getDatabaseName() << std::endl;

            std::string errMsg;
            bool res = true;

            registerSet(std::pair<std::string, std::string>(request->getSetName(),
                                                            request->getDatabaseName()),
                        PartitionPolicyFactory::buildPartitionPolicy(request->getPolicy()));

            Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);
            res = sendUsingMe->sendObject(response, errMsg);

            return make_pair(res, errMsg);
          }));
}

void DispatcherServer::registerStorageNodes(
    Handle<Vector<Handle<NodeDispatcherData>>> storageNodes) {
  this->storageNodes = storageNodes;
  for (int i = 0; i < storageNodes->size(); i++) {
    auto node = (*storageNodes)[i];
    PDB_COUT << "Dispatcher register node: " << node->getAddress() << " : " << node->getPort()
             << std::endl;
  }

  for (auto const &partitionPolicy : partitionPolicies) {
    partitionPolicy.second->updateStorageNodes(storageNodes);
  }
}

void DispatcherServer::registerSet(std::pair<std::string, std::string> setAndDatabase,
                                   PartitionPolicyPtr partitionPolicy) {
  if (partitionPolicies.find(setAndDatabase) != partitionPolicies.end()) {
    PDB_COUT << "Updating old set" << setAndDatabase.first << ":" << setAndDatabase.second
             << std::endl;
  } else {
    PDB_COUT << "Found new set: " << setAndDatabase.first << ":" << setAndDatabase.second
             << std::endl;
  }
  partitionPolicies.insert(std::pair<std::pair<std::string, std::string>, PartitionPolicyPtr>(setAndDatabase,
                                                                                              partitionPolicy));
  partitionPolicies[setAndDatabase]->updateStorageNodes(storageNodes);
}

bool DispatcherServer::dispatchData(std::pair<std::string, std::string> setAndDatabase,
                                    std::string type,
                                    Handle<Vector<Handle<Object>>> toDispatch) {
  // TODO: Implement this

  if (partitionPolicies.find(setAndDatabase) == partitionPolicies.end()) {
    PDB_COUT << "No partition policy was found for set: " << setAndDatabase.first << ":"
             << setAndDatabase.second << std::endl;
    PDB_COUT << "Defaulting to random policy" << std::endl;
    registerSet(setAndDatabase, PartitionPolicyFactory::buildDefaultPartitionPolicy());
    return dispatchData(setAndDatabase, type, toDispatch);
  } else {
    auto mappedPartitions = partitionPolicies[setAndDatabase]->partition(toDispatch);
    PDB_COUT << "mappedPartitions size = " << mappedPartitions->size() << std::endl;
    for (auto const &pair : (*mappedPartitions)) {
      if (!sendData(setAndDatabase, type, findNode(pair.first), pair.second)) {
        return false;
      }
    }
    return true;
  }
}

bool DispatcherServer::dispatchBytes(std::pair<std::string, std::string> setAndDatabase,
                                     std::string type,
                                     char *bytes,
                                     size_t numBytes) {
  // TODO: Implement this

  if (partitionPolicies.find(setAndDatabase) == partitionPolicies.end()) {
    PDB_COUT << "No partition policy was found for set: " << setAndDatabase.first << ":"
             << setAndDatabase.second << std::endl;
    PDB_COUT << "Defaulting to random policy" << std::endl;
    registerSet(setAndDatabase, PartitionPolicyFactory::buildDefaultPartitionPolicy());
    return dispatchBytes(setAndDatabase, type, bytes, numBytes);
  } else {
    auto mappedPartitions = partitionPolicies[setAndDatabase]->partition(nullptr);
    PDB_COUT << "mappedPartitions size = " << mappedPartitions->size() << std::endl;
    for (auto const &pair : (*mappedPartitions)) {
      if (!sendBytes(setAndDatabase, type, findNode(pair.first), bytes, numBytes)) {
        return false;
      }
    }
    return true;
  }
}

bool DispatcherServer::sendData(std::pair<std::string, std::string> setAndDatabase,
                                std::string type,
                                Handle<NodeDispatcherData> destination,
                                Handle<Vector<Handle<Object>>> toSend) {

  PDB_COUT << "Sending data to " << destination->getPort() << " : " << destination->getAddress() << std::endl;
  std::string err;
  auto storageClient = StorageClient(destination->getPort(), destination->getAddress(), logger);
  if (!storageClient.storeData(toSend, setAndDatabase.second, setAndDatabase.first, type, err)) {
    PDB_COUT << "Not able to store data: " << err << std::endl;
    return false;
  }
  return true;
}

bool DispatcherServer::sendBytes(std::pair<std::string, std::string> setAndDatabase,
                                 std::string type,
                                 Handle<NodeDispatcherData> destination,
                                 char *bytes,
                                 size_t numBytes) {
#ifndef ENABLE_COMPRESSION
  std::cout << "Now only objects or compressed bytes can be dispatched!!" << std::endl;
#endif
  int port = destination->getPort();
  std::string address = destination->getAddress();
  std::string databaseName = setAndDatabase.second;
  std::string setName = setAndDatabase.first;
  std::string errMsg;
  std::cout << "store compressed bytes to address=" << address << " and port=" << port
            << ", with compressed byte size = " << numBytes << " to database=" << databaseName
            << " and set=" << setName << " and type = IntermediateData" << std::endl;
  return simpleSendBytesRequest<StorageAddData, SimpleRequestResult, bool>(
      logger,
      port,
      address,
      false,
      1024,
      [&](Handle<SimpleRequestResult> result) {
        if (result != nullptr)
          if (!result->getRes().first) {
            logger->error("Error sending data: " + result->getRes().second);
            errMsg = "Error sending data: " + result->getRes().second;
          }
        return true;
      },
      bytes,
      numBytes,
      databaseName,
      setName,
      "IntermediateData",
      false,
      true,
      true,
      true);
}

Handle<NodeDispatcherData> DispatcherServer::findNode(NodeID nodeId) {
  for (int i = 0; i < storageNodes->size(); i++) {
    auto storageNode = (*storageNodes)[i];
    if (storageNode->getNodeId() == nodeId) {
      return storageNode;
    }
  }
  return nullptr;
}

void DispatcherServer::waitAllRequestsProcessed() {

  // lock the mutex to access the @see numRequestsInProcessing
  std::unique_lock<std::mutex> lk(mutex);

  // wait until we finished with all requests
  cv.wait(lk, [ this ] { return this->numRequestsInProcessing <= 0; });
}

pair<bool, string> DispatcherServer::dispatch(Handle<DispatcherAddData> &request,
                                              PDBCommunicatorPtr &sendUsingMe) {

  std::string errMsg;
  bool res = true;
  PDB_COUT << "DispatcherAddData handler running" << std::endl;
  // Receive the data to send
  size_t numBytes = sendUsingMe->getSizeOfNextObject();
  std::cout << "Dispacher received numBytes = " << numBytes << std::endl;
  Handle<Vector<Handle<Object>>> dataToSend;
  char *tempPage = nullptr;
  char *readToHere = nullptr;

  if (!request->isShallowCopy()) {
    const UseTemporaryAllocationBlock tempBlock{numBytes + 65535};
    dataToSend = sendUsingMe->getNextObject<Vector<Handle<Object>>>(res, errMsg);
  } else {

    readToHere = (char*) malloc(numBytes);
    sendUsingMe->receiveBytes(readToHere, errMsg);
  }

  if (dataToSend->size() == 0) {
    errMsg = "Warning: client attemps to store zero object vector";
    Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(false, errMsg);
    res = sendUsingMe->sendObject(response, errMsg);
    std::cout << errMsg << std::endl;
    return make_pair(false, errMsg);

  } else {
    std::cout << "Dispatch to send vector size = " << dataToSend->size() << std::endl;
  }

  Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);
  res = sendUsingMe->sendObject(response, errMsg);

  if (!request->isShallowCopy()) {
    dispatchData(std::pair<std::string, std::string>(request->getSetName(),
                                                     request->getDatabaseName()),
                 request->getTypeName(),
                 dataToSend);
  } else {

    dispatchBytes(std::pair<std::string, std::string>(request->getSetName(),
                                                            request->getDatabaseName()),
                        request->getTypeName(),
                        readToHere,
                        numBytes);
    free(readToHere);
  }

  return make_pair(res, errMsg);
}

pair<bool, string> DispatcherServer::dispatchCompressed(Handle<DispatcherAddData> &request,
                                                        PDBCommunicatorPtr &sendUsingMe) {
  std::string errMsg;
  bool res = true;
  PDB_COUT << "DispatcherAddData handler running" << std::endl;

  // Receive the data to send
  size_t numBytes = sendUsingMe->getSizeOfNextObject();
  std::cout << "Dispacher received numBytes = " << numBytes << std::endl;
  Handle<Vector<Handle<Object>>> dataToSend;
  char *tempPage = nullptr;
  char *readToHere = nullptr;
  if (!request->isShallowCopy()) {
    const UseTemporaryAllocationBlock tempBlock{numBytes + 65535};
    dataToSend = sendUsingMe->getNextObject<Vector<Handle<Object>>>(res, errMsg);
  } else {

    tempPage = new char[numBytes];
    sendUsingMe->receiveBytes(tempPage, errMsg);

    size_t uncompressedSize = 0;
    snappy::GetUncompressedLength(tempPage, numBytes, &uncompressedSize);
    readToHere = (char *) malloc(uncompressedSize);
    snappy::RawUncompress(tempPage, numBytes, (char *) (readToHere));
    auto *myRecord = (Record<Vector<Handle<Object>>> *) readToHere;
    dataToSend = myRecord->getRootObject();
  }

  if (dataToSend->size() == 0) {
    errMsg = "Warning: client attemps to store zero object vector";
    Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(false, errMsg);
    res = sendUsingMe->sendObject(response, errMsg);
    std::cout << errMsg << std::endl;
    return make_pair(false, errMsg);

  } else {
    std::cout << "Dispatch to send vector size = " << dataToSend->size() << std::endl;
  }

  Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);
  res = sendUsingMe->sendObject(response, errMsg);

  if (!request->isShallowCopy()) {
    dispatchData(std::pair<std::string, std::string>(request->getSetName(), request->getDatabaseName()),
                 request->getTypeName(),
                 dataToSend);
  } else {

    dispatchBytes(std::pair<std::string, std::string>(request->getSetName(),
                                                      request->getDatabaseName()),
                  request->getTypeName(),
                  tempPage,
                  numBytes);

    free(tempPage);
    free(readToHere);
  }

  return make_pair(res, errMsg);
}

}

#endif
