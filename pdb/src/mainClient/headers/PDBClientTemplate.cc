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
#ifndef PDB_CLIENT_TEMPLATE_CC
#define PDB_CLIENT_TEMPLATE_CC

#include "PDBClient.h"

namespace pdb {

    template <class DataType>
    bool PDBClient::createSet(const std::string &databaseName,
                              const std::string &setName, size_t pageSize) {

      bool result = distributedStorageClient->createSet<DataType>(
            databaseName, setName, returnedMsg, pageSize);

      if (result==false) {
          errorMsg = "Not able to create set: " + returnedMsg;
          exit(-1);
      } else {
          cout << "Created set.\n";
      }
      return result;
    }

    template <class DataType>
    bool PDBClient::createSet(const std::string &databaseName,
                              const std::string &setName) {

      bool result = distributedStorageClient->createSet<DataType>(
          databaseName, setName, returnedMsg, DEFAULT_PAGE_SIZE);

      if (result==false) {
          errorMsg = "Not able to create set: " + returnedMsg;
          exit(-1);
      } else {
          cout << "Created set.\n";
      }
      return result;
    }

    template <class DataType>
    bool PDBClient::sendData(std::pair<std::string, std::string> setAndDatabase,
                             Handle<Vector<Handle<DataType>>> dataToSend) {

      bool result = dispatcherClient->sendData<DataType>(setAndDatabase, dataToSend,
                                                 returnedMsg);

      if (result==false) {
          errorMsg = "Not able to send data: " + returnedMsg;
          exit(-1);
      } else {
          cout << "Data sent.\n";
      }
      return result;
    }

    template <class DataType>
    bool PDBClient::sendBytes(std::pair<std::string, std::string> setAndDatabase,
                   char* bytes,
                   size_t numBytes){

      bool result = dispatcherClient->sendBytes<DataType>(
                setAndDatabase,
                bytes,
                numBytes,
                returnedMsg);

        if (result==false) {
            errorMsg = "Not able to send bytes: " + returnedMsg;
            exit(-1);
        } else {
            cout << "Bytes sent.\n";
        }
        return result;
    }

    template <class... Types>
    bool PDBClient::executeComputations(Handle<Computation> firstParam,
                                        Handle<Types>... args) {

      bool result = queryClient->executeComputations(returnedMsg, firstParam, args...);

      if (result==false) {
          errorMsg = "Not able to execute computations: " + returnedMsg;
          exit(-1);
      }
      return result;
    }

    template <class Type>
    SetIterator<Type> PDBClient::getSetIterator(std::string databaseName,
                                                std::string setName) {

      return queryClient->getSetIterator<Type>(databaseName, setName);
    }

    template <class KeyClass, class ValueClass>
    bool PDBClient::partitionSet(std::pair<std::string, std::string> inputSet, 
                                 std::pair<std::string, std::string> outputSet, 
                                 Handle<PartitionComp<KeyClass, ValueClass>> partitionComp) {

        std::shared_ptr<Partitioner<KeyClass, ValueClass>> myPartitioner 
                          = std::make_shared<Partitioner<KeyClass, ValueClass>>(inputSet, outputSet);
        return myPartitioner->partition(errorMsg, this->queryClient, partitionComp);

    }

    template <class KeyClass, class ValueClass>
    bool PDBClient::partitionAndTransformSet(std::pair<std::string, std::string> inputSet,
                                 std::pair<std::string, std::string> outputSet,
                                 Handle<PartitionTransformationComp<KeyClass, ValueClass>> partitionComp) {

        std::shared_ptr<Partitioner<KeyClass, ValueClass>> myPartitioner
                          = std::make_shared<Partitioner<KeyClass, ValueClass>>(inputSet, outputSet);
        return myPartitioner->partitionWithTransformation(errorMsg, this->queryClient, partitionComp);

    }


}
#endif
