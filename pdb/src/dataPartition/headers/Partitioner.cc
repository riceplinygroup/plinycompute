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

#ifndef PARTITIONER_CC
#define PARTITIONER_CC



namespace pdb {

template<class OutputClass, class InputClass>
Partitioner<OutputClass, InputClass> :: Partitioner (std::pair<std::string, std::string> inputDatabaseAndSet,
                            std::pair<std::string, std::string> outputDatabaseAndSet,
                            Handle<PartitionComp<OutputClass, InputClass>> partitionComp) {

        this->inputDatabaseAndSet = inputDatabaseAndSet;
        this->outputDatabaseAndSet = outputDatabaseAndSet;
        this->partitionComp = partitionComp;

}

template<class OutputClass, class InputClass>
bool Partitioner<OutputClass, InputClass> :: partition ( std::string & errMsg, std::shared_ptr<pdb::QueryClient> queryClient) {

        /* Step 1. to check whether the input set and output set exists, if not we return false
         * TODO: we do not have such function at master yet
         */

        /* Step 2. to check whether partitionComp is null, if yes, we return false */
        if (this->partitionComp == nullptr) {
            errMsg = "Error: null partitionComp";
            return false;
        }

        /* Step 3. to check whether queryClient is null, if yes, we return false */
        if (this->queryClient == nullptr) {
            errMsg = "Error: null queryClient";
            return false;
        }
        
        const UseTemporaryAllocationBlock myBlock{256 * 1024 * 1024};

        /* Step 4. to deep copy the partition computation */
        Handle<PartitionComp<OutputClass, InputClass>> curPartitionComp = deepCopyToCurrentAllocationBlock(partitionComp);
          
        /* Step 5. to create a scanner computation */
        Handle<ScanUserSet<InputClass>> scanner = makeObject<ScanUserSet<InputClass>> (inputDatabaseAndSet.first, inputDatabaseAndSet.second);

        /* Step 6. to create a writer computation */
        Handle<WriteUserSet<OutputClass>> writer = makeObject<WriteUserSet<OutputClass>> (inputDatabaseAndSet.first, inputDatabaseAndSet.second);

        /* Step 7. to compose a query graph */
        this->partitionComp->setInput(scanner);
        this->writer->setInput(curPartitionComp);

        /* Step 8. to get the tcap string */
        queryClient->setQueryGraph(this->writer);
        std::vector<Handle<Computation>> computations;
        std::string tcapString = queryClient->getTCAP(computations);

        /* Step 9. to register the input-output mapping as well as the tcap string with StatsDB */
        /* TODO */


        /* Step 10. to execute the partition computation */ 
        return queryClient->executeComputations(errMsg, tcapString, computations);
      
}


}

#endif
