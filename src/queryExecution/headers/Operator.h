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

#ifndef OPERATOR_H
#define OPERATOR_H

//by Jia, Sept 2016

namespace pdb {


// this pure virtual class defines the common interfaces of operators;
// each query type can be split into a graph of operators;
// the system supports a fixed set of operators for query execution at backend.
// it has several differences with the SimpleSingleTableQueryProcessor interface:
//     - it is batch oriented
//     - it can describe join, aggregation, simple selection and so on.



class Operator {

public:

    // must be called before the operator is asked to do anything
    virtual void initialize() = 0;

    // loads up another batch of the i-th input data to process
    virtual void loadInputBatch(int inputDataId, void * batchToProcess) = 0;

    // loads up another batch of the i-th output data to fill
    virtual void loadOutputBatch(int outputDataId, void * batchToFill, size_t numBytesInBatch) = 0;

    // attempts to fill the next output batch of the i-th output data. returns true if it can. if it cannot, returns false, and the next call to loadInputBatch should be made
    virtual bool fillNextOutputBatch(int outputDataId) = 0;

    // must be called after all of the input pages have been processed
    virtual void finalize () = 0;



};



}










#endif
