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

#ifndef SCAN_USER_SET_H
#define SCAN_USER_SET_H

//by Jia, Mar 2017

#include "Computation.h"
#include "PageCircularBufferIterator.h"
#include "VectorTupleSetIterator.h"
#include "PDBString.h"

namespace pdb {

template <class OutputClass>
class ScanUserSet : public Computation {

public:

        ENABLE_DEEP_COPY

        void initialize() {
            this->iterator = nullptr;
        }

        ComputeSourcePtr getComputeSource (TupleSpec &schema, ComputePlan &plan) override {
             return std :: make_shared <VectorTupleSetIterator> (

                 [&] () -> void * {
                     if (this->iterator == nullptr) {
                         return nullptr;
                     }
                     while (this->iterator->hasNext() == true) {

                        PDBPagePtr page = this->iterator->next();
                        if(page != nullptr) {
                            return page->getBytes();
                        }
                     }
                     
                     return nullptr;

                 },

                 [] (void * freeMe) -> void {
                     free (freeMe);
                 },

                 this->batchSize

            );
        }
         
        //JiaNote: not sure whether we can include PageCircularBufferIteratorPtr in a pdb object, if it can't work, we need a way to recreate the instance with PageCircularBufferIterator at backend.
        void setIterator(PageCircularBufferIteratorPtr iterator) {
                this->iterator = iterator;
        }

        void setBatchSize(int batchSize) {
                this->batchSize = batchSize;

        }

        void setDatabaseName (std :: string dbName) {
                this->dbName = dbName;
        }

        void setSetName (std :: string setName) {
                this->setName = setName;
        }


        std :: string getDatabaseName () {
                return dbName;
        }

        std :: string getSetName () {
                return setName;
        }


	std :: string getComputationType () override {
		return std :: string ("ScanUserSet");
	}

protected:

       //JiaNote: not sure whether we can include PageCircularBufferIteratorPtr in a pdb object, if it can't work, we need a way to recreate the instance with PageCircularBufferIterator at backend.
       PageCircularBufferIteratorPtr iterator;

       String dbName;
 
       String setName;


       int batchSize;

};




}

#endif
