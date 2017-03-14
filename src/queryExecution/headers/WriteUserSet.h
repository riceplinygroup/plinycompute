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

#ifndef WRITE_USER_SET_H
#define WRITE_USER_SET_H

//by Jia, Mar 2017

#include "Computation.h"
#include "VectorSink.h"
#include "PDBString.h"
#include "TypeName.h"

namespace pdb {

template <class OutputClass>
class WriteUserSet : public Computation {

public:

        ENABLE_DEEP_COPY

        void initialize() {
            outputType = getTypeName <OutputClass>();
        }

        ComputeSinkPtr getComputeSink (TupleSpec &consumeMe, TupleSpec &projection, ComputePlan &plan) override {
        
             return std :: make_shared<VectorSink <OutputClass>> (consumeMe, projection);

        }

        void setDatabaseName (std :: string dbName) {
            this->dbName = dbName;
        }

        std :: string getDatabaseName () {
            return  dbName;
        }

        void setSetName (std :: string setName) {
            this->setName = setName;
        }

        std :: string getSetName () {
            return setName;
        }

	std :: string getComputationType () override {
		return std :: string ("WriteUserSet");
	}

        std :: string getOutputType() {
                return outputType;
        }



protected:

        String dbName;
        String setName;
        String outputType;

};

}

#endif
