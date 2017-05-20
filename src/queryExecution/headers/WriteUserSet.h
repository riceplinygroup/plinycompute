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


        ComputeSinkPtr getComputeSink (TupleSpec &consumeMe, TupleSpec &projection, ComputePlan &plan) override {
        
             return std :: make_shared<VectorSink <OutputClass>> (consumeMe, projection);

        }

        void setOutput (std :: string dbName, std :: string setName) override {
              this->dbName = dbName;
              this->setName = setName;
        }

        void setDatabaseName (std :: string dbName) {
            this->dbName = dbName;
        }

        std :: string getDatabaseName () override {
            return  dbName;
        }

        void setSetName (std :: string setName) {
            this->setName = setName;
        }

        std :: string getSetName () override {
            return setName;
        }

	std :: string getComputationType () override {
            return std :: string ("WriteUserSet");
	}

        std :: string getOutputType() override {
            return getTypeName <OutputClass>();
        }

        int getNumInputs() override {
           return 1;   
        }

        std :: string getIthInputType (int i) override {
           if (i==0) {
               return getTypeName <OutputClass>();
           } else {
               return "";
           }
        }


        // below function implements the interface for parsing computation into a TCAP string
        std :: string toTCAPString (std :: vector <InputTupleSetSpecifier> inputTupleSets, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string>& outputColumnNames, std :: string& addedOutputColumnName) override {

    if (inputTupleSets.size() == 0) {
        return "";
    }
    InputTupleSetSpecifier inputTupleSet = inputTupleSets[0];
    return toTCAPString (inputTupleSet.getTupleSetName(), inputTupleSet.getColumnNamesToKeep(), inputTupleSet.getColumnNamesToApply(), computationLabel, outputTupleSetName, outputColumnNames, addedOutputColumnName);
 }

        // below function returns a TCAP string for this Computation
       std :: string toTCAPString (std :: string inputTupleSetName, std :: vector<std :: string> inputColumnNames, std :: vector<std :: string> inputColumnsToApply, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string>& outputColumnNames, std :: string& addedOutputColumnName) {
              std :: cout << "\n/*Write to output set*/\n";
              std :: string ret = std :: string("out() <= OUTPUT (") + inputTupleSetName + " (" + inputColumnsToApply[0] + ")" + std :: string(", '") + std :: string(setName) + std :: string("', '") +  std :: string(dbName) + std :: string ("', '") + getComputationType() + std :: string("_") + std :: to_string(computationLabel)  + std :: string("')");
              ret = ret + "\n";
              outputTupleSetName = "out";
              outputColumnNames.push_back("");
              addedOutputColumnName = "";
              this->setTraversed (true);
              this->setOutputTupleSetName (outputTupleSetName);
              this->setOutputColumnToApply (addedOutputColumnName);
              return ret;
        }

        bool needsMaterializeOutput () override {
            return true;
        }

protected:

        String dbName;
        String setName;
        String outputType;

};

}

#endif
