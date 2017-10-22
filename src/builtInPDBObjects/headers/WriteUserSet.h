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

// PRELOAD %WriteUserSet <Nothing>%

#include "Computation.h"
#include "VectorSink.h"
#include "PDBString.h"
#include "TypeName.h"

namespace pdb {

//this class encapsulates a computation that write objects of OutputClass type to a userset defined by setName and dbName.


template <class OutputClass>
class WriteUserSet : public Computation {

public:

    // normally these would be defined by the ENABLE_DEEP_COPY macro, but because
    // Array is the one variable-sized type that we allow, we need to manually override
    // these methods

    // for deep copy
    void setUpAndCopyFrom(void* target, void* source) const override {
        new (target) WriteUserSet<OutputClass> ();
        WriteUserSet<OutputClass>& fromMe = *((WriteUserSet<OutputClass> *) source);
        WriteUserSet<OutputClass>& toMe = *((WriteUserSet<OutputClass> *) target);
        toMe.outputType = fromMe.outputType;
        toMe.dbName = fromMe.dbName;
        toMe.setName = fromMe.setName;


    }

    // for deletion
    void deleteObject(void* deleteMe) override {
        deleter(deleteMe, this);
    }

    // for compute size
    size_t getSize(void* forMe) override {
        return sizeof(WriteUserSet<OutputClass>);
    }

    //this constructor is for constructing builtin object
    WriteUserSet () {}


    //user should only use following constructor
    WriteUserSet (std :: string dbName, std :: string setName) {
        this->dbName = dbName;
        this->setName = setName;
        this->outputType = getTypeName<OutputClass>();
    }


    // returns a ComputeSink for this computation
    ComputeSinkPtr getComputeSink(TupleSpec& consumeMe,
                                  TupleSpec& projection,
                                  ComputePlan& plan) override {

        return std::make_shared<VectorSink<OutputClass>>(consumeMe, projection);
    }

    // to set the user set for writing objects to
    void setOutput(std::string dbName, std::string setName) override {
        this->dbName = dbName;
        this->setName = setName;
    }

    // to set the database name
    void setDatabaseName(std::string dbName) {
        this->dbName = dbName;
    }

    // to get the database name
    std::string getDatabaseName() override {
        return dbName;
    }

    // to set the user set name
    void setSetName(std::string setName) {
        this->setName = setName;
    }

    // to get the user set name
    std::string getSetName() override {
        return setName;
    }

    // to return the type of the computation
    std::string getComputationType() override {
        return std::string("WriteUserSet");
    }

    // to return the output type
    std::string getOutputType() override {
        return outputType;
    }

    // to return the number of inputs
    int getNumInputs() override {
        return 1;
    }

    // to return the i-th input type
    std::string getIthInputType(int i) override {
        if (i == 0) {
            return outputType;
        } else {
            return "";
        }
    }


    // below function implements the interface for parsing computation into a TCAP string
    std::string toTCAPString(std::vector<InputTupleSetSpecifier>& inputTupleSets,
                             int computationLabel,
                             std::string& outputTupleSetName,
                             std::vector<std::string>& outputColumnNames,
                             std::string& addedOutputColumnName) override {

        if (inputTupleSets.size() == 0) {
            return "";
        }
        InputTupleSetSpecifier inputTupleSet = inputTupleSets[0];
        return toTCAPString(inputTupleSet.getTupleSetName(),
                            inputTupleSet.getColumnNamesToKeep(),
                            inputTupleSet.getColumnNamesToApply(),
                            computationLabel,
                            outputTupleSetName,
                            outputColumnNames,
                            addedOutputColumnName);
    }

    // below function returns a TCAP string for this Computation
    std::string toTCAPString(std::string inputTupleSetName,
                             std::vector<std::string>& inputColumnNames,
                             std::vector<std::string>& inputColumnsToApply,
                             int computationLabel,
                             std::string& outputTupleSetName,
                             std::vector<std::string>& outputColumnNames,
                             std::string& addedOutputColumnName) {
        // std :: cout << "\n/*Write to output set*/\n";
        std::string ret = std::string("out() <= OUTPUT (") + inputTupleSetName + " (" +
            inputColumnsToApply[0] + ")" + std::string(", '") + std::string(setName) +
            std::string("', '") + std::string(dbName) + std::string("', '") + getComputationType() +
            std::string("_") + std::to_string(computationLabel) + std::string("')");
        ret = ret + "\n";
        outputTupleSetName = "out";
        outputColumnNames.push_back("");
        addedOutputColumnName = "";
        this->setTraversed(true);
        this->setOutputTupleSetName(outputTupleSetName);
        this->setOutputColumnToApply(addedOutputColumnName);
        return ret;
    }

    //this computation always materializes output
    bool needsMaterializeOutput() override {
        return true;
    }

protected:
    String dbName;
    String setName;
    String outputType;
};
}

#endif
