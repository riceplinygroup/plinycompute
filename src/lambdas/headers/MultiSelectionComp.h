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

#ifndef MULTISELECTION_COMP_H
#define MULTISELECTION_COMP_H


#include "Computation.h"
#include "ComputePlan.h"
#include "VectorSink.h"
#include "ScanUserSet.h"
#include "TypeName.h"

namespace pdb {


template <class OutputClass, class InputClass>
class MultiSelectionComp : public Computation {

public:
    // the computation returned by this method is called to see if a data item should be returned in
    // the output set
    virtual Lambda<bool> getSelection(Handle<InputClass> checkMe) = 0;

    // the computation returned by this method is called to produce output tuples from this method
    virtual Lambda<Vector<Handle<OutputClass>>> getProjection(Handle<InputClass> checkMe) = 0;

    // calls getProjection and getSelection to extract the lambdas
    void extractLambdas(std::map<std::string, GenericLambdaObjectPtr>& returnVal) override {
        int suffix = 0;
        Handle<InputClass> checkMe = nullptr;
        Lambda<bool> selectionLambda = getSelection(checkMe);
        Lambda<Vector<Handle<OutputClass>>> projectionLambda = getProjection(checkMe);
        selectionLambda.toMap(returnVal, suffix);
        projectionLambda.toMap(returnVal, suffix);
    }

    // this is a MultiSelection computation
    std::string getComputationType() override {
        return std::string("MultiSelectionComp");
    }

    // gets the name of the i^th input type...
    std::string getIthInputType(int i) override {
        if (i == 0) {
            return getTypeName<InputClass>();
        } else {
            return "";
        }
    }

    // get the number of inputs to this query type
    int getNumInputs() override {
        return 1;
    }

    // return the output type
    std::string getOutputType() override {
        return getTypeName<OutputClass>();
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
        std::vector<std::string> childrenLambdaNames;
        std::string myLambdaName;
        return toTCAPString(inputTupleSet.getTupleSetName(),
                            inputTupleSet.getColumnNamesToKeep(),
                            inputTupleSet.getColumnNamesToApply(),
                            childrenLambdaNames,
                            computationLabel,
                            outputTupleSetName,
                            outputColumnNames,
                            addedOutputColumnName,
                            myLambdaName);
    }

    // to return Selection tcap string
    std::string toTCAPString(std::string inputTupleSetName,
                             std::vector<std::string>& inputColumnNames,
                             std::vector<std::string>& inputColumnsToApply,
                             std::vector<std::string>& childrenLambdaNames,
                             int computationLabel,
                             std::string& outputTupleSetName,
                             std::vector<std::string>& outputColumnNames,
                             std::string& addedOutputColumnName,
                             std::string& myLambdaName) {
        PDB_COUT << "To GET TCAP STRING FOR SELECTION" << std::endl;
        std::string tcapString = "";
        Handle<InputClass> checkMe = nullptr;
        PDB_COUT << "TO GET TCAP STRING FOR SELECTION LAMBDA" << std::endl;
        Lambda<bool> selectionLambda = getSelection(checkMe);
        std::string tupleSetName;
        std::vector<std::string> columnNames;
        std::string addedColumnName;
        int lambdaLabel = 0;
        tcapString += "\n/* Apply MultiSelection filtering */\n";
        tcapString += selectionLambda.toTCAPString(inputTupleSetName,
                                                   inputColumnNames,
                                                   inputColumnsToApply,
                                                   childrenLambdaNames,
                                                   lambdaLabel,
                                                   getComputationType(),
                                                   computationLabel,
                                                   tupleSetName,
                                                   columnNames,
                                                   addedColumnName,
                                                   myLambdaName,
                                                   false);
        PDB_COUT << "tcapString after parsing selection lambda: " << tcapString << std::endl;
        PDB_COUT << "lambdaLabel=" << lambdaLabel << std::endl;
        std::string newTupleSetName =
            "filteredInputFor" + getComputationType() + std::to_string(computationLabel);
        tcapString += newTupleSetName + "(" + inputColumnNames[0];
        for (int i = 1; i < inputColumnNames.size(); i++) {
            tcapString += ", " + inputColumnNames[i];
        }
        tcapString += ") <= FILTER (" + tupleSetName + "(" + addedColumnName + "), " +
            tupleSetName + "(" + inputColumnNames[0];
        for (int i = 1; i < inputColumnNames.size(); i++) {
            tcapString += ", ";
            tcapString += inputColumnNames[i];
        }
        tcapString += ")";
        tcapString +=
            ", '" + getComputationType() + "_" + std::to_string(computationLabel) + "')\n";
        PDB_COUT << "tcapString after adding filter operation: " << tcapString << std::endl;
        Lambda<Vector<Handle<OutputClass>>> projectionLambda = getProjection(checkMe);
        PDB_COUT << "TO GET TCAP STRING FOR PROJECTION LAMBDA" << std::endl;
        PDB_COUT << "lambdaLabel=" << lambdaLabel << std::endl;
        tcapString += "\n/* Apply MultiSelection projection */\n";
        tcapString += projectionLambda.toTCAPString(newTupleSetName,
                                                    inputColumnNames,
                                                    inputColumnsToApply,
                                                    childrenLambdaNames,
                                                    lambdaLabel,
                                                    getComputationType(),
                                                    computationLabel,
                                                    outputTupleSetName,
                                                    outputColumnNames,
                                                    addedOutputColumnName,
                                                    myLambdaName,
                                                    true);
        // TODO: add Flatten
        newTupleSetName =
            "flattenedOutFor" + getComputationType() + std::to_string(computationLabel);
        std::string newOutputColumnName = "flattened_" + addedOutputColumnName;
        tcapString += newTupleSetName + "(" + newOutputColumnName + ") <= FLATTEN (" +
            outputTupleSetName + "(" + addedOutputColumnName + "), " + outputTupleSetName +
            "(), '" + getComputationType() + "_" + std::to_string(computationLabel) + "')\n";


        this->setTraversed(true);
        this->setOutputTupleSetName(newTupleSetName);
        outputTupleSetName = newTupleSetName;
        this->setOutputColumnToApply(newOutputColumnName);
        addedOutputColumnName = newOutputColumnName;
        outputColumnNames.clear();
        outputColumnNames.push_back(addedOutputColumnName);
        return tcapString;
    }

    void setOutput(std::string dbName, std::string setName) override {
        this->materializeSelectionOut = true;
        this->outputSetScanner = makeObject<ScanUserSet<OutputClass>>();
        this->outputSetScanner->setDatabaseName(dbName);
        this->outputSetScanner->setSetName(setName);
    }

    void setBatchSize(int batchSize) override {
        if (this->outputSetScanner != nullptr) {
            this->outputSetScanner->setBatchSize(batchSize);
        }
    }

    // to return the database name
    std::string getDatabaseName() override {
        return this->outputSetScanner->getDatabaseName();
    }

    // to return the set name
    std::string getSetName() override {
        return this->outputSetScanner->getSetName();
    }

    // source for consumer to read selection output, which has been written to a user set
    ComputeSourcePtr getComputeSource(TupleSpec& outputScheme, ComputePlan& plan) override {

        if (this->materializeSelectionOut == true) {
            if (this->outputSetScanner != nullptr) {
                return outputSetScanner->getComputeSource(outputScheme, plan);
            }
        }
        return nullptr;
    }

    // sink to write selection output
    ComputeSinkPtr getComputeSink(TupleSpec& consumeMe,
                                  TupleSpec& projection,
                                  ComputePlan& plan) override {

        if (this->materializeSelectionOut == true) {
            return std::make_shared<VectorSink<OutputClass>>(consumeMe, projection);
        }
        return nullptr;
    }

    bool needsMaterializeOutput() override {
        return materializeSelectionOut;
    }

    Handle<ScanUserSet<OutputClass>>& getOutputSetScanner() {
        return outputSetScanner;
    }


private:
    bool materializeSelectionOut = false;
    Handle<ScanUserSet<OutputClass>> outputSetScanner = nullptr;
};
}

#endif
