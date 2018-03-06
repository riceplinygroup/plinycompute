
#ifndef SET_WRITER_H
#define SET_WRITER_H

#include "Computation.h"
#include "TypeName.h"
#include "mustache.hpp"

namespace pdb {

template <class OutputClass>
class SetWriter : public Computation {

    std::string getComputationType() override {
        return std::string("SetWriter");
    }

    // to return the type if of this computation
    ComputationTypeID getComputationTypeID() override {
        return SetWriterTypeID;
    }

    // gets the name of the i^th input type...
    std::string getIthInputType(int i) override {
        if (i == 0) {
            return getTypeName<OutputClass>();
        } else {
            return "";
        }
    }

    // get the number of inputs to this query type
    int getNumInputs() override {
        return 1;
    }

    // gets the output type of this query as a string
    std::string getOutputType() override {
        return getTypeName<OutputClass>();
    }

    bool needsMaterializeOutput() override {
        return true;
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

        //Names for output stuff
        outputTupleSetName = inputTupleSetName + "_out";
        outputColumnNames.push_back("");
        addedOutputColumnName = "";

        // the template we are going to use to create the TCAP string for this ScanUserSet
        mustache::mustache writeSetTemplate{"{{outputTupleSetName}}( {{outputColumnNames}}) <= "
                                            "OUTPUT ( {{inputTupleSetName}} ( {{inputColumnsToApply}} ), "
                                            "'EmptySet()', 'EmptySet()', '{{computationType}}_{{computationLabel}}')\n"};


        // the data required to fill in the template
        mustache::data writeSetData;
        writeSetData.set("outputTupleSetName", outputTupleSetName);
        writeSetData.set("outputColumnNames", outputColumnNames[0]);
        writeSetData.set("inputTupleSetName", inputTupleSetName);
        writeSetData.set("inputColumnsToApply", inputColumnsToApply[0]);
        writeSetData.set("computationType", getComputationType());
        writeSetData.set("computationLabel", std::to_string(computationLabel));



        // update the state of the computation
        this->setTraversed(true);
        this->setOutputTupleSetName(outputTupleSetName);
        this->setOutputColumnToApply(addedOutputColumnName);


        // return the TCAP string
        return writeSetTemplate.render(writeSetData);
    }
};
}

#endif
