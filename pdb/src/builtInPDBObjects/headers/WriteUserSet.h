
#ifndef WRITE_USER_SET_H
#define WRITE_USER_SET_H

// PRELOAD %WriteUserSet <Nothing>%

#include "Computation.h"
#include "VectorSink.h"
#include "PDBString.h"
#include "TypeName.h"
#include "mustache.h"


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

    // to return the type if of this computation
    ComputationTypeID getComputationTypeID() override {
        return WriteUserSetTypeID;
    }

    // to return the output type
    std::string getOutputType() override {
        if (outputType == "") {
           outputType = getTypeName<OutputClass>();
        }
        return outputType;
    }

    // to return the number of inputs
    int getNumInputs() override {
        return 1;
    }

    // to return the i-th input type
    std::string getIthInputType(int i) override {
        if (i == 0) {
            return this->getOutputType();
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

        //Names for output stuff
        outputTupleSetName = inputTupleSetName + "_out";
        outputColumnNames.push_back("");
        addedOutputColumnName = "";

        // the template we are going to use to create the TCAP string for this ScanUserSet
        mustache::mustache writeSetTemplate{"{{outputTupleSetName}}( {{outputColumnNames}}) <= "
                                            "OUTPUT ( {{inputTupleSetName}} ( {{inputColumnsToApply}} ), "
                                            "'{{setName}}', '{{dbName}}', '{{computationType}}_{{computationLabel}}')\n"};


        // the data required to fill in the template
        mustache::data writeSetData;
        writeSetData.set("outputTupleSetName", outputTupleSetName);
        writeSetData.set("outputColumnNames", outputColumnNames[0]);
        writeSetData.set("inputTupleSetName", inputTupleSetName);
        writeSetData.set("inputColumnsToApply", inputColumnsToApply[0]); //TODO? Only consider first column
        writeSetData.set("computationType", getComputationType());
        writeSetData.set("computationLabel", std::to_string(computationLabel));
        writeSetData.set("setName", std::string(setName));
        writeSetData.set("dbName", std::string(dbName));


        // update the state of the computation
        this->setTraversed(true);
        this->setOutputTupleSetName(outputTupleSetName);
        this->setOutputColumnToApply(addedOutputColumnName);

        // return the TCAP string
        return writeSetTemplate.render(writeSetData);
    }

    //this computation always materializes output
    bool needsMaterializeOutput() override {
        return true;
    }

protected:
    String dbName;
    String setName;
    String outputType="";
};
}

#endif
