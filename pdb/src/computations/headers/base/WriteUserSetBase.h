//
// Created by dimitrije on 3/17/18.
//

#ifndef PDB_WRITEUSERSETBASE_H
#define PDB_WRITEUSERSETBASE_H

#include "Computation.h"
#include "VectorSink.h"
#include "PDBString.h"
#include "TypeName.h"
#include "mustache.h"

namespace pdb {

template <class OutputClass>
class WriteUserSetBase : public Computation {

public:

  /**
   * Normally these would be defined by the ENABLE_DEEP_COPY macro, but because
   * Array is the one variable-sized type that we allow, we need to manually overridethese methods
   */

  /**
   * This constructor is for constructing builtin object
   */
  WriteUserSetBase () = default;

  /**
   * User should only use following constructor
   * @param dbName
   * @param setName
   */
  WriteUserSetBase (std :: string dbName, std :: string setName) {
    this->dbName = dbName;
    this->setName = setName;
    this->outputType = getTypeName<OutputClass>();
  }

  /**
   * for deep copy
   * @param target
   * @param source
   */
  void setUpAndCopyFrom(void* target, void* source) const override {
    new (target) WriteUserSetBase<OutputClass> ();
    WriteUserSetBase<OutputClass>& fromMe = *((WriteUserSetBase<OutputClass> *) source);
    WriteUserSetBase<OutputClass>& toMe = *((WriteUserSetBase<OutputClass> *) target);

    toMe.outputType = fromMe.outputType;
    toMe.dbName = fromMe.dbName;
    toMe.setName = fromMe.setName;
  }

  /**
   * Used for deletion
   * @param deleteMe
   */
  void deleteObject(void* deleteMe) override {
    deleter(deleteMe, this);
  }

  /**
   * Used for compute size
   * @param forMe
   * @return
   */
  size_t getSize(void* forMe) override {
    return sizeof(WriteUserSetBase<OutputClass>);
  }

  /**
   * Returns a ComputeSink for this computation
   * @param consumeMe
   * @param projection
   * @param plan
   * @return
   */
  ComputeSinkPtr getComputeSink(TupleSpec& consumeMe, TupleSpec& projection, ComputePlan& plan) override {
    return std::make_shared<VectorSink<OutputClass>>(consumeMe, projection);
  }

  /**
   * Used to set the user set for writing objects to
   * @param dbName
   * @param setName
   */
  void setOutput(std::string dbName, std::string setName) override {
    this->dbName = dbName;
    this->setName = setName;
  }

  /**
   * Used to set the database name
   * @param dbName
   */
  void setDatabaseName(std::string dbName) {
    this->dbName = dbName;
  }

  /**
   * Used to get the database name
   * @return
   */
  std::string getDatabaseName() override {
    return dbName;
  }

  /**
   * Used to set the user set name
   * @param setName
   */
  void setSetName(std::string setName) {
    this->setName = setName;
  }

  /**
   * Used to get the user set name
   * @return
   */
  std::string getSetName() override {
    return setName;
  }

  /**
   * Used to return the type of the computation
   * @return
   */
  std::string getComputationType() override {
    return std::string("WriteUserSet");
  }

  /**
   * Used to return the type if of this computation
   * @return
   */
  ComputationTypeID getComputationTypeID() override {
    return WriteUserSetTypeID;
  }

  /**
   * Used to return the output type
   * @return
   */
  std::string getOutputType() override {
    if (outputType == "") {
      outputType = getTypeName<OutputClass>();
    }
    return outputType;
  }

  /**
   * Used to return the number of inputs
   * @return
   */
  int getNumInputs() override {
    return 1;
  }

  /**
   * Used to return the i-th input type
   * @param i
   * @return
   */
  std::string getIthInputType(int i) override {
    if (i == 0) {
      return this->getOutputType();
    } else {
      return "";
    }
  }

  /**
   * below function implements the interface for parsing computation into a TCAP string
   * @param inputTupleSets
   * @param computationLabel
   * @param outputTupleSetName
   * @param outputColumnNames
   * @param addedOutputColumnName
   * @return
   */
  std::string toTCAPString(std::vector<InputTupleSetSpecifier>& inputTupleSets,
                           int computationLabel,
                           std::string& outputTupleSetName,
                           std::vector<std::string>& outputColumnNames,
                           std::string& addedOutputColumnName) override {

    if (inputTupleSets.empty()) {
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

  /**
   * below function returns a TCAP string for this Computation
   * @param inputTupleSetName
   * @param inputColumnNames
   * @param inputColumnsToApply
   * @param computationLabel
   * @param outputTupleSetName
   * @param outputColumnNames
   * @param addedOutputColumnName
   * @return
   */
  std::string toTCAPString(std::string inputTupleSetName,
                           std::vector<std::string>& inputColumnNames,
                           std::vector<std::string>& inputColumnsToApply,
                           int computationLabel,
                           std::string& outputTupleSetName,
                           std::vector<std::string>& outputColumnNames,
                           std::string& addedOutputColumnName) {

    //Names for output stuff
    outputTupleSetName = inputTupleSetName + "_out";
    outputColumnNames.emplace_back("");
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

  /**
   * this computation always materializes output
   * @return
   */
  bool needsMaterializeOutput() override {
    return true;
  }

protected:

  String dbName;

  String setName;

  String outputType = "";
};

}

#endif //PDB_WRITEUSERSETBASE_H
