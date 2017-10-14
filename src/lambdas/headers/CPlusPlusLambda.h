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

#ifndef C_PLUS_PLUS_LAM_CC
#define C_PLUS_PLUS_LAM_CC

#include <memory>
#include <iostream>
#include <vector>

#define CAST(TYPENAME, WHICH) ((*(((std::vector<Handle<TYPENAME>>**)args)[WHICH]))[which])

namespace pdb {

template <typename F,
          typename ReturnType,
          typename ParamOne,
          typename ParamTwo,
          typename ParamThree,
          typename ParamFour,
          typename ParamFive>
typename std::enable_if<
    !std::is_base_of<Nothing, ParamOne>::value && std::is_base_of<Nothing, ParamTwo>::value &&
        std::is_base_of<Nothing, ParamThree>::value && std::is_base_of<Nothing, ParamFour>::value &&
        std::is_base_of<Nothing, ParamFive>::value,
    void>::type
callLambda(F& func, std::vector<ReturnType>& assignToMe, int which, void** args) {
    assignToMe[which] = func(CAST(ParamOne, 0));
}

template <typename F,
          typename ReturnType,
          typename ParamOne,
          typename ParamTwo,
          typename ParamThree,
          typename ParamFour,
          typename ParamFive>
typename std::enable_if<
    !std::is_base_of<Nothing, ParamOne>::value && !std::is_base_of<Nothing, ParamTwo>::value &&
        std::is_base_of<Nothing, ParamThree>::value && std::is_base_of<Nothing, ParamFour>::value &&
        std::is_base_of<Nothing, ParamFive>::value,
    void>::type
callLambda(F& func, std::vector<ReturnType>& assignToMe, int which, void** args) {
    assignToMe[which] = func(CAST(ParamOne, 0), CAST(ParamTwo, 1));
}

template <typename F,
          typename ReturnType,
          typename ParamOne,
          typename ParamTwo,
          typename ParamThree,
          typename ParamFour,
          typename ParamFive>
typename std::enable_if<
    !std::is_base_of<Nothing, ParamOne>::value && !std::is_base_of<Nothing, ParamTwo>::value &&
        !std::is_base_of<Nothing, ParamThree>::value &&
        std::is_base_of<Nothing, ParamFour>::value && std::is_base_of<Nothing, ParamFive>::value,
    void>::type
callLambda(F& func, std::vector<ReturnType>& assignToMe, int which, void** args) {
    assignToMe[which] = func(CAST(ParamOne, 0), CAST(ParamTwo, 1), CAST(ParamThree, 2));
}

template <typename F,
          typename ReturnType,
          typename ParamOne,
          typename ParamTwo,
          typename ParamThree,
          typename ParamFour,
          typename ParamFive>
typename std::enable_if<
    !std::is_base_of<Nothing, ParamOne>::value && !std::is_base_of<Nothing, ParamTwo>::value &&
        !std::is_base_of<Nothing, ParamThree>::value &&
        !std::is_base_of<Nothing, ParamFour>::value && std::is_base_of<Nothing, ParamFive>::value,
    void>::type
callLambda(F& func, std::vector<ReturnType>& assignToMe, int which, void** args) {
    assignToMe[which] =
        func(CAST(ParamOne, 0), CAST(ParamTwo, 1), CAST(ParamThree, 2), CAST(ParamFour, 3));
}

template <typename F,
          typename ReturnType,
          typename ParamOne,
          typename ParamTwo,
          typename ParamThree,
          typename ParamFour,
          typename ParamFive>
typename std::enable_if<
    !std::is_base_of<Nothing, ParamOne>::value && !std::is_base_of<Nothing, ParamTwo>::value &&
        !std::is_base_of<Nothing, ParamThree>::value &&
        !std::is_base_of<Nothing, ParamFour>::value && !std::is_base_of<Nothing, ParamFive>::value,
    void>::type
callLambda(F& func, std::vector<ReturnType>& assignToMe, int which, void** args) {
    assignToMe[which] = func(CAST(ParamOne, 0),
                             CAST(ParamTwo, 1),
                             CAST(ParamThree, 2),
                             CAST(ParamFour, 3),
                             CAST(ParamFive, 4));
}

template <typename F,
          typename ReturnType,
          typename ParamOne = Nothing,
          typename ParamTwo = Nothing,
          typename ParamThree = Nothing,
          typename ParamFour = Nothing,
          typename ParamFive = Nothing>
class CPlusPlusLambda : public TypedLambdaObject<ReturnType> {

private:
    F myFunc;
    int numInputs = 0;

public:
    // JiaNote: I changed CPlusPlusLambda constructor interface to obtain input information for
    // query graph analysis.
    CPlusPlusLambda(F arg,
                    Handle<ParamOne>& input1,
                    Handle<ParamTwo>& input2,
                    Handle<ParamThree>& input3,
                    Handle<ParamFour>& input4,
                    Handle<ParamFive>& input5)
        : myFunc(arg) {

        if (getTypeName<ParamOne>() != "pdb::Nothing") {
            this->numInputs++;
            this->setInputIndex(0, -((input1.getExactTypeInfoValue() + 1)));
        }
        if (getTypeName<ParamTwo>() != "pdb::Nothing") {
            this->numInputs++;
            this->setInputIndex(1, -((input2.getExactTypeInfoValue() + 1)));
        }
        if (getTypeName<ParamThree>() != "pdb::Nothing") {
            this->numInputs++;
            this->setInputIndex(2, -((input3.getExactTypeInfoValue() + 1)));
        }
        if (getTypeName<ParamFour>() != "pdb::Nothing") {
            this->numInputs++;
            this->setInputIndex(3, -((input4.getExactTypeInfoValue() + 1)));
        }
        if (getTypeName<ParamFive>() != "pdb::Nothing") {
            this->numInputs++;
            this->setInputIndex(4, -((input5.getExactTypeInfoValue() + 1)));
        }
    }


    unsigned int getNumInputs() override {
        return this->numInputs;
    }

    std::string getTypeOfLambda() override {
        return std::string("native_lambda");
    }

    GenericLambdaObjectPtr getChild(int which) override {
        return nullptr;
    }

    int getNumChildren() override {
        return 0;
    }

    ~CPlusPlusLambda() {}


    ComputeExecutorPtr getExecutor(TupleSpec& inputSchema,
                                   TupleSpec& attsToOperateOn,
                                   TupleSpec& attsToIncludeInOutput) override {

        // create the output tuple set
        TupleSetPtr output = std::make_shared<TupleSet>();

        // create the machine that is going to setup the output tuple set, using the input tuple set
        TupleSetSetupMachinePtr myMachine =
            std::make_shared<TupleSetSetupMachine>(inputSchema, attsToIncludeInOutput);

        // this is the list of input attributes that we need to match on
        std::vector<int> matches = myMachine->match(attsToOperateOn);

        // fix this!!  Use a smart pointer
        std::shared_ptr<std::vector<void*>> inputAtts = std::make_shared<std::vector<void*>>();
        for (int i = 0; i < matches.size(); i++) {
            inputAtts->push_back(nullptr);
        }

        // this is the output attribute
        int outAtt = attsToIncludeInOutput.getAtts().size();

        return std::make_shared<SimpleComputeExecutor>(
            output,
            [=](TupleSetPtr input) {

                // set up the output tuple set
                myMachine->setup(input, output);

                // get the columns to operate on
                int numAtts = matches.size();
                void** inAtts = inputAtts->data();
                for (int i = 0; i < numAtts; i++) {
                    inAtts[i] = &(input->getColumn<int>(matches[i]));
                }

                // setup the output column, if it is not already set up
                if (!output->hasColumn(outAtt)) {
                    std::vector<ReturnType>* outputCol = new std::vector<ReturnType>;
                    output->addColumn(outAtt, outputCol, true);
                }

                // get the output column
                std::vector<ReturnType>& outColumn = output->getColumn<ReturnType>(outAtt);

                // loop down the columns, setting the output
                int numTuples = ((std::vector<Handle<ParamOne>>*)inAtts[0])->size();
                outColumn.resize(numTuples);
                for (int i = 0; i < numTuples; i++) {
                    callLambda<F, ReturnType, ParamOne, ParamTwo, ParamThree, ParamFour, ParamFive>(
                        myFunc, outColumn, i, inAtts);
                }

                return output;
            },
            "nativeLambda");
    }

    // JiaNote: comment below, because now HashOne is a separate TCAP executor
    /*
            //Jia Note: add 1 to each tuple set for cartesian join
            ComputeExecutorPtr getOneHasher (TupleSpec &inputSchema, TupleSpec &attsToOperateOn,
       TupleSpec &attsToIncludeInOutput) override {

                    // create the output tuple set
                    TupleSetPtr output = std :: make_shared <TupleSet> ();

                    // create the machine that is going to setup the output tuple set, using the
       input tuple set
                    TupleSetSetupMachinePtr myMachine = std :: make_shared <TupleSetSetupMachine>
       (inputSchema, attsToIncludeInOutput);

                    // these are the input attributes that we will process
                    std :: vector <int> inputAtts = myMachine->match (attsToOperateOn);
                    int firstAtt = inputAtts[0];


                    // this is the output attribute
                    int outAtt = attsToIncludeInOutput.getAtts ().size ();

                    return std :: make_shared <SimpleComputeExecutor> (
                            output,
                            [=] (TupleSetPtr input) {

                                    // set up the output tuple set
                                    myMachine->setup (input, output);

                                    // get the columns to get size of tuples
                                    //std :: vector <Handle<ParamOne>> & firstColumn =
       input->getColumn<Handle<ParamOne>> (firstAtt);

                                    // create the output attribute, if needed
                                    if (!output->hasColumn (outAtt)) {
                                            std :: vector <size_t> *outColumn = new std :: vector
       <size_t>;
                                            output->addColumn (outAtt, outColumn, true);
                                    }

                                    // get the output column
                                    std :: vector <size_t> &outColumn = output->getColumn <size_t>
       (outAtt);

                                    // loop down the columns, setting the output
                                    //int numTuples = firstColumn.size();
                                    int numTuples = input->getNumRows(firstAtt);
                                    outColumn.resize (numTuples);
                                    for (int i = 0; i < numTuples; i++) {
                                            outColumn [i] = 1;
                                    }
                                    return output;
                            }
                    );
           }
    */

    // JiaNote: we need this to generate TCAP for a cartesian join
    std::string toTCAPStringForCartesianJoin(int lambdaLabel,
                                             std::string computationName,
                                             int computationLabel,
                                             std::string& outputTupleSetName,
                                             std::vector<std::string>& outputColumns,
                                             std::string& outputColumnName,
                                             std::string& myLambdaName,
                                             MultiInputsBase* multiInputsComp) override {

        std::string tcapString = "";
        myLambdaName = "native_lambda_" + std::to_string(lambdaLabel);
        std::string myComputationName = computationName + "_" + std::to_string(computationLabel);
        if (multiInputsComp == nullptr) {
            return tcapString;
        }


        // Step 1. for each input get its current tupleset name;
        unsigned int numInputs = this->getNumInputs();
        std::vector<std::string> inputTupleSetNames;
        std::map<std::string, std::vector<unsigned int>> inputPartitions;
        std::vector<std::vector<std::string>> inputColumnNames;
        std::vector<std::vector<std::string>> inputColumnsToApply;
        for (unsigned int i = 0; i < numInputs; i++) {
            unsigned int index = this->getInputIndex(i);
            std::string curTupleSetName = multiInputsComp->getTupleSetNameForIthInput(index);
            auto iter =
                std::find(inputTupleSetNames.begin(), inputTupleSetNames.end(), curTupleSetName);
            if (iter == inputTupleSetNames.end()) {
                inputTupleSetNames.push_back(curTupleSetName);
                inputColumnNames.push_back(multiInputsComp->getInputColumnsForIthInput(index));
                inputColumnsToApply.push_back(
                    multiInputsComp->getInputColumnsToApplyForIthInput(index));
            }
            inputPartitions[curTupleSetName].push_back(index);
        }

        for (unsigned int i = 0; i < inputTupleSetNames.size(); i++) {
            std::string curTupleSetName = inputTupleSetNames[i];
            std::vector<unsigned int> curVec = inputPartitions[curTupleSetName];
        }


        std::string curLeftTupleSetName;
        std::vector<std::string> curLeftColumnsToKeep;
        std::string curLeftHashColumnName;
        std::vector<unsigned int> curLeftIndexes;
        // Step 2. if there are more than one input tuplesets, we need to do cartesian join for all
        // of them
        if (inputTupleSetNames.size() > 1) {
            for (unsigned int i = 0; i < inputTupleSetNames.size() - 1; i++) {
                if (i == 0) {
                    // HashOne for the 0-th tupleset
                    std::string curLeftInputTupleSetName = inputTupleSetNames[0];
                    curLeftIndexes = inputPartitions[curLeftInputTupleSetName];
                    curLeftColumnsToKeep = inputColumnNames[0];
                    std::vector<std::string> curInputColumnsToApply = inputColumnsToApply[0];
                    std::string curPrefix = "hashOneFor_" + std::to_string(computationLabel) + "_" +
                        std::to_string(lambdaLabel);
                    if (curLeftIndexes.size() > 1) {
                        curPrefix += "Joined";
                    }
                    curLeftTupleSetName =
                        curPrefix + "_" + multiInputsComp->getNameForIthInput(curLeftIndexes[0]);
                    for (unsigned int j = 1; j < curLeftIndexes.size(); j++) {
                        curLeftTupleSetName +=
                            "_" + multiInputsComp->getNameForIthInput(curLeftIndexes[j]);
                    }
                    curLeftHashColumnName = "OneFor_0_" + std::to_string(computationLabel) + "_" +
                        std::to_string(lambdaLabel);
                    std::vector<std::string> curOutputColumnNames;
                    for (unsigned int j = 0; j < curLeftColumnsToKeep.size(); j++) {
                        curOutputColumnNames.push_back(curLeftColumnsToKeep[j]);
                    }
                    curOutputColumnNames.push_back(curLeftHashColumnName);
                    tcapString += this->getTCAPString(curLeftInputTupleSetName,
                                                      curLeftColumnsToKeep,
                                                      curInputColumnsToApply,
                                                      curLeftTupleSetName,
                                                      curOutputColumnNames,
                                                      curLeftHashColumnName,
                                                      "HASHONE",
                                                      myComputationName,
                                                      "");
                }

                // HashOne for the (i+1)-th table
                std::string curInputTupleSetName = inputTupleSetNames[i + 1];
                std::vector<std::string> curInputColumnNames = inputColumnNames[i + 1];
                std::vector<std::string> curInputColumnsToApply = inputColumnsToApply[i + 1];
                std::vector<unsigned int> curIndexes = inputPartitions[curInputTupleSetName];
                std::string curPrefix = "hashOneFor_" + std::to_string(computationLabel) + "_" +
                    std::to_string(lambdaLabel);
                if (curIndexes.size() > 1) {
                    curPrefix += "Joined";
                }
                std::string curOutputTupleSetName =
                    curPrefix + "_" + multiInputsComp->getNameForIthInput(curIndexes[0]);
                for (unsigned int j = 1; j < curIndexes.size(); j++) {
                    curOutputTupleSetName +=
                        "_" + multiInputsComp->getNameForIthInput(curIndexes[j]);
                }
                std::string curOutputColumnName = "OneFor_" + std::to_string(i + 1) + "_" +
                    std::to_string(computationLabel) + "_" + std::to_string(lambdaLabel);
                std::vector<std::string> curOutputColumnNames;
                for (unsigned int j = 0; j < curInputColumnNames.size(); j++) {
                    curOutputColumnNames.push_back(curInputColumnNames[j]);
                }
                curOutputColumnNames.push_back(curOutputColumnName);
                tcapString += this->getTCAPString(curInputTupleSetName,
                                                  curInputColumnNames,
                                                  curInputColumnsToApply,
                                                  curOutputTupleSetName,
                                                  curOutputColumnNames,
                                                  curOutputColumnName,
                                                  "HASHONE",
                                                  myComputationName,
                                                  "");


                // Join the two tables
                tcapString += "\n/* CartesianJoin ( " +
                    multiInputsComp->getNameForIthInput(curLeftIndexes[0]);
                std::string outputTupleSetName =
                    "CartesianJoined__" + multiInputsComp->getNameForIthInput(curLeftIndexes[0]);
                for (unsigned int j = 1; j < curLeftIndexes.size(); j++) {
                    outputTupleSetName +=
                        "_" + multiInputsComp->getNameForIthInput(curLeftIndexes[j]);
                    tcapString += " " + multiInputsComp->getNameForIthInput(curLeftIndexes[j]);
                }
                outputTupleSetName += "___" + multiInputsComp->getNameForIthInput(curIndexes[0]);
                tcapString += " ) and ( " + multiInputsComp->getNameForIthInput(curIndexes[0]);
                for (unsigned int j = 1; j < curIndexes.size(); j++) {
                    outputTupleSetName += "_" + multiInputsComp->getNameForIthInput(curIndexes[j]);
                    tcapString += " " + multiInputsComp->getNameForIthInput(curIndexes[j]);
                }
                outputTupleSetName += "_";
                tcapString += " ) */\n";
                // push down projection here
                tcapString += outputTupleSetName + "(" + curLeftColumnsToKeep[0];
                for (unsigned int j = 1; j < curLeftColumnsToKeep.size(); j++) {
                    tcapString += ", " + curLeftColumnsToKeep[j];
                }
                for (unsigned int j = 0; j < curInputColumnNames.size(); j++) {
                    tcapString += ", " + curInputColumnNames[j];
                }
                if (i + 1 < inputTupleSetNames.size() - 1) {
                    tcapString += ", " + curOutputColumnName;
                }
                tcapString +=
                    ") <= JOIN (" + curLeftTupleSetName + "(" + curLeftHashColumnName + "), ";
                tcapString += curLeftTupleSetName + "(" + curLeftColumnsToKeep[0];
                for (unsigned int j = 1; j < curLeftColumnsToKeep.size(); j++) {
                    tcapString += ", " + curLeftColumnsToKeep[j];
                }
                tcapString += "), ";
                tcapString += curOutputTupleSetName + "(" + curOutputColumnName + "), ";
                tcapString += curOutputTupleSetName + "(" + curInputColumnNames[0];
                for (unsigned int j = 1; j < curInputColumnNames.size(); j++) {
                    tcapString += ", " + curInputColumnNames[j];
                }
                if (i + 1 < inputTupleSetNames.size() - 1) {
                    tcapString += ", " + curOutputColumnName;
                }
                tcapString += "), '" + myComputationName + "')\n";


                // update counters
                curLeftTupleSetName = outputTupleSetName;
                for (unsigned int j = 0; j < curInputColumnNames.size(); j++) {
                    curLeftColumnsToKeep.push_back(curInputColumnNames[j]);
                }
                for (unsigned int j = 0; j < curIndexes.size(); j++) {
                    curLeftIndexes.push_back(curIndexes[j]);
                }
                curLeftHashColumnName = curOutputColumnName;
            }
        } else {

            curLeftTupleSetName = inputTupleSetNames[0];
            curLeftColumnsToKeep = inputColumnNames[0];
        }

        // Step 3. do an apply to add a boolean column
        std::vector<std::string> curInputColumnsToApply;
        for (int i = 0; i < numInputs; i++) {
            unsigned int index = this->getInputIndex(i);
            curInputColumnsToApply.push_back(multiInputsComp->getNameForIthInput(index));
        }
        std::string curOutputTupleSetName = "nativOutFor_" + myLambdaName + "_" + myComputationName;
        std::string curOutputColumnName =
            "nativOut_" + std::to_string(lambdaLabel) + "_" + std::to_string(computationLabel);
        std::vector<std::string> curOutputColumnNames;
        for (unsigned int i = 0; i < curLeftColumnsToKeep.size(); i++) {
            curOutputColumnNames.push_back(curLeftColumnsToKeep[i]);
        }
        curOutputColumnNames.push_back(curOutputColumnName);
        tcapString += this->getTCAPString(curLeftTupleSetName,
                                          curLeftColumnsToKeep,
                                          curInputColumnsToApply,
                                          curOutputTupleSetName,
                                          curOutputColumnNames,
                                          curOutputColumnName,
                                          "APPLY",
                                          myComputationName,
                                          myLambdaName);

        // Step 4. do a filter to remove false rows
        outputColumns.clear();
        outputTupleSetName = "filtedOutFor_" + myLambdaName + "_" + myComputationName;
        tcapString += outputTupleSetName + "(" + curLeftColumnsToKeep[0];
        outputColumns.push_back(curLeftColumnsToKeep[0]);
        for (unsigned int i = 1; i < curLeftColumnsToKeep.size(); i++) {
            tcapString += ", " + curLeftColumnsToKeep[i];
            outputColumns.push_back(curLeftColumnsToKeep[0]);
        }
        tcapString += ") <= FILTER (" + curOutputTupleSetName + "(" + curOutputColumnName + "), ";
        tcapString += curOutputTupleSetName + "(" + curLeftColumnsToKeep[0];
        for (unsigned int i = 1; i < curLeftColumnsToKeep.size(); i++) {
            tcapString += ", " + curLeftColumnsToKeep[i];
        }
        tcapString += "), '" + myComputationName + "')\n";

        // Step 5. update tupleset names, columns and columns to apply in multiInputsComp
        for (unsigned int i = 0; i < multiInputsComp->getNumInputs(); i++) {
            std::string curInput = multiInputsComp->getNameForIthInput(i);
            auto iter =
                std::find(curLeftColumnsToKeep.begin(), curLeftColumnsToKeep.end(), curInput);
            if (iter != curLeftColumnsToKeep.end()) {
                multiInputsComp->setTupleSetNameForIthInput(i, outputTupleSetName);
                multiInputsComp->setInputColumnsForIthInput(i, curLeftColumnsToKeep);
                multiInputsComp->setInputColumnsToApplyForIthInput(i, curInputColumnsToApply);
            }
        }
        return tcapString;
    }
};
}

#endif
