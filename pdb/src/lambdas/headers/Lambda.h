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

#ifndef LAMBDA_H
#define LAMBDA_H

#include <memory>
#include <vector>
#include <functional>
#include "Object.h"
#include "Handle.h"
#include "Ptr.h"
#include "TupleSpec.h"
#include "ComputeExecutor.h"
#include "GenericLambdaObject.h"
#include "DereferenceLambda.h"
#include "MultiInputsBase.h"

namespace pdb {

/**
 *
 * @tparam ReturnType
 */
template<class ReturnType>
class Lambda {
 public:

  /**
   * create a lambda tree that returns a pointer
   * this is an implicit
   * @param treeWithPointer
   */
  Lambda(LambdaTree<Ptr<ReturnType>> treeWithPointer) {

    // a problem is that consumers of this lambda will not be able to deal with a
    // Ptr<ReturnType>...
    // so we need to add an additional operation that dereferences the pointer
    std::shared_ptr<DereferenceLambda<ReturnType>> newRoot = std::make_shared<DereferenceLambda<ReturnType>>(treeWithPointer);
    tree = newRoot;
  }

  /**
   * create a lambda tree that returns a non-pointer
   * @param tree
   */
  Lambda(LambdaTree<ReturnType> tree) : tree(tree.getPtr()) {}

  unsigned int getInputIndex() {
    return tree->getInputIndex();
  }

  /**
   * convert one of these guys to a map
   * @param returnVal
   * @param suffix
   */
  void toMap(std::map<std::string, GenericLambdaObjectPtr> &returnVal, int &suffix) {
    traverse(returnVal, tree, suffix);
  }

  std::vector<std::string> getAllInputs(MultiInputsBase *multiInputsBase) {
    std::vector<std::string> ret;
    this->getInputs(ret, tree, multiInputsBase);
    return ret;
  }

  /**
   * This is to get the TCAPString for this lambda tree
   * @param inputTupleSetName
   * @param inputColumnNames
   * @param inputColumnsToApply
   * @param childrenLambdaNames
   * @param lambdaLabel
   * @param computationName
   * @param computationLabel
   * @param outputTupleSetName
   * @param outputColumnNames
   * @param addedOutputColumnName
   * @param myLambdaName
   * @param whetherToRemoveUnusedOutputColumns
   * @param multiInputsComp
   * @param amIPartOfJoinPredicate
   * @return
   */
  std::string toTCAPString(std::string inputTupleSetName,
                           std::vector<std::string> &inputColumnNames,
                           std::vector<std::string> &inputColumnsToApply,
                           std::vector<std::string> &childrenLambdaNames,
                           int &lambdaLabel,
                           std::string computationName,
                           int computationLabel,
                           std::string &outputTupleSetName,
                           std::vector<std::string> &outputColumnNames,
                           std::string &addedOutputColumnName,
                           std::string &myLambdaName,
                           bool whetherToRemoveUnusedOutputColumns,
                           MultiInputsBase *multiInputsComp = nullptr,
                           bool amIPartOfJoinPredicate = false) {
    std::vector<std::string> tcapStrings;
    std::string outputTCAPString;
    std::vector<std::string> inputTupleSetNames;
    inputTupleSetNames.push_back(inputTupleSetName);
    std::vector<std::string> columnNames;

    for (const auto &inputColumnName : inputColumnNames) {
      columnNames.push_back(inputColumnName);
    }

    std::vector<std::string> columnsToApply;
    for (const auto &i : inputColumnsToApply) {
      columnsToApply.push_back(i);
    }

    std::vector<std::string> childrenLambdas;
    for (const auto &childrenLambdaName : childrenLambdaNames) {
      childrenLambdas.push_back(childrenLambdaName);
    }

    getTCAPString(tcapStrings,
                  inputTupleSetNames,
                  columnNames,
                  columnsToApply,
                  childrenLambdas,
                  tree,
                  lambdaLabel,
                  computationName,
                  computationLabel,
                  addedOutputColumnName,
                  myLambdaName,
                  outputTupleSetName,
                  multiInputsComp,
                  amIPartOfJoinPredicate);
    PDB_COUT << "Lambda: lambdaLabel=" << lambdaLabel << std::endl;
    bool isOutputInInput = false;
    outputColumnNames.clear();

    if (!whetherToRemoveUnusedOutputColumns) {

      for (const auto &columnName : columnNames) {
        outputColumnNames.push_back(columnName);
        if (addedOutputColumnName == columnName) {
          isOutputInInput = true;
        }
      }

      if (!isOutputInInput) {
        outputColumnNames.push_back(addedOutputColumnName);
      }

    } else {
      outputColumnNames.push_back(addedOutputColumnName);
    }

    // TODO this is very dirty and should not be done like that! For now I'm going to patch it!
    if (whetherToRemoveUnusedOutputColumns) {

      // get the last tcap string
      unsigned long last = tcapStrings.size() - 1;

      PDB_COUT << "tcapStrings[" << last << "]=" << tcapStrings[last] << std::endl;
      std::string right = tcapStrings[last].substr(tcapStrings[last].find("<="));

      // by default the end is an empty string
      std::string end;

      // check if we have an info dictionary if we have chop off the end and store it in the end variable
      if (right.find('[') != std::string::npos) {
        end = right.substr(right.find('['));
        right = right.substr(0, right.find('['));
      }

      // find the positions of the last brackets ()
      unsigned long pos1 = right.find_last_of('(');
      unsigned long pos2 = right.rfind("),");

      // empty out anything between the brackets
      right.replace(pos1 + 1, pos2 - 1 - (pos1 + 1) + 1, "");

      // combine the string and replace it
      tcapStrings[last] = outputTupleSetName + " (" + addedOutputColumnName + ") " + right + end;
    }

    // combine all the tcap strings
    for (const auto &tcapString : tcapStrings) {
      outputTCAPString.append(tcapString);
    }

    return outputTCAPString;
  }

 private:

  /**
   * in case we wrap up a non-pointer type
   */
  std::shared_ptr<TypedLambdaObject<ReturnType>> tree;

  /**
   * does the actual tree traversal
   * JiaNote: I changed below method from pre-order traversing to post-order traversing, so that  it follows
   * the lambda execution ordering
   * @param fillMe
   * @param root
   * @param startLabel
   */
  static void traverse(std::map<std::string, GenericLambdaObjectPtr> &fillMe,
                       GenericLambdaObjectPtr root,
                       int &startLabel) {

    for (int i = 0; i < root->getNumChildren(); i++) {
      GenericLambdaObjectPtr child = root->getChild(i);
      traverse(fillMe, child, startLabel);
    }

    std::string myName = root->getTypeOfLambda();
    myName = myName + "_" + std::to_string(startLabel);
    startLabel++;
    fillMe[myName] = root;
  }

  /**
   *
   * @param allInputs
   * @param root
   * @param multiInputsBase
   */
  void getInputs(std::vector<std::string> &allInputs, GenericLambdaObjectPtr root, MultiInputsBase *multiInputsBase) {

    for (int i = 0; i < root->getNumChildren(); i++) {

      GenericLambdaObjectPtr child = root->getChild(i);
      getInputs(allInputs, child, multiInputsBase);
    }

    if (root->getNumChildren() == 0) {
      for (int i = 0; i < root->getNumInputs(); i++) {
        std::string myName = multiInputsBase->getNameForIthInput(root->getInputIndex(i));
        auto iter = std::find(allInputs.begin(), allInputs.end(), myName);

        if (iter == allInputs.end()) {
          allInputs.push_back(myName);
        }
      }
    }
  }

  /**
   * JiaNote: below function is to generate a sequence of TCAP Strings for this Lambda tree
   * @param tcapStrings
   * @param inputTupleSetNames
   * @param inputColumnNames
   * @param inputColumnsToApply
   * @param childrenLambdaNames
   * @param root
   * @param lambdaLabel
   * @param computationName
   * @param computationLabel
   * @param addedOutputColumnName
   * @param myLambdaName
   * @param outputTupleSetName
   * @param multiInputsComp
   * @param amIPartOfJoinPredicate
   * @param amILeftChildOfEqualLambda
   * @param amIRightChildOfEqualLambda
   * @param parentLambdaName
   * @param isSelfJoin
   */
  static void getTCAPString(std::vector<std::string> &tcapStrings,
                            std::vector<std::string> &inputTupleSetNames,
                            std::vector<std::string> &inputColumnNames,
                            std::vector<std::string> &inputColumnsToApply,
                            std::vector<std::string> &childrenLambdaNames,
                            GenericLambdaObjectPtr root,
                            int &lambdaLabel,
                            std::string computationName,
                            int computationLabel,
                            std::string &addedOutputColumnName,
                            std::string &myLambdaName,
                            std::string &outputTupleSetName,
                            MultiInputsBase *multiInputsComp = nullptr,
                            bool amIPartOfJoinPredicate = false,
                            bool amILeftChildOfEqualLambda = false,
                            bool amIRightChildOfEqualLambda = false,
                            std::string parentLambdaName = "",
                            bool isSelfJoin = false) {

    std::vector<std::string> columnsToApply;
    std::vector<std::string> childrenLambdas;
    std::vector<std::string> inputNames;
    std::vector<std::string> inputColumns;

    if (root->getNumChildren() > 0) {

      for (const auto &i : inputColumnsToApply) {
        columnsToApply.push_back(i);
      }

      inputColumnsToApply.clear();

      for (const auto &childrenLambdaName : childrenLambdaNames) {
        childrenLambdas.push_back(childrenLambdaName);
      }

      childrenLambdaNames.clear();

      for (const auto &inputTupleSetName : inputTupleSetNames) {
        auto iter = std::find(inputNames.begin(), inputNames.end(), inputTupleSetName);
        if (iter == inputNames.end()) {
          inputNames.push_back(inputTupleSetName);
        }
      }

      inputTupleSetNames.clear();

      for (const auto &inputColumnName : inputColumnNames) {
        inputColumns.push_back(inputColumnName);
      }

      inputColumnNames.clear();
    }

    std::string myTypeName = root->getTypeOfLambda();
    PDB_COUT << "\tExtracted lambda named: " << myTypeName << "\n";
    std::string myName = myTypeName + "_" + std::to_string(lambdaLabel + root->getNumChildren());

    bool isLeftChildOfEqualLambda = false;
    bool isRightChildOfEqualLambda = false;
    bool isChildSelfJoin = false;

    GenericLambdaObjectPtr nextChild = nullptr;
    for (int i = 0; i < root->getNumChildren(); i++) {
      GenericLambdaObjectPtr child = root->getChild(i);

      if ((i + 1) < root->getNumChildren()) {
        nextChild = root->getChild(i + 1);
      }

      if (myTypeName == "==") {

        if (i == 0) {
          isLeftChildOfEqualLambda = true;
        }

        if (i == 1) {
          isRightChildOfEqualLambda = true;
        }

      }

      if ((isLeftChildOfEqualLambda || isRightChildOfEqualLambda) && (multiInputsComp != nullptr)) {

        std::string nextInputName;

        if (nextChild != nullptr) {
          nextInputName = multiInputsComp->getNameForIthInput(nextChild->getInputIndex(0));
        }

        std::string myInputName = multiInputsComp->getNameForIthInput(child->getInputIndex(0));

        if (nextInputName == myInputName) {
          isChildSelfJoin = true;
        }
      }

      getTCAPString(tcapStrings,
                    inputNames,
                    inputColumns,
                    columnsToApply,
                    childrenLambdas,
                    child,
                    lambdaLabel,
                    computationName,
                    computationLabel,
                    addedOutputColumnName,
                    myLambdaName,
                    outputTupleSetName,
                    multiInputsComp,
                    amIPartOfJoinPredicate,
                    isLeftChildOfEqualLambda,
                    isRightChildOfEqualLambda,
                    myName,
                    isChildSelfJoin);

      inputColumnsToApply.push_back(addedOutputColumnName);
      childrenLambdaNames.push_back(myLambdaName);

      if (multiInputsComp != nullptr) {
        auto iter = std::find(inputTupleSetNames.begin(), inputTupleSetNames.end(), outputTupleSetName);

        if (iter == inputTupleSetNames.end()) {
          inputTupleSetNames.push_back(outputTupleSetName);
        }

      } else {

        inputTupleSetNames.clear();
        inputTupleSetNames.push_back(outputTupleSetName);
        inputColumnNames.clear();
      }

      for (const auto &inputColumn : inputColumns) {
        auto iter =
            std::find(inputColumnNames.begin(), inputColumnNames.end(), inputColumn);
        if (iter == inputColumnNames.end()) {
          inputColumnNames.push_back(inputColumn);
        }
      }

      isLeftChildOfEqualLambda = false;
      isRightChildOfEqualLambda = false;
      isChildSelfJoin = false;
      nextChild = nullptr;
    }

    std::vector<std::string> outputColumns;
    std::string tcapString = root->toTCAPString(inputTupleSetNames,
                                                inputColumnNames,
                                                inputColumnsToApply,
                                                childrenLambdaNames,
                                                lambdaLabel,
                                                computationName,
                                                computationLabel,
                                                outputTupleSetName,
                                                outputColumns,
                                                addedOutputColumnName,
                                                myLambdaName,
                                                multiInputsComp,
                                                amIPartOfJoinPredicate,
                                                amILeftChildOfEqualLambda,
                                                amIRightChildOfEqualLambda,
                                                parentLambdaName,
                                                isSelfJoin);

    tcapStrings.push_back(tcapString);
    lambdaLabel++;

    if (multiInputsComp == nullptr) {
      inputTupleSetNames.clear();
      inputTupleSetNames.push_back(outputTupleSetName);
    }

    inputColumnNames.clear();
    for (const auto &outputColumn : outputColumns) {
      inputColumnNames.push_back(outputColumn);
    }

  }
};
}

#endif
