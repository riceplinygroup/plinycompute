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

#ifndef PDB_METHODCALLLAMBDA_H
#define PDB_METHODCALLLAMBDA_H

#include <MethodCallLambda.h>

namespace pdb {

/**
 *  this bit of nasty templating defines a function that creates a LambdaBase object encapsulating a
 *  method call on an underlying object.  This
 *  particular template accepts only methods that return references, because such a method's output
 *  is converted into a pointer (for speed)
 *  rather than actually copying the method's output
 * @tparam ReturnType // TODO add proper description
 * @tparam ClassType // TODO add proper description
 * @param inputTypeName // TODO add proper description
 * @param methodName // TODO add proper description
 * @param var // TODO add proper description
 * @param returnTypeName // TODO add proper description
 * @param arg // TODO add proper description
 * @param columnBuilder // TODO add proper description
 * @param getExecutor // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ReturnType, typename ClassType>
LambdaTree<std::enable_if_t<std::is_reference<ReturnType>::value, Ptr<typename std::remove_reference<ReturnType>::type>>> makeLambdaUsingMethod(std::string inputTypeName,
                                                                                                                                                std::string methodName,
                                                                                                                                                Handle<ClassType>& var,
                                                                                                                                                std::string returnTypeName,
                                                                                                                                                ReturnType (ClassType::*arg)(),
                                                                                                                                                std::function<bool(std::string&, TupleSetPtr, int)> columnBuilder,
                                                                                                                                                std::function<SimpleComputeExecutorPtr(TupleSpec&, TupleSpec&, TupleSpec&)> getExecutor) {
  PDB_COUT << "makeLambdaFromMethod: input type code is " << var.getExactTypeInfoValue() << std::endl;
  return LambdaTree<Ptr<typename std::remove_reference<ReturnType>::type>>(std::make_shared<MethodCallLambda<Ptr<typename std::remove_reference<ReturnType>::type>, ClassType>>(inputTypeName,
                                                                                                                                                                                methodName,
                                                                                                                                                                                returnTypeName,
                                                                                                                                                                                var,
                                                                                                                                                                                columnBuilder,
                                                                                                                                                                                getExecutor));
}

/**
 * // TODO add proper description
 * @tparam ReturnType // TODO add proper description
 * @tparam ClassType // TODO add proper description
 * @param inputTypeName // TODO add proper description
 * @param methodName // TODO add proper description
 * @param var // TODO add proper description
 * @param returnTypeName // TODO add proper description
 * @param arg // TODO add proper description
 * @param columnBuilder // TODO add proper description
 * @param getExecutor // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ReturnType, typename ClassType>
LambdaTree<std::enable_if_t<!(std::is_reference<ReturnType>::value), ReturnType>> makeLambdaUsingMethod(std::string inputTypeName,
                                                                                                        std::string methodName,
                                                                                                        Handle<ClassType>& var,
                                                                                                        std::string returnTypeName,
                                                                                                        ReturnType (ClassType::*arg)(),
                                                                                                        std::function<bool(std::string&, TupleSetPtr, int)> columnBuilder,
                                                                                                        std::function<SimpleComputeExecutorPtr(TupleSpec&, TupleSpec&, TupleSpec&)> getExecutor) {

  PDB_COUT << "makeLambdaFromMethod: input type code is " << var.getExactTypeInfoValue() << std::endl;
  return LambdaTree<ReturnType>(std::make_shared<MethodCallLambda<ReturnType, ClassType>>(inputTypeName,
                                                                                          methodName,
                                                                                          returnTypeName,
                                                                                          var,
                                                                                          columnBuilder,
                                                                                          getExecutor));
}


/**
 * called if ReturnType is a reference
 * @tparam B // TODO add proper description
 * @tparam InputType // TODO add proper description
 * @param arg // TODO add proper description
 * @return // TODO add proper description
 */
template <bool B, typename InputType>
auto tryReference(InputType& arg) -> typename std::enable_if_t<B, InputType*> {
  return &arg;
}

/**
 * called if ReturnType is a reference
 * @tparam B // TODO add proper description
 * @tparam InputType // TODO add proper description
 * @param arg // TODO add proper description
 * @return // TODO add proper description
 */
template <bool B, typename InputType>
auto tryReference(InputType arg) -> typename std::enable_if_t<!B, InputType*> {
  InputType* temp = nullptr;
  return temp;
}

/**
 * // TODO add proper description
 * @param VAR // TODO add proper description
 * @tparam METHOD // TODO add proper description
 */
#define makeLambdaFromMethod(VAR, METHOD)                                                          \
    (makeLambdaUsingMethod(                                                                        \
        getTypeName<typename std::remove_reference<decltype(*VAR)>::type>(),                       \
        std::string(#METHOD),                                                                      \
        VAR,                                                                                       \
        getTypeName<typename std::remove_reference<decltype(*VAR)>::type>(),                       \
        &std::remove_reference<decltype(*VAR)>::type::METHOD,                                      \
        [](std::string& pleaseCreateThisType, TupleSetPtr input, int outAtt) {                     \
            if (pleaseCreateThisType ==                                                            \
                getTypeName<typename std::remove_reference<decltype(VAR->METHOD())>::type>()) {    \
                std::vector<typename std::remove_reference<decltype(VAR->METHOD())>::type>*        \
                    outColumn = new std::vector<                                                   \
                        typename std::remove_reference<decltype(VAR->METHOD())>::type>;            \
                input->addColumn(outAtt, outColumn, true);                                         \
                return true;                                                                       \
            }                                                                                      \
                                                                                                   \
            if (pleaseCreateThisType ==                                                            \
                getTypeName<                                                                       \
                    Ptr<typename std::remove_reference<decltype(VAR->METHOD())>::type>>()) {       \
                std::vector<Ptr<typename std::remove_reference<decltype(VAR->METHOD())>::type>>*   \
                    outColumn = new std::vector<                                                   \
                        Ptr<typename std::remove_reference<decltype(VAR->METHOD())>::type>>;       \
                input->addColumn(outAtt, outColumn, true);                                         \
                return true;                                                                       \
            }                                                                                      \
                                                                                                   \
            return false;                                                                          \
        },                                                                                         \
        [](TupleSpec& inputSchema, TupleSpec& attsToOperateOn, TupleSpec& attsToIncludeInOutput) { \
                                                                                                   \
            /* create the output tuple set */                                                      \
            TupleSetPtr output = std::make_shared<TupleSet>();                                     \
                                                                                                   \
            /* create the machine that is going to setup the output tuple set, using the input     \
             * tuple set */                                                                        \
            TupleSetSetupMachinePtr myMachine =                                                    \
                std::make_shared<TupleSetSetupMachine>(inputSchema, attsToIncludeInOutput);        \
                                                                                                   \
            /* this is the input attribute that we will process */                                 \
            std::vector<int> matches = myMachine->match(attsToOperateOn);                          \
            int whichAtt = matches[0];                                                             \
                                                                                                   \
            /* this is the output attribute */                                                     \
            int outAtt = attsToIncludeInOutput.getAtts().size();                                   \
                                                                                                   \
            return std::make_shared<SimpleComputeExecutor>(output, [=](TupleSetPtr input) {        \
                                                                                                   \
                /* if the method returns a reference, then we transform the method output into a   \
                 * pointer */                                                                      \
                if (std::is_reference<decltype(VAR->METHOD())>::value) {                           \
                                                                                                   \
                    /* set up the output tuple set */                                              \
                    myMachine->setup(input, output);                                               \
                                                                                                   \
                    /* get the column to operate on... the input type is taken from the argument   \
                     * VAR */                                                                      \
                    std::vector<typename std::remove_reference<decltype(VAR)>::type>&              \
                        inputColumn =                                                              \
                            input->getColumn<typename std::remove_reference<decltype(VAR)>::type>( \
                                whichAtt);                                                         \
                                                                                                   \
                    /* setup the output column, if it is not already set up */                     \
                    if (!output->hasColumn(outAtt)) {                                              \
                        std::vector<                                                               \
                            Ptr<typename std::remove_reference<decltype(VAR->METHOD())>::type>>*   \
                            outColumn = new std::vector<Ptr<                                       \
                                typename std::remove_reference<decltype(VAR->METHOD())>::type>>;   \
                        output->addColumn(outAtt, outColumn, true);                                \
                    }                                                                              \
                                                                                                   \
                    /* get the output column */                                                    \
                    std::vector<                                                                   \
                        Ptr<typename std::remove_reference<decltype(VAR->METHOD())>::type>>&       \
                        outColumn = output->getColumn<                                             \
                            Ptr<typename std::remove_reference<decltype(VAR->METHOD())>::type>>(   \
                            outAtt);                                                               \
                                                                                                   \
                    /* loop down the column, setting the output */                                 \
                    int numTuples = inputColumn.size();                                            \
                    outColumn.resize(numTuples);                                                   \
                    for (int i = 0; i < numTuples; i++) {                                          \
                        outColumn[i] =                                                             \
                            tryReference<std::is_reference<decltype(VAR->METHOD())>::value>(       \
                                inputColumn[i]->METHOD());                                         \
                    }                                                                              \
                    outColumn = output->getColumn<                                                 \
                        Ptr<typename std::remove_reference<decltype(VAR->METHOD())>::type>>(       \
                        outAtt);                                                                   \
                    return output;                                                                 \
                                                                                                   \
                    /* if the method does not return a reference, then we just go ahead and store  \
                     * a copy of what was returned */                                              \
                } else {                                                                           \
                                                                                                   \
                    /* set up the output tuple set */                                              \
                    myMachine->setup(input, output);                                               \
                                                                                                   \
                    /* get the column to operate on... the input type is taken from the argument   \
                     * VAR */                                                                      \
                    std::vector<typename std::remove_reference<decltype(VAR)>::type>&              \
                        inputColumn =                                                              \
                            input->getColumn<typename std::remove_reference<decltype(VAR)>::type>( \
                                whichAtt);                                                         \
                                                                                                   \
                    /* setup the output column, if it is not already set up	*/                     \
                    if (!output->hasColumn(outAtt)) {                                              \
                        std::vector<typename std::remove_reference<decltype(                       \
                            VAR->METHOD())>::type>* outColumn =                                    \
                            new std::vector<                                                       \
                                typename std::remove_reference<decltype(VAR->METHOD())>::type>;    \
                        output->addColumn(outAtt, outColumn, true);                                \
                    }                                                                              \
                                                                                                   \
                    /* get the output column */                                                    \
                    std::vector<typename std::remove_reference<decltype(VAR->METHOD())>::type>&    \
                        outColumn = output->getColumn<                                             \
                            typename std::remove_reference<decltype(VAR->METHOD())>::type>(        \
                            outAtt);                                                               \
                                                                                                   \
                    /* loop down the column, setting the output */                                 \
                    int numTuples = inputColumn.size();                                            \
                    outColumn.resize(numTuples);                                                   \
                    for (int i = 0; i < numTuples; i++) {                                          \
                        outColumn[i] = inputColumn[i]->METHOD();                                   \
                    }                                                                              \
                    return output;                                                                 \
                }                                                                                  \
            });                                                                                    \
        }))

}

#endif //PDB_METHODCALLLAMBDA_H
