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

#ifndef PDB_CPPLAMBDACREATIONFUNCTIONS_H
#define PDB_CPPLAMBDACREATIONFUNCTIONS_H

#include "Lambda.h"
#include "Ptr.h"
#include "AttAccessLambda.h"
#include "AndLambda.h"
#include "SelfLambdaCreationFunctions.h"
#include "MethodCallLambdaCreationFunctions.h"
#include "EqualsLambdaCreationFunctions.h"
#include "SimpleComputeExecutor.h"
#include "CPlusPlusLambda.h"
#include "TypeName.h"

namespace pdb {

/**
 * these next ten functions are used to create PDB Lambdas out of C++ lambdas
 */

/**
 * // TODO add proper description
 * @tparam ParamOne // TODO add proper description
 * @tparam F // TODO add proper description
 * @param pOne // TODO add proper description
 * @param arg // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ParamOne, typename F>
auto makeLambda(Handle<ParamOne>& pOne, F arg) -> LambdaTree<std::enable_if_t<std::is_reference<decltype(arg(pOne))>::value,
                                                                              Ptr<typename std::remove_reference<decltype(arg(pOne))>::type>>> {
  Handle<Nothing> p2, p3, p4, p5;
  return LambdaTree<Ptr<typename std::remove_reference<decltype(arg(pOne))>::type>>(
    std::make_shared<CPlusPlusLambda<F,
                                     Ptr<typename std::remove_reference<decltype(arg(pOne))>::type>,
                                     ParamOne>>(arg, pOne, p2, p3, p4, p5));
}

/**
 * // TODO add proper description
 * @tparam ParamOne // TODO add proper description
 * @tparam F // TODO add proper description
 * @param pOne // TODO add proper description
 * @param arg // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ParamOne, typename F>
auto makeLambda(Handle<ParamOne>& pOne, F arg) -> LambdaTree<std::enable_if_t<!std::is_reference<decltype(arg(pOne))>::value,
                                                                              decltype(arg(pOne))>> {
  Handle<Nothing> p2, p3, p4, p5;
  return LambdaTree<decltype(arg(pOne))>(std::make_shared<CPlusPlusLambda<F,
                                                                          decltype(arg(pOne)),
                                                                          ParamOne>>(arg, pOne, p2, p3, p4, p5));
}

/**
 * // TODO add proper description
 * @tparam ParamOne // TODO add proper description
 * @tparam ParamTwo // TODO add proper description
 * @tparam F // TODO add proper description
 * @param pOne // TODO add proper description
 * @param pTwo // TODO add proper description
 * @param arg // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ParamOne, typename ParamTwo, typename F>
auto makeLambda(Handle<ParamOne>& pOne, Handle<ParamTwo>& pTwo, F arg) -> LambdaTree<std::enable_if_t<std::is_reference<decltype(arg(pOne, pTwo))>::value,
                                                                                                      Ptr<typename std::remove_reference<decltype(arg(pOne, pTwo))>::type>>> {

  Handle<Nothing> p3, p4, p5;
  return LambdaTree<Ptr<typename std::remove_reference<decltype(arg(pOne, pTwo))>::type>>(std::make_shared<CPlusPlusLambda<F,
                                                                                                                           Ptr<typename std::remove_reference<decltype(arg(pOne, pTwo))>::type>,
                                                                                                                           ParamOne,
                                                                                                                           ParamTwo>>(arg, pOne, pTwo, p3, p4, p5));
}

/**
 * // TODO add proper description
 * @tparam ParamOne // TODO add proper description
 * @tparam ParamTwo // TODO add proper description
 * @tparam F // TODO add proper description
 * @param pOne // TODO add proper description
 * @param pTwo // TODO add proper description
 * @param arg // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ParamOne, typename ParamTwo, typename F>
auto makeLambda(Handle<ParamOne>& pOne, Handle<ParamTwo>& pTwo, F arg) -> LambdaTree<std::enable_if_t<!std::is_reference<decltype(arg(pOne, pTwo))>::value,
                                                                                                      decltype(arg(pOne, pTwo))>> {
  Handle<Nothing> p3, p4, p5;
  return LambdaTree<decltype(arg(pOne, pTwo))>(std::make_shared<CPlusPlusLambda<F, decltype(arg(pOne, pTwo)), ParamOne, ParamTwo>>(arg, pOne, pTwo, p3, p4, p5));
}

/**
 * // TODO add proper description
 * @tparam ParamOne // TODO add proper description
 * @tparam ParamTwo // TODO add proper description
 * @tparam ParamThree // TODO add proper description
 * @tparam F // TODO add proper description
 * @param pOne // TODO add proper description
 * @param pTwo // TODO add proper description
 * @param pThree // TODO add proper description
 * @param arg // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ParamOne, typename ParamTwo, typename ParamThree, typename F>

  auto makeLambda(Handle<ParamOne>& pOne, Handle<ParamTwo>& pTwo, Handle<ParamThree> pThree, F arg) -> LambdaTree<std::enable_if_t<std::is_reference<decltype(arg(pOne, pTwo, pThree))>::value,
                                                                                                                                   Ptr<typename std::remove_reference<decltype(arg(pOne, pTwo, pThree))>::type>>> {
  Handle<Nothing> p4, p5;

  return LambdaTree<Ptr<typename std::remove_reference<decltype(arg(pOne, pTwo, pThree))>::type>>(std::make_shared<CPlusPlusLambda<F,
                                                                                                                                   Ptr<typename std::remove_reference<decltype(arg(pOne, pTwo, pThree))>::type>,
                                                                                                                                   ParamOne,
                                                                                                                                   ParamTwo,
                                                                                                                                   ParamThree>>(arg, pOne, pTwo, pThree, p4, p5));
}

/**
 * // TODO add proper description
 * @tparam ParamOne // TODO add proper description
 * @tparam ParamTwo // TODO add proper description
 * @tparam ParamThree // TODO add proper description
 * @tparam F // TODO add proper description
 * @param pOne // TODO add proper description
 * @param pTwo // TODO add proper description
 * @param pThree // TODO add proper description
 * @param arg // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ParamOne, typename ParamTwo, typename ParamThree, typename F>
auto makeLambda(Handle<ParamOne>& pOne, Handle<ParamTwo>& pTwo, Handle<ParamThree>& pThree, F arg) -> LambdaTree<std::enable_if_t<!std::is_reference<decltype(arg(pOne, pTwo, pThree))>::value,
                                                                                                                                  decltype(arg(pOne, pTwo, pThree))>> {
  Handle<Nothing> p4, p5;
  return LambdaTree<decltype(arg(pOne, pTwo, pThree))>(std::make_shared<CPlusPlusLambda<F,
                                                                                        decltype(arg(pOne, pTwo, pThree)),
                                                                                        ParamOne,
                                                                                        ParamTwo,
                                                                                        ParamThree>>(arg, pOne, pTwo, pThree, p4, p5));
}

/**
 * // TODO add proper description
 * @tparam ParamOne // TODO add proper description
 * @tparam ParamTwo  // TODO add proper description
 * @tparam ParamThree // TODO add proper description
 * @tparam ParamFour // TODO add proper description
 * @tparam F // TODO add proper description
 * @param pOne // TODO add proper description
 * @param pTwo // TODO add proper description
 * @param pThree // TODO add proper description
 * @param pFour // TODO add proper description
 * @param arg // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ParamOne, typename ParamTwo, typename ParamThree, typename ParamFour, typename F>
auto makeLambda(Handle<ParamOne>& pOne, Handle<ParamTwo>& pTwo, Handle<ParamThree>& pThree, Handle<ParamFour>& pFour, F arg) -> LambdaTree<std::enable_if_t<std::is_reference<decltype(arg(pOne, pTwo, pThree, pFour))>::value,
                                                                                                                                                            Ptr<typename std::remove_reference<decltype(arg(pOne, pTwo, pThree, pFour))>::type>>> {
  Handle<Nothing> p5;
  return LambdaTree<Ptr<typename std::remove_reference<decltype(arg(pOne, pTwo, pThree, pFour))>::type>>(std::make_shared<CPlusPlusLambda<F,
                                                                                                                                          Ptr<typename std::remove_reference<decltype(arg(pOne, pTwo, pThree, pFour))>::type>,
                                                                                                                                          ParamOne,
                                                                                                                                          ParamTwo,
                                                                                                                                          ParamThree,
                                                                                                                                          ParamFour>>(arg, pOne, pTwo, pThree, pFour, p5));
}

/**
 * // TODO add proper description
 * @tparam ParamOne // TODO add proper description
 * @tparam ParamTwo // TODO add proper description
 * @tparam ParamThree // TODO add proper description
 * @tparam ParamFour // TODO add proper description
 * @tparam F // TODO add proper description
 * @param pOne // TODO add proper description
 * @param pTwo // TODO add proper description
 * @param pThree // TODO add proper description
 * @param pFour // TODO add proper description
 * @param arg // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ParamOne, typename ParamTwo, typename ParamThree, typename ParamFour, typename F>
auto makeLambda(Handle<ParamOne>& pOne, Handle<ParamTwo>& pTwo, Handle<ParamThree>& pThree, Handle<ParamFour>& pFour, F arg) -> LambdaTree<
std::enable_if_t<!std::is_reference<decltype(arg(pOne, pTwo, pThree, pFour))>::value, decltype(arg(pOne, pTwo, pThree, pFour))>> {

  Handle<Nothing> p5;
  return LambdaTree<decltype(arg(pOne, pTwo, pThree, pFour))>(std::make_shared<CPlusPlusLambda<F,
                                                                                               decltype(arg(pOne, pTwo, pThree, pFour)),
                                                                                               ParamOne,
                                                                                               ParamTwo,
                                                                                               ParamThree,
                                                                                               ParamFour>>(arg, pOne, pTwo, pThree, pFour, p5));
}

/**
 * // TODO add proper description
 * @tparam ParamOne // TODO add proper description
 * @tparam ParamTwo // TODO add proper description
 * @tparam ParamThree // TODO add proper description
 * @tparam ParamFour // TODO add proper description
 * @tparam ParamFive // TODO add proper description
 * @tparam F // TODO add proper description
 * @param pOne // TODO add proper description
 * @param pTwo // TODO add proper description
 * @param pThree // TODO add proper description
 * @param pFour // TODO add proper description
 * @param pFive // TODO add proper description
 * @param arg // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ParamOne, typename ParamTwo, typename ParamThree, typename ParamFour, typename ParamFive, typename F>
auto makeLambda(Handle<ParamOne>& pOne,
                Handle<ParamTwo>& pTwo,
                Handle<ParamThree>& pThree,
                Handle<ParamFour>& pFour,
                Handle<ParamFive>& pFive,
                F arg) -> LambdaTree<std::enable_if_t<std::is_reference<decltype(arg(pOne, pTwo, pThree, pFour, pFive))>::value, Ptr<typename std::remove_reference<decltype(arg(pOne, pTwo, pThree, pFour, pFive))>::type>>> {

  return LambdaTree<Ptr<typename std::remove_reference<decltype(arg(pOne,
                                                                  pTwo,
                                                                  pThree,
                                                                  pFour,
                                                                  pFive))>::type>>(std::make_shared<CPlusPlusLambda<F,
                                                                                                                    Ptr<typename std::remove_reference<decltype(arg(pOne, pTwo, pThree, pFour, pFive))>::type>,
                                                                                                                    ParamOne,
                                                                                                                    ParamTwo,
                                                                                                                    ParamThree,
                                                                                                                    ParamFour,
                                                                                                                    ParamFive>>(arg, pOne, pTwo, pThree, pFour, pFive));
}

/**
 * // TODO add proper description
 * @tparam ParamOne // TODO add proper description
 * @tparam ParamTwo // TODO add proper description
 * @tparam ParamThree // TODO add proper description
 * @tparam ParamFour // TODO add proper description
 * @tparam ParamFive // TODO add proper description
 * @tparam F // TODO add proper description
 * @param pOne // TODO add proper description
 * @param pTwo // TODO add proper description
 * @param pThree // TODO add proper description
 * @param pFour // TODO add proper description
 * @param pFive // TODO add proper description
 * @param arg // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ParamOne, typename ParamTwo, typename ParamThree, typename ParamFour, typename ParamFive, typename F>
auto makeLambda(Handle<ParamOne>& pOne,
                Handle<ParamTwo>& pTwo,
                Handle<ParamThree>& pThree,
                Handle<ParamFour>& pFour,
                Handle<ParamFive>& pFive,
                F arg) -> LambdaTree<std::enable_if_t<!std::is_reference<decltype(arg(pOne, pTwo, pThree, pFour, pFive))>::value,
                                                      decltype(arg(pOne, pTwo, pThree, pFour, pFive))>> {
  return LambdaTree<decltype(arg(pOne, pTwo, pThree, pFour, pFive))>(std::make_shared<CPlusPlusLambda<F,
                                                                                                      decltype(arg(pOne, pTwo, pThree, pFour, pFive)),
                                                                                                      ParamOne,
                                                                                                      ParamTwo,
                                                                                                      ParamThree,
                                                                                                      ParamFour,
                                                                                                      ParamFive>>(arg, pOne, pTwo, pThree, pFour, pFive));
}

}
#endif //PDB_CPPLAMBDACREATIONFUNCTIONS_H
