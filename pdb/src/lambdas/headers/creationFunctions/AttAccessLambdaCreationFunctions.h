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

#ifndef PDB_ATTACCESSLAMBDACREATIONFUNCTIONS_H
#define PDB_ATTACCESSLAMBDACREATIONFUNCTIONS_H

#include <LambdaTree.h>
#include <AttAccessLambda.h>
#include <Handle.h>
#include <Ptr.h>

namespace  pdb {

/**
 * creates a PDB lambda that returns a member of a C++ class
 * @tparam ReturnType // TODO add proper description
 * @tparam ClassType // TODO add proper description
 * @param inputTypeName // TODO add proper description
 * @param attName // TODO add proper description
 * @param attType // TODO add proper description
 * @param var // TODO add proper description
 * @param member // TODO add proper description
 * @param offset // TODO add proper description
 * @return// TODO add proper description
 */
template <typename ReturnType, typename ClassType>
LambdaTree<Ptr<ReturnType>> makeLambdaUsingMember(std::string inputTypeName,
                                                  std::string attName,
                                                  std::string attType,
                                                  Handle<ClassType>& var,
                                                  ReturnType* member,
                                                  size_t offset) {
  PDB_COUT << "makeLambdaUsingMember: input type code is " << var.getExactTypeInfoValue() << std::endl;
  return LambdaTree<Ptr<ReturnType>>(std::make_shared<AttAccessLambda<ReturnType, ClassType>>(inputTypeName,
                                                                                              attName,
                                                                                              attType,
                                                                                              var,
                                                                                              offset));
}

/**
 * we have this here only so that we can cast it to be of type VAR
 */
extern void* someRandomPointer;

/**
 * @param VAR // TODO add proper description
 * @param MEMBER // TODO add proper description
 */
#define makeLambdaFromMember(VAR, MEMBER)                                                       \
    (makeLambdaUsingMember(                                                                     \
        getTypeName<std::remove_reference<decltype(*VAR)>::type>(),                             \
        std::string(#MEMBER),                                                                   \
        getTypeName<typename std::remove_reference<decltype(VAR->MEMBER)>::type>(),             \
        VAR,                                                                                    \
        (decltype(VAR->MEMBER)*)someRandomPointer,                                              \
        ((char*)&(((std::remove_reference<decltype(*VAR)>::type*)someRandomPointer)->MEMBER)) - \
            (char*)someRandomPointer))

}

#endif //PDB_ATTACCESSLAMBDACREATIONFUNCTIONS_H
