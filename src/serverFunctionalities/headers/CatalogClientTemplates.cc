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

#ifndef CAT_CLIENT_TEMPL_CC
#define CAT_CLIENT_TEMPL_CC

#include "CatalogClient.h"
#include "CatCreateSetRequest.h"
#include "SimpleRequest.h"
#include "SimpleRequestResult.h"

namespace pdb {

template <class DataType>
bool CatalogClient::createSet(std::string databaseName, std::string setName,
                              std::string &errMsg) {

  int16_t typeID = VTableMap::getIDByName(getTypeName<DataType>(), false);
  if (typeID == -1) {
    errMsg = "Could not find type " + getTypeName<DataType>();
    return -1;
  }

  return simpleRequest<CatCreateSetRequest, SimpleRequestResult, bool>(
      myLogger, port, address, false, 1024,
      [&](Handle<SimpleRequestResult> result) {
        if (result != nullptr) {
          if (!result->getRes().first) {
            errMsg = "Error creating set: " + result->getRes().second;
            myLogger->error("Error creating set: " + result->getRes().second);
            return false;
          }
          return true;
        }
        errMsg = "Error getting type name: got nothing back from catalog";
        return false;
      },
      databaseName, setName, typeID);
}
}

#endif
