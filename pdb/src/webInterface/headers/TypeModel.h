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

#ifndef PDB_TYPEMODEL_H
#define PDB_TYPEMODEL_H

#include <mustache.h>
#include <CatalogServer.h>

namespace pdb {

class TypeModel;
typedef std::shared_ptr<TypeModel> TypeModelPtr;

class TypeModel {
public:

  TypeModel(CatalogServer *catalogServer);

  /**
   * Returns the info about all the registered types in the catalog
   * @return the info about the types
   */
  mustache::data getTypes();

  /**
 *
 * @param dbName
 * @param setName
 * @return
 */
  mustache::data getType(std::string& typeID);

private:

  /**
   * Catalog server pointer
   */
  pdb::CatalogServer *catalogServer;

};

}


#endif //PDB_TYPEMODEL_H
