//
// Created by dimitrije on 8/5/18.
//

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
