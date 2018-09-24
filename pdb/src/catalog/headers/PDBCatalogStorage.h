//
// Created by dimitrije on 9/5/18.
//

#ifndef CATALOGREFACTORING_PDBCATALOGSTORAGE_H
#define CATALOGREFACTORING_PDBCATALOGSTORAGE_H

#include <sqlite_orm.h>

#include "PDBCatalogNode.h"
#include "PDBCatalogDatabase.h"
#include "PDBCatalogType.h"
#include "PDBCatalogSet.h"

namespace pdb {

  /***
   * This method makes the sqlite storage.
   * Note : it has to be defined like this in order for the auto keyword to work.
   * @param location - this is the location where we are creating the storage
   * @return the created storage
   */
  inline auto makeStorage(const std::string *location) {

    // creates the storage
    return sqlite_orm::make_storage(*location, PDBCatalogDatabase::getSchema(),
                                               PDBCatalogSet::getSchema(),
                                               PDBCatalogNode::getSchema(),
                                               PDBCatalogType::getSchema());
  }

  /**
   * This is the type for the storage I'm just using it because it is easier
   */
  using PDBCatalogStorage = decltype(makeStorage(nullptr));
}

#endif //CATALOGREFACTORING_PDBCATALOGSTORAGE_H
