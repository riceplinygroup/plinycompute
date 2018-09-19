//
// Created by dimitrije on 9/4/18.
//

#ifndef PDB_PDBCATALOGSET_H
#define PDB_PDBCATALOGSET_H

#include <string>
#include <sqlite_orm.h>
#include "PDBCatalogDatabase.h"
#include "PDBCatalogType.h"
#include "PDBCatalogNode.h"

namespace pdb {

/**
 * This is just a definition for the shared pointer on the type
 */
class PDBCatalogSet;
typedef std::shared_ptr<PDBCatalogSet> PDBCatalogSetPtr;

/**
 * A class to map the sets
 */
class PDBCatalogSet {
 public:

  /**
   * The default constructor for the set required by the orm
   */
  PDBCatalogSet() = default;

  /**
   * The initialization constructor
   * @param name - the name of the set
   * @param database - the database the set belongs to
   * @param type - the id of the set type, something like 8xxx
   */
  PDBCatalogSet(const std::string &name,
                const std::string &database,
                const std::string &type) : setIdentifier(database + ":" + name), name(name), database(database), type(std::make_shared<std::string>(type)) {}


  /**
   * The set is a string of the form "dbName:setName"
   */
  std::string setIdentifier;

  /**
   * The name of the set
   */
  std::string name;

  /**
   * The database of the set
   */
  std::string database;

  /**
   * The type of the set
   */
  std::shared_ptr<std::string> type;

  /**
   * Return the schema of the database object
   * @return the schema
   */
  static auto getSchema() {

    // return the schema
    return sqlite_orm::make_table("sets",  sqlite_orm::make_column("setIdentifier", &PDBCatalogSet::setIdentifier),
                                           sqlite_orm::make_column("setName", &PDBCatalogSet::name),
                                           sqlite_orm::make_column("setDatabase", &PDBCatalogSet::database),
                                           sqlite_orm::make_column("setType", &PDBCatalogSet::type),
                                           sqlite_orm::foreign_key(&PDBCatalogSet::database).references(&PDBCatalogDatabase::name),
                                           sqlite_orm::foreign_key(&PDBCatalogSet::type).references(&PDBCatalogType::name),
                                           sqlite_orm::primary_key(&PDBCatalogSet::setIdentifier));
  }

};

}

#endif //PDB_PDBCATALOGSET_H
