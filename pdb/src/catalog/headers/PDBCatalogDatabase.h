//
// Created by dimitrije on 9/4/18.
//

#ifndef PDB_PDBCATALOGDATABASE_H
#define PDB_PDBCATALOGDATABASE_H

#include <string>
#include <sqlite_orm.h>

namespace pdb {

/**
 * This is just a definition for the shared pointer on the type
 */
class PDBCatalogDatabase;
typedef std::shared_ptr<PDBCatalogDatabase> PDBCatalogDatabasePtr;

class PDBCatalogDatabase {

public:

  /**
   * The default constructor needed by the orm
   */
  PDBCatalogDatabase() = default;

  /**
   * The initializer constructor
   * @param name - the name of the database
   */
  explicit PDBCatalogDatabase(std::string name) : name(std::move(name)) {}

  /**
   * The initializer constructor
   * @param name - the name of the database
   * @param createdOn - the time the set was created on
   */
  PDBCatalogDatabase(std::string name, long createdOn) : name(std::move(name)), createdOn(createdOn) {}

  /**
   * The name of the database
   */
  std::string name;

  /**
   * The timestamp this database is created on
   */
  long createdOn;

  /**
   * Return the schema of the database object
   * @return the schema
   */
  static auto getSchema() {

    // return the schema
    return sqlite_orm::make_table("databases", sqlite_orm::make_column("databaseName", &PDBCatalogDatabase::name, sqlite_orm::primary_key()),
                                               sqlite_orm::make_column("databaseCreatedOn", &PDBCatalogDatabase::createdOn));
  }
};

}



#endif //PDB_PDBCATALOGDATABASE_H
