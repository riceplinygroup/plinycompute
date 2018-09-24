#include <utility>

//
// Created by dimitrije on 9/4/18.
//

#ifndef PDB_PDBCATALOGTYPE_H
#define PDB_PDBCATALOGTYPE_H

#include <vector>
#include <string>
#include <sqlite_orm.h>

namespace pdb {

/**
 * This is just a definition for the shared pointer on the type
 */
class PDBCatalogType;
typedef std::shared_ptr<PDBCatalogType> PDBCatalogTypePtr;

class PDBCatalogType {
 public:

  /**
   * The default constructor needed by the orm
   */
  PDBCatalogType() = default;

  /**
   * The initializer constructor
   * @param id - the id of the type something like 8xxx
   * @param typeCategory - the category of the type {userDefined, builtIn}
   * @param name - the name of the type
   * @param soBytes - the bytes of the .so library
   */
  PDBCatalogType(int id, std::string typeCategory, std::string name, const std::vector<char> &soBytes)
      : id(id), typeCategory(std::move(typeCategory)), name(std::move(name)), soBytes(soBytes) {}

  /**
   * The id of the type
   */
  int id;

  /**
   * The category of the type TODO could be something stupid
   */
  std::string typeCategory;

  /**
   * The name of the type
   */
  std::string name;

  /**
   * The bytes of the library
   */
  std::vector<char> soBytes;

  /**
   * Return the schema of the database object
   * @return the schema
   */
  static auto getSchema() {

    // return the schema
    return sqlite_orm::make_table("types", sqlite_orm::make_column("typeID", &PDBCatalogType::id),
                                  sqlite_orm::make_column("typeCategory", &PDBCatalogType::typeCategory),
                                  sqlite_orm::make_column("typeName", &PDBCatalogType::name),
                                  sqlite_orm::make_column("typeSoBytes", &PDBCatalogType::soBytes),
                                  sqlite_orm::primary_key(&PDBCatalogType::name));
  }

};

}

#endif //PDB_PDBCATALOGTYPE_H
