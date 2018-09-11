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
                const long type) : setIdentifier(database + ":" + name), name(name), database(database), type(std::make_shared<int>(type)) {}


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
  std::shared_ptr<int> type;

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
                                           sqlite_orm::foreign_key(&PDBCatalogSet::type).references(&PDBCatalogType::id),
                                           sqlite_orm::primary_key(&PDBCatalogSet::setIdentifier));
  }

};

/**
 * This is just a definition for the shared pointer on the type
 */
class PDBCatalogSetOnNodes;
typedef std::shared_ptr<PDBCatalogSetOnNodes> PDBCatalogSetOnNodesPtr;

/**
 * This marks that a particular node has data of a particular set
 * This class is intended to be used inside the class
 */
class PDBCatalogSetOnNodes {
public:

  /**
   * The default constructor needed by the orm
   */
  PDBCatalogSetOnNodes() = default;

  /**
   * The initialization constructor
   * @param setIdentifier - the set identifier is a string in the form of "dbName:setName"
   * @param node - the node is the node identifier setring in the form of the "ip:port"
   */
  PDBCatalogSetOnNodes(const std::string &setIdentifier, const std::string &node) : setIdentifier(setIdentifier),
                                                                                    node(node) {}
  /**
   * The identifier of the set
   */
  std::string setIdentifier;

  /**
   * The id of the node that contains the set
   */
  std::string node;

  /**
   * Return the schema of the database object
   * @return the schema
   */
  static auto getSchema() {

    // return the schema
    return sqlite_orm::make_table("setOnNodes", sqlite_orm::make_column("setOnNode", &PDBCatalogSetOnNodes::setIdentifier),
                                                sqlite_orm::make_column("nodeWithSet", &PDBCatalogSetOnNodes::node),
                                                sqlite_orm::foreign_key(&PDBCatalogSetOnNodes::node).references(&PDBCatalogNode::nodeID),
                                                sqlite_orm::foreign_key(&PDBCatalogSetOnNodes::setIdentifier).references(&PDBCatalogSet::setIdentifier));
  }

};

}

#endif //PDB_PDBCATALOGSET_H
