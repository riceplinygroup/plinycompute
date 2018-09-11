#include <iostream>
#include "PDBCatalog.h"
#include <qunit.h>

class Tests {

  QUnit::UnitTest qunit;

  /**
   * This test tests a simple aggregation
   */
  void test() {

    pdb::PDBCatalog catalog("out.sqlite");

    std::string error;

    // create the databases
    QUNIT_IS_TRUE(catalog.registerDatabase(std::make_shared<pdb::PDBCatalogDatabase>("db1"), error));
    QUNIT_IS_TRUE(catalog.registerDatabase(std::make_shared<pdb::PDBCatalogDatabase>("db2"), error));

    // create a type
    QUNIT_IS_TRUE(catalog.registerType(std::make_shared<pdb::PDBCatalogType>(8341, "built-in", "Type1", std::vector<char>()), error));
    QUNIT_IS_TRUE(catalog.registerType(std::make_shared<pdb::PDBCatalogType>(8342, "built-in", "Type2", std::vector<char>()), error));

    // create the set
    QUNIT_IS_TRUE(catalog.registerSet(std::make_shared<pdb::PDBCatalogSet>("set1", "db1", 8341), error));
    QUNIT_IS_TRUE(catalog.registerSet(std::make_shared<pdb::PDBCatalogSet>("set2", "db1", 8341), error));
    QUNIT_IS_TRUE(catalog.registerSet(std::make_shared<pdb::PDBCatalogSet>("set3", "db2", 8342), error));

    // create the nodes
    QUNIT_IS_TRUE(catalog.registerNode(std::make_shared<pdb::PDBCatalogNode>("localhost:8080", "localhost", 8080, "master"), error));
    QUNIT_IS_TRUE(catalog.registerNode(std::make_shared<pdb::PDBCatalogNode>("localhost:8081", "localhost", 8081, "worker"), error));
    QUNIT_IS_TRUE(catalog.registerNode(std::make_shared<pdb::PDBCatalogNode>("localhost:8082", "localhost", 8082, "worker"), error));

    // assign the nodes to sets
    QUNIT_IS_TRUE(catalog.addNodeToSet("localhost:8081", "db1", "set1", error));
    QUNIT_IS_TRUE(catalog.addNodeToSet("localhost:8082", "db1", "set1", error));
    QUNIT_IS_TRUE(catalog.addNodeToSet("localhost:8082", "db2", "set3", error));

    // check the exists functions for databases
    QUNIT_IS_TRUE(catalog.databaseExists("db1"));
    QUNIT_IS_TRUE(catalog.databaseExists("db2"));

    // check the exists functions for sets
    QUNIT_IS_TRUE(catalog.setExists("db1", "set1"));
    QUNIT_IS_TRUE(catalog.setExists("db1", "set2"));
    QUNIT_IS_TRUE(catalog.setExists("db2", "set3"));

    QUNIT_IS_FALSE(catalog.setExists("db1", "set3"));
    QUNIT_IS_FALSE(catalog.setExists("db2", "set1"));
    QUNIT_IS_FALSE(catalog.setExists("db2", "set2"));

    // check if the types exist
    QUNIT_IS_TRUE(catalog.typeExists(8341));
    QUNIT_IS_TRUE(catalog.typeExists(8342));

    // check if the node exist
    QUNIT_IS_TRUE(catalog.nodeExists("localhost:8080"));
    QUNIT_IS_TRUE(catalog.nodeExists("localhost:8081"));
    QUNIT_IS_TRUE(catalog.nodeExists("localhost:8082"));

    // check the get method for the database
    auto db = catalog.getDatabase("db1");

    QUNIT_IS_EQUAL(db->name, "db1");
    QUNIT_IS_TRUE(db->createdOn > 0)

    db = catalog.getDatabase("db2");

    QUNIT_IS_EQUAL(db->name, "db2");
    QUNIT_IS_TRUE(db->createdOn > 0)

    // check the get method for the sets
    auto set = catalog.getSet("db1", "set1");

    QUNIT_IS_EQUAL(set->name, "set1")
    QUNIT_IS_EQUAL(set->setIdentifier, "db1:set1")
    QUNIT_IS_EQUAL(set->database, "db1")
    QUNIT_IS_EQUAL(*set->type, 8341)


    set = catalog.getSet("db1", "set2");

    QUNIT_IS_EQUAL(set->name, "set2")
    QUNIT_IS_EQUAL(set->setIdentifier, "db1:set2")
    QUNIT_IS_EQUAL(set->database, "db1")
    QUNIT_IS_EQUAL(*set->type, 8341)

    set = catalog.getSet("db2", "set3");

    QUNIT_IS_EQUAL(set->name, "set3")
    QUNIT_IS_EQUAL(set->setIdentifier, "db2:set3")
    QUNIT_IS_EQUAL(set->database, "db2")
    QUNIT_IS_EQUAL(*set->type, 8342)

    set = catalog.getSet("db1", "set3");
    QUNIT_IS_TRUE(set == nullptr);

    // check the nodes
    auto node = catalog.getNode("localhost:8080");

    QUNIT_IS_EQUAL(node->nodeID, "localhost:8080");
    QUNIT_IS_EQUAL(node->nodeType, "master");
    QUNIT_IS_EQUAL(node->address, "localhost");
    QUNIT_IS_EQUAL(node->port, 8080);

    node = catalog.getNode("localhost:8081");

    QUNIT_IS_EQUAL(node->nodeID, "localhost:8081");
    QUNIT_IS_EQUAL(node->nodeType, "worker");
    QUNIT_IS_EQUAL(node->address, "localhost");
    QUNIT_IS_EQUAL(node->port, 8081);

    node = catalog.getNode("localhost:8082");

    QUNIT_IS_EQUAL(node->nodeID, "localhost:8082");
    QUNIT_IS_EQUAL(node->nodeType, "worker");
    QUNIT_IS_EQUAL(node->address, "localhost");
    QUNIT_IS_EQUAL(node->port, 8082);

    // test if the sets are properly assigned to nodes
    auto nodes = catalog.getNodesWithSet("db1", "set1");
    QUNIT_IS_EQUAL(nodes.size(), 2);

    nodes = catalog.getNodesWithSet("db2", "set3");
    QUNIT_IS_EQUAL(nodes.size(), 1);

    nodes = catalog.getNodesWithDatabase("db1");
    QUNIT_IS_EQUAL(nodes.size(), 2);

    nodes = catalog.getNodesWithDatabase("db1");
    QUNIT_IS_EQUAL(nodes.size(), 2);

    auto t1 = catalog.getTypeWithoutLibrary(8341);
    auto t2 = catalog.getTypeWithoutLibrary("Type1");

    QUNIT_IS_EQUAL(t1->name, t2->name)
    QUNIT_IS_EQUAL(t1->id, t2->id)
    QUNIT_IS_EQUAL(t1->typeCategory, t2->typeCategory)

    // print out the catalog
    std::cout << catalog.listNodesInCluster() << std::endl;
    std::cout << catalog.listRegisteredDatabases() << std::endl;
    std::cout << catalog.listUserDefinedTypes() << std::endl;

    // check if we are good.
    QUNIT_IS_EQUAL(catalog.numRegisteredTypes(), 2);

    // remove the database
    catalog.removeDatabase("db1", error);

    // check the get method for the sets
    QUNIT_IS_TRUE(!catalog.setExists("db1", "set1"));
    QUNIT_IS_TRUE(!catalog.setExists("db1", "set2"));

    // remove the node from the set
    QUNIT_IS_EQUAL(catalog.getNodesWithSet("db2", "set3").size(), 1);
    QUNIT_IS_TRUE(catalog.removeNodeFromSet("localhost:8082", "db2", "set3", error));
    QUNIT_IS_EQUAL(catalog.getNodesWithSet("db2", "set3").size(), 0);

  }

 public:

  explicit Tests(std::ostream & out, int verboseLevel = QUnit::verbose): qunit(out, verboseLevel) {}

  /**
   * Runs the tests
   * @return if the tests succeeded
   */
  int run() {

    // run tests
    test();

    // return the errors
    return qunit.errors();
  }

};

int main() {
  return Tests(std::cerr).run();
}