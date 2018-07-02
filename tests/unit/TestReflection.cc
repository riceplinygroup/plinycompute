#include <qunit.h>
#include <iostream>
#include <SharedEmployee.h>
#include <SymbolReader.h>

class Tests {

private:

  QUnit::UnitTest qunit;

  /**
   * This test tests a simple aggregation
   */
  void testClassAttributes() {

    // load the shared library
    pdb::SymbolReader reader;
    reader.load("libraries/libSharedEmployee.so");

    // load the info about the SharedEmployee
    auto classInfo = reader.getClassInformation("SharedEmployee");

    // grab the info about the age name and salary attributes
    auto age = (*classInfo.attributes)[0];
    auto salary = (*classInfo.attributes)[1];
    auto name = (*classInfo.attributes)[2];

    // make a dummy pointer
    SharedEmployee* dummy = nullptr;

    // check the age attribute
    QUNIT_IS_EQUAL(age.name, "age");
    QUNIT_IS_EQUAL(age.size, sizeof(int));
    QUNIT_IS_EQUAL(age.type, "int");
    QUNIT_IS_EQUAL(age.offset, (size_t) &dummy->age - (size_t) dummy);

    // check the salary attribute
    QUNIT_IS_EQUAL(salary.name, "salary");
    QUNIT_IS_EQUAL(salary.size, sizeof(double));
    QUNIT_IS_EQUAL(salary.type, "double");
    QUNIT_IS_EQUAL(salary.offset, (size_t) &dummy->salary - (size_t) dummy);

    // check the salary attribute
    QUNIT_IS_EQUAL(name.name, "name");
    QUNIT_IS_EQUAL(name.size, sizeof(pdb::Handle<pdb::String>));
    QUNIT_IS_EQUAL(name.type, "pdb::Handle<pdb::String>");
    QUNIT_IS_EQUAL(name.offset, (size_t) &dummy->name - (size_t) dummy);
  }

public:

  explicit Tests(std::ostream & out, int verboseLevel = QUnit::verbose): qunit(out, verboseLevel) {}

  /**
   * Runs the tests
   * @return if the tests succeeded
   */
  int run() {

    // run tests
    testClassAttributes();

    // return the errors
    return qunit.errors();
  }

};


int main() {
  return Tests(std::cerr).run();
}
