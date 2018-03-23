#include <cstddef>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>
#include <chrono>
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

#include "InterfaceFunctions.h"
#include "Employee.h"
#include "Supervisor.h"
#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "PDBClient.h"
#include "DistributedStorageManagerClient.h"
#include "ScanEmployeeSet.h"
#include "WriteStringSet.h"
#include "EmployeeSelection.h"
#include "SharedEmployee.h"
#include "Set.h"
#include "DataTypes.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

#include "SharedEmployee.h"
#include "EmployeeSelection.h"
#include "ScanEmployeeSet.h"
#include "WriteStringSet.h"
#include "Computation.h"

using namespace pdb;

int main() {

  // load up the allocator with RAM
  makeObjectAllocatorBlock(1024 * 1024 * 1024, true);

  Handle<Supervisor> super = makeObject<Supervisor>("Joe", 20);

  // A vector of handle on the allocation block
  for (int i = 0; i < 100; i ++) {
    Handle<Employee> temp = makeObject<Employee>("Steve Stevens", 20 + (i % 29));
    super->addEmp(temp);
  }

  RefCountedObject<Supervisor>* super_ref_object = getHandle(*super);

  // load up the allocator with RAM
  makeObjectAllocatorBlock(1024 * 1024 * 1024, true);

  Handle<Vector<Handle<Supervisor>>> supers = makeObject<Vector<Handle<Supervisor>>>(100);
  std::cout << "Are " << getBytesAvailableInCurrentAllocatorBlock()
            << " bytes left in the current allocation block.\n";
  for (int i = 0; i < 100; i++) {
    (*supers)[i] = makeObject<Supervisor>();
  }
  std::cout << "Are " << getBytesAvailableInCurrentAllocatorBlock()
            << " bytes left in the current allocation block.\n";

  // Have the supervisor slots on the active block
  for (int i = 0; i < 50; i++) {
    (*supers)[i] = super_ref_object;
  } // Duplicate copying should happen here.
  for (int i = 50; i < 100; i++) {
    (*supers)[i] = super;
  } // Duplicate copying should happen here.

  std::cout << "First address is " << (*supers)[0].getOffset() + ((char *) &((*supers)[0])) << std::endl;
  std::cout << "Ref assignment address is " << (*supers)[25].getOffset() + ((char *) &((*supers)[25])) << std::endl;

  std::cout << "Handle assignment address is " << (*supers)[75].getOffset()  + ((char *) &((*supers)[75])) << std::endl;


//    // load up the allocator with RAM
//    makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//    Handle<Vector<Handle<Supervisor>>> superVector = makeObject<Vector<Handle<Supervisor>>>(100);
//    for (int i = 0; i < 100; i++) {
//        superVector->push_back(makeObject<Supervisor>("Joe", 10));
//    }
//
//    makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//    Handle<Vector<Handle<Supervisor>>> superVectorNew = makeObject<Vector<Handle<Supervisor>>>(100);
//
//    superVectorNew = superVector;
//
//    for (int i = 0; i < 10; i++) {
//        std::cout << ((*superVectorNew)[i]->getName())->c_str();
//    }

  // load up the allocator with RAM
//    makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//    Handle<Vector<Handle<Employee>>> employeeVector = makeObject<Vector<Handle<Employee>>>(1);
//    (*employeeVector)[0] = makeObject<Employee>("Joe", 10);
//
//    makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//    Handle<Vector<Handle<Employee>>> employeeVectorNew = makeObject<Vector<Handle<Employee>>>(1);
//
//    employeeVectorNew = employeeVector;
//
//    std::cout << ((*employeeVectorNew)[0]->getName())->c_str() << std::endl;


  // load up the allocator with RAM
//    makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//    Handle<Vector<Handle<SharedEmployee>>> employeeVector = makeObject<Vector<Handle<SharedEmployee>>>(1);
//    employeeVector->push_back(makeObject<SharedEmployee>("Joe", 10));
//
//    makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//    Handle<Vector<Handle<SharedEmployee>>> employeeVectorNew = makeObject<Vector<Handle<SharedEmployee>>>(1);
//
//    employeeVectorNew = employeeVector;
//
//    std::cout << ((*employeeVectorNew)[0]->getName())->c_str() << std::endl;
//=====================================================================================
//    makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//    Handle<Computation> myScanSet = makeObject<ScanEmployeeSet>("chris_db", "chris_set");
//    Handle<Computation> myQuery = makeObject<EmployeeSelection>();
//    myQuery->setInput(myScanSet);
//    Handle<Computation> myWriteSet = makeObject<WriteStringSet>("chris_db", "output_set1");
//    myWriteSet->setInput(myQuery);
//
//
//    makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//
//    Handle<Computation> newWriteSet = makeObject<WriteStringSet>();
//    newWriteSet = myWriteSet;
//
//    std::cout << newWriteSet->getSetName() << std::endl;
// ============================================================================================

//  makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//  Handle<Computation> myScanSet = makeObject<ScanEmployeeSet>("chris_db", "chris_set");
//  Handle<Computation> myQuery = makeObject<EmployeeSelection>();
//  myQuery->setInput(myScanSet);
//  Handle<Computation> myWriteSet = makeObject<WriteStringSet>("chris_db", "output_set1");
//  myWriteSet->setInput(myQuery);
//
//  Handle<Vector<Handle<Computation>>> comps = makeObject<Vector<Handle<Computation>>>();
//  comps->push_back(myScanSet);
//  comps->push_back(myQuery);
//  comps->push_back(myWriteSet);
//
//  Handle<Vector<Handle<Computation>>> newComps = makeObject<Vector<Handle<Computation>>>();
//  makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//  newComps = deepCopyToCurrentAllocationBlock(comps);
//  std::cout << (*newComps)[2]->getSetName() << std::endl;
//
//  makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//  Handle<Vector<Handle<Computation>>> newComps2 = makeObject<Vector<Handle<Computation>>>();
//  newComps2 = newComps;
//  std::cout << (*newComps2)[2]->getSetName() << std::endl;
////    =============================================================================
//
//  makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//  Handle<Computation> myScanSet = makeObject<ScanEmployeeSet>("chris_db", "chris_set");
//  Handle<Computation> myQuery = makeObject<EmployeeSelection>();
//  myQuery->setInput(myScanSet);
//  Handle<Computation> myWriteSet = makeObject<WriteStringSet>("chris_db", "output_set1");
//  myWriteSet->setInput(myQuery);
//  Handle<Vector<Handle<Computation>>> comps = makeObject<Vector<Handle<Computation>>>();
//  comps->push_back(myScanSet);
//  comps->push_back(myQuery);
//  comps->push_back(myWriteSet);
//
//  Handle<Vector<Handle<Computation>>> newComps = makeObject<Vector<Handle<Computation>>>();
//
//  makeObjectAllocatorBlock(1024 * 1024 * 1024, true);
//  newComps = deepCopyToCurrentAllocationBlock(comps);
//  std::cout << (*newComps)[0]->getSetName() << std::endl;
}