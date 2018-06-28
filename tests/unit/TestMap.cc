
#ifndef TEST_MAP_CC
#define TEST_MAP_CC

#include "PDBString.h"
#include "PDBMap.h"
#include "Employee.h"
#include "InterfaceFunctions.h"

#include <cstddef>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>
#include <ctime>
#include <chrono>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

using namespace pdb;

int main(int argc, char* argv[]) {

    //parse the parameters
    //parameter 1: size of allocation block in MB
    //parameter 2: size of each map
    //parameter 3: benchmark mode or not

    if (argc <= 3) {
        std::cout << "Usage: #sizeOfAllocationBlock(MB) #numEntriesPerMap #benchmarkMode(Y/N)" << std::endl;
    }

    size_t allocationBlockSize = (size_t)(atol(argv[1])) * (size_t)1024 * (size_t)1024;
    int numEntriesInMap = atoi(argv[2]);
    bool benchmarkMode = true;
    if (strcmp(argv[3], "N") == 0) {
       benchmarkMode = false;
    }

    auto begin = std::chrono::high_resolution_clock::now();
    makeObjectAllocatorBlock(allocationBlockSize, true);
    Handle<int> temp = makeObject<int>();

    Handle<Map<int, int>> myMap = makeObject<Map<int, int>>();

    for (int i = 0; i < numEntriesInMap; i++) {
        (*myMap)[i] = i + 120;
    }


    if (!benchmarkMode) {
        for (int i = 0; i < numEntriesInMap; i++) {
            std::cout << (*myMap)[i] << " ";
        }
        std::cout << "\n";
    }
    myMap = nullptr;

    Handle<Map<String, int>> myOtherMap = makeObject<Map<String, int>>();

    for (int i = 0; i < numEntriesInMap; i++) {
        String temp(std::to_string(i) + std::string(" is my number"));
        (*myOtherMap)[temp] = i;
    }
  
    if (!benchmarkMode) {
        for (int i = 0; i < numEntriesInMap; i++) {
            String temp(std::to_string(i) + std::string(" is my number"));
            std::cout << (*myOtherMap)[temp] << " ";
        }
        std::cout << "\n";
    }
    myOtherMap = nullptr;

    Handle<Map<Handle<String>, Handle<Employee>>> anotherMap =
        makeObject<Map<Handle<String>, Handle<Employee>>>();

    for (int i = 0; i < numEntriesInMap; i++) {
        Handle<String> empName =
            makeObject<String>(std::to_string(i) + std::string(" is my number"));
        Handle<Employee> myEmp = makeObject<Employee>("Joe Johnston " + std::to_string(i), i);
        (*anotherMap)[empName] = myEmp;
    }

    if (!benchmarkMode) {
        for (int i = 0; i < numEntriesInMap; i++) {
            Handle<String> empName =
                makeObject<String>(std::to_string(i) + std::string(" is my number"));
            (*anotherMap)[empName]->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }

    int filedesc = open("testfile", O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    Record<Map<Handle<String>, Handle<Employee>>>* myBytes =
        getRecord<Map<Handle<String>, Handle<Employee>>>(anotherMap);
    write(filedesc, myBytes, myBytes->numBytes());
    close(filedesc);
    std::cout << "Wrote " << myBytes->numBytes() << " bytes to the file.\n";

    std::ifstream in("testfile", std::ifstream::ate | std::ifstream::binary);
    size_t fileLen = in.tellg();
    filedesc = open("testfile", O_RDONLY);
    myBytes = (Record<Map<Handle<String>, Handle<Employee>>>*)malloc(fileLen);
    read(filedesc, myBytes, fileLen);
    close(filedesc);
    anotherMap = myBytes->getRootObject();
    std::cout << "Type code is " << anotherMap.getTypeCode() << "\n";

    if (!benchmarkMode) {
        for (int i = 0; i < numEntriesInMap; i++) {
            Handle<String> empName =
                makeObject<String>(std::to_string(i) + std::string(" is my number"));
            (*anotherMap)[empName]->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
    // now, do a deep copy
    makeObjectAllocatorBlock(allocationBlockSize, true);
    std::cout << "Allocation.\n";
    Handle<Map<Handle<String>, Handle<Employee>>> aFinalMap =
        makeObject<Map<Handle<String>, Handle<Employee>>>();
    std::cout << "Copy.\n";
    *aFinalMap = *anotherMap;
    std::cout << "Deletion.\n";
    anotherMap = nullptr;
    std::cout << "Zeroing.\n";
    bzero(myBytes, fileLen);
    free(myBytes);

    if (!benchmarkMode) {
        for (auto& a : *aFinalMap) {
            a.value->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
    aFinalMap = nullptr;

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to write the objects to a file: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns."
              << std::endl;

}

#endif
