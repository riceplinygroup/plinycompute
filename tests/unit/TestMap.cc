
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

//    getAllocator().setPolicy(noReuseAllocator);

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
    auto end1 = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to create the first map: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end1 - begin).count() << " ns."
              << std::endl;

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

    auto end2 = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to create the second map: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end2 - end1).count() << " ns."
              << std::endl;

    Handle<Map<String, Employee>> anotherMap =
        makeObject<Map<String, Employee>>();

    for (int i = 0; i < numEntriesInMap; i++) {
        String empName (std::to_string(i) + std::string(" is my number"));
        Employee myEmp("Joe Johnston " + std::to_string(i), i);
        (*anotherMap)[empName] = myEmp;
    }

    if (!benchmarkMode) {
        for (int i = 0; i < numEntriesInMap; i++) {
            String empName (std::to_string(i) + std::string(" is my number"));
            (*anotherMap)[empName].print();
            std::cout << " ";
        }
        std::cout << "\n";
    }

    auto end3 = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to create the third map: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end3 - end2).count() << " ns."
              << std::endl;

    int filedesc = open("testfile", O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    Record<Map<String, Employee>>* myBytes =
        getRecord<Map<String, Employee>>(anotherMap);
    write(filedesc, myBytes, myBytes->numBytes());
    close(filedesc);
    std::cout << "Wrote " << myBytes->numBytes() << " bytes to the file.\n";


    auto end4 = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to write the third map to a file: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end4 - end3).count() << " ns."
              << std::endl;

    std::ifstream in("testfile", std::ifstream::ate | std::ifstream::binary);
    size_t fileLen = in.tellg();
    filedesc = open("testfile", O_RDONLY);
    myBytes = (Record<Map<String, Employee>>*)malloc(fileLen);
    read(filedesc, myBytes, fileLen);
    close(filedesc);
    anotherMap = myBytes->getRootObject();
    std::cout << "Type code is " << anotherMap.getTypeCode() << "\n";

    if (!benchmarkMode) {
        for (int i = 0; i < numEntriesInMap; i++) {
            String empName (std::to_string(i) + std::string(" is my number"));
            (*anotherMap)[empName].print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
    auto end5 = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to read the objects from a file: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end5 - end4).count() << " ns."
              << std::endl;

    // now, do a deep copy
    makeObjectAllocatorBlock(allocationBlockSize, true);
    std::cout << "Allocation.\n";
    Handle<Map<String, Employee>> aFinalMap =
        makeObject<Map<String, Employee>>();
    std::cout << "Copy.\n";
    *aFinalMap = *anotherMap;
    std::cout << "Deletion.\n";
    anotherMap = nullptr;
    std::cout << "Zeroing.\n";
    bzero(myBytes, fileLen);
    free(myBytes);

    if (!benchmarkMode) {
        for (auto& a : *aFinalMap) {
            a.value.print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
    aFinalMap = nullptr;

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to write the objects to a file: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - end5).count() << " ns."
              << std::endl;

    std::cout << "Duration begin: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns."
              << std::endl;

}

#endif
