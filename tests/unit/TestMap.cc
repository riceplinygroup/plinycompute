
#ifndef TEST_36_CC
#define TEST_36_CC

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

int main() {

    makeObjectAllocatorBlock(124 * 1024 * 1024, true);
    Handle<int> temp = makeObject<int>();

    Handle<Map<int, int>> myMap = makeObject<Map<int, int>>();

    for (int i = 0; i < 100; i++) {
        (*myMap)[i] = i + 120;
    }

    for (int i = 0; i < 100; i++) {
        std::cout << (*myMap)[i] << " ";
    }
    std::cout << "\n";
    myMap = nullptr;

    Handle<Map<String, int>> myOtherMap = makeObject<Map<String, int>>();

    for (int i = 0; i < 100; i++) {
        String temp(std::to_string(i) + std::string(" is my number"));
        (*myOtherMap)[temp] = i;
    }

    for (int i = 0; i < 100; i++) {
        String temp(std::to_string(i) + std::string(" is my number"));
        std::cout << (*myOtherMap)[temp] << " ";
    }
    std::cout << "\n";
    myOtherMap = nullptr;

    Handle<Map<Handle<String>, Handle<Employee>>> anotherMap =
        makeObject<Map<Handle<String>, Handle<Employee>>>();

    for (int i = 0; i < 100; i++) {
        Handle<String> empName =
            makeObject<String>(std::to_string(i) + std::string(" is my number"));
        Handle<Employee> myEmp = makeObject<Employee>("Joe Johnston " + std::to_string(i), i);
        (*anotherMap)[empName] = myEmp;
    }

    for (int i = 0; i < 100; i++) {
        Handle<String> empName =
            makeObject<String>(std::to_string(i) + std::string(" is my number"));
        (*anotherMap)[empName]->print();
        std::cout << " ";
    }
    std::cout << "\n";


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

    for (int i = 0; i < 100; i++) {
        Handle<String> empName =
            makeObject<String>(std::to_string(i) + std::string(" is my number"));
        (*anotherMap)[empName]->print();
        std::cout << " ";
    }
    std::cout << "\n";

    // now, do a deep copy
    makeObjectAllocatorBlock(124 * 1024 * 1024, true);
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

    for (auto& a : *aFinalMap) {
        a.value->print();
        std::cout << " ";
    }
    std::cout << "\n";
    aFinalMap = nullptr;
}

#endif
