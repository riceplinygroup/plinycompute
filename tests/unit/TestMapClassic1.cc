
#ifndef TEST_MAP_CLASSIC_CC
#define TEST_MAP_CLASSIC_CC

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

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/version.hpp>
#include <boost/serialization/serialization.hpp>
#include <iostream>
#include <fstream>


class XString {
    friend class boost::serialization::access;
    std::string s;
    template <class Archive>
    void serialize(Archive &ar, unsigned int) {
        ar & s;
    }

public:
    XString(std::string str) {
        s = str;
    }

    const std::string & getString () const {
        return s;
    }
    XString() {}

/*
    bool operator< (const XString &s1) {
        return this->getString() < s1.getString();
    }

    bool operator== (const XString &s1) {
        return this->getString() == s1.getString();
    }
*/
};

static bool operator < (const XString& a1, const XString& a2) {
    return a1.getString() < a2.getString();
}

static bool operator == (const XString& a1, const XString& a2) {
    return a1.getString() == a2.getString();
}


class Employee {
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version) {
        ar & name;
        ar & age;
        ar & department;
        ar & salary;
    }
    XString* name;
    int age;


public:
    ~Employee() {
        delete name;
    }

    Employee() {}

    Employee(std::string nameIn, int ageIn) {
        name = new XString(nameIn);
        age = ageIn;
        department = XString ("myDept");
        salary = 123.45;
    }

    Employee(const Employee& emp) {
        name = emp.name;
        age = emp.age;
        department = XString ("myDept");
        salary = 123.45;
    }

    XString department;
    double salary;    


    void print() {
        std::cout << "name is " << name->getString() << ", age is " << age << std::endl;
    }
};



int main(int argc, char* argv[]) {

    //parse the parameters
    //parameter 1: size of each map
    //parameter 2: benchmark mode or not

    if (argc <= 2) {
        std::cout << "Usage: #numEntriesPerMap #benchmarkMode(Y/N)" << std::endl;
        exit(1);
    }

    int numEntriesInMap = atoi(argv[1]);
    bool benchmarkMode = true;
    if (strcmp(argv[2], "N") == 0) {
       benchmarkMode = false;
    }

    auto begin = std::chrono::high_resolution_clock::now();

    std::map<int, int>* myMap = new std::map<int, int>();

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

    std::map<XString, int>* myOtherMap = new std::map<XString, int>();

    for (int i = 0; i < numEntriesInMap; i++) {
        XString temp(std::to_string(i) + std::string(" is my number"));
        (*myOtherMap)[temp] = i;
    }
  
    if (!benchmarkMode) {
        for (int i = 0; i < numEntriesInMap; i++) {
            XString temp(std::to_string(i) + std::string(" is my number"));
            std::cout << (*myOtherMap)[temp] << " ";
        }
        std::cout << "\n";
    }
    myOtherMap = nullptr;

    std::map<XString*, Employee*>* anotherMap =
        new std::map<XString*, Employee*>();

    for (int i = 0; i < numEntriesInMap; i++) {
        XString*  empName = new XString(std::to_string(i) + std::string(" is my number"));
        Employee* myEmp = new Employee("Joe Johnston " + std::to_string(i), i);
        (*anotherMap)[empName] = myEmp;
    }

    if (!benchmarkMode) {
        for (int i = 0; i < numEntriesInMap; i++) {
            XString* empName = new XString(std::to_string(i) + std::string(" is my number"));
            (*anotherMap)[empName]->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }

    std::ofstream ofs("boostObjectModelTest36");
    boost::archive::text_oarchive oa(ofs);
    oa << *anotherMap;
    ofs.close();
    std::map<XString*, Employee*> anotherMap1;

    std::ifstream ifs("boostObjectModelTest36");
    boost::archive::text_iarchive ia(ifs);
    ia >> anotherMap1;        

    if (!benchmarkMode) {
        for (auto & a : anotherMap1) {
            anotherMap1[a.first]->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
    // now, do a deep copy
    std::map<XString*, Employee *>* aFinalMap =
        new std::map<XString*, Employee*>();
    std::cout << "Copy.\n";
    for (auto& a : anotherMap1) {
        (*aFinalMap)[a.first] = a.second;
    }
    std::cout << "Deletion.\n";
    anotherMap1.clear();

    if (!benchmarkMode) {
        for (auto& a : *aFinalMap) {
            a.second->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }

    std::cout << "Deletion.\n";
    aFinalMap->clear();

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to write the objects to a file: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns."
              << std::endl;

}

#endif
