/*****************************************************************************
 *                                                                           *
 *  Copyright 2018 Rice University                                           *
 *                                                                           *
 *  Licensed under the Apache License, Version 2.0 (the "License");          *
 *  you may not use this file except in compliance with the License.         *
 *  You may obtain a copy of the License at                                  *
 *                                                                           *
 *      http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                           *
 *  Unless required by applicable law or agreed to in writing, software      *
 *  distributed under the License is distributed on an "AS IS" BASIS,        *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *  See the License for the specific language governing permissions and      *
 *  limitations under the License.                                           *
 *                                                                           *
 *****************************************************************************/

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <ctime>
#include <chrono>
#include <cstring>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/list.hpp>
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

    XString() {}
};


class Employee {
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version) {
        ar & name;
        ar & age;
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
    }
};


class Supervisor {

private:
    friend class boost::serialization::access;
    Employee* me;
    std::vector<Employee*> myGuys;
    template <class Archive>
    void serialize(Archive & ar, const unsigned int version) {
       ar & me;
       ar & myGuys;
    }
public:
    ~Supervisor() {
        delete me;
        for (auto a : myGuys) {
            delete a;
        }
    }

    Supervisor() {}

    Supervisor(std::string name, int age) {
        me = new Employee(name, age);
    }

    void addEmp(Employee* addMe) {
        myGuys.push_back(addMe);
    }

    Employee * getEmp(int i) {
        return myGuys[i];
    }

};


int main(int argc, char* argv[]) {



    // for timing
    auto begin = std::chrono::high_resolution_clock::now();

    std::vector<Supervisor*> supers;      
    std::ifstream ifs("boostObjectModelTest1");
    boost::archive::text_iarchive ia(ifs);
    ia >> supers;

    int numSupers = supers.size();
    std::vector<Employee *> result(10);
    for (int i = 0; i < numSupers; i++) {
       result.push_back(supers[i]->getEmp(i%10));
    }

    auto end = std::chrono::high_resolution_clock::now();

    std::cout << "Duration to read all of the objects: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns."
              << std::endl;

    std::ofstream ofs("boostObjectModelTest2");
    boost::archive::text_oarchive oa(ofs);
    oa << result;

    auto end1 = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to write all of the objects to file: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end1 - end).count() << " ns."
              << std::endl;
    for (auto v : supers) {
        delete v;
    }

}
