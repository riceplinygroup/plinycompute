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
#include <fstream>
#include <vector>
#include <chrono>
#include <algorithm>
#include <iterator>
#include <ctime>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

Allocator allocator;

template <class DataType>
class LinkedList {

private:
    class Node {

    public:
        DataType data;
        Node* next;
        ~Node() {
            if (next != nullptr)
                delete next;
        }
        Node() {
            next = nullptr;
        }
    };

    Node* first;

public:
    LinkedList() {
        first = nullptr;
    }

    ~LinkedList() {
        if (first != nullptr)
            delete first;
    }

    void insertAtHead(DataType& addMe) {
        Node* newHead = new Node();
        newHead->next = first;
        newHead->data = addMe;
        first = newHead;
    }

    void remove(int whichOne) {

        // see if we are to remove the first one
        if (whichOne == 0) {

            Node* killMe = first;
            first = first->next;
            killMe->next = nullptr;
            delete killMe;
            return;
        }

        // find the guy to remove
        Node* cur = first;
        for (int i = 0; i < whichOne - 1; i++) {
            cur = cur->next;
        }

        Node* killMe = cur->next;
        cur->next = cur->next->next;
        killMe->next = nullptr;
        delete killMe;
        return;
    }

    DataType& operator[](int whichOne) {

        // find the guy we want
        Node* cur = first;
        for (int i = 0; i < whichOne; i++) {
            cur = cur->next;
        }

        // and return him
        return cur->data;
    }
};

class Employee {
    std::string* name;
    int age;

public:
    ~Employee() {}
    Employee() {}

    void print() {
        std::cout << "name is: " << *name << " age is: " << age << "\n";
    }

    Employee(std::string nameIn, int ageIn) {
        name = new std::string(nameIn);
        age = ageIn;
    }
};

class Supervisor {

private:
    Employee* me;
    std::vector<Employee*> myGuys;

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

    Employee* getEmp(int who) {
        return myGuys[who];
    }

    void print() {
        me->print();
        std::cout << "Plus have " << myGuys.size() << " employees.\n";
    }
};

int main() {

    // for timing
    auto begin = std::chrono::high_resolution_clock::now();

    // load up the allocator with RAM
    makeObjectAllocatorBlock(1024 * 1024 * 24, false);

    LinkedList<Supervisor*>* supers = new LinkedList<Supervisor*>();
    int i = 0;
    try {
        // put a lot of copies of it into a vector
        for (; true; i++) {

            // std :: cout << i << "\n";
            Supervisor* tempSup = new Supervisor("Joe Johnson", 20 + (i % 29));
            supers->insertAtHead(tempSup);
            for (int j = 0; j < 10; j++) {
                Employee* temp = new Employee("Steve Stevens", 20 + ((i + j) % 29));
                (*supers)[0]->addEmp(temp);
            }
            if (i > 14370) {
                break;
            }
        }

    } catch (NotEnoughSpace& e) {
        std::cout << "Finally, after " << i << " inserts, ran out of RAM for the objects.\n";
    }

    // print the first ten items
    std::cout << "The first 10 items are: \n";
    for (int i = 0; i < 10; i++) {
        (*supers)[i]->print();
    }

    std::cout << "\nRemoving every other one of the first 5 items.\n";
    for (int i = 0; i < 5; i++) {
        supers->remove(i);
    }

    // print the first ten items
    std::cout << "\nNow first 5 items are: \n";
    for (int i = 0; i < 5; i++) {
        (*supers)[i]->print();
    }

    // get every thousandth item
    std::cout << "\nPrinting every 1000th item: \n";
    for (int i = 0; i < 14001; i += 1000) {
        (*supers)[i]->print();
    }

    // remove every thousandth item
    std::cout << "\nRemoving every 1000th item.\n";
    for (int i = 0; i < 14001; i += 1000) {
        supers->remove(i);
    }

    delete supers;

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to create all of the objects and do the manipulation: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns."
              << std::endl;
}
