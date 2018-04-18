
#ifndef SET_H
#define SET_H

#include <vector>
#include <functional>
#include <iostream>
#include <memory>
#include "Query.h"

// PRELOAD %Set <Nothing>%

namespace pdb {

// this corresponds to a database set
template <typename Out>
class Set : public Query<Out> {

public:
    ENABLE_DEEP_COPY

    Set() {}
    ~Set() {}

    Set(bool isError) {
        this->setError();
    }

    Set(std::string dbName, std::string setName) {
        this->setDBName(dbName);
        this->setSetName(setName);
    }

    void match(function<void(QueryBase&)> forSelection,
               function<void(QueryBase&)> forSet,
               function<void(QueryBase&)> forQueryOutput) override {
        forSet(*this);
    };

    // gets the number of inputs
    virtual int getNumInputs() override {
        return 0;
    }

    // gets the name of the i^th input type...
    virtual std::string getIthInputType(int i) override {
        return "I have no inputs!!";
    }

    virtual std::string getQueryType() override {
        return "set";
    }
};
}

#endif
