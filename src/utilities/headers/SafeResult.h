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
#ifndef PDB_UTILITIES_SAFERESULT_H
#define PDB_UTILITIES_SAFERESULT_H

#include <functional>
#include <memory>
#include <string>
#include <stdlib.h>

using std::function;
using std::string;
using std::shared_ptr;

namespace pdb {

template <typename P>
class SafeResult {

public:
    virtual void apply(function<void(P)> forSuccessCase,
                       function<void(string errorMsg)> forErrorCase) = 0;

    /**
     * If this safe result is in the success case, returns the result item.  Otherwise calls
     * exit(exitCode)
     * and terminates the process.
     */
    P getResultOrExit(int exitCode) {
        P* result;
        this->apply([&](P successResult) { result = &successResult; },

                    [&](string errorMessage) { exit(exitCode); });

        return *result;
    }
};

template <typename P>
class SafeResultSuccess : public SafeResult<P> {

public:
    SafeResultSuccess(P result) : _result(result) {}

    void apply(function<void(P)> forSuccessCase, function<void(string)> /*forFailureCase*/) {
        return forSuccessCase(_result);
    }

private:
    P _result;
};

template <typename P>
class SafeResultFailure : public SafeResult<P> {
public:
    SafeResultFailure(string errorMessage) {
        _errorMessage = errorMessage;
    }

    void apply(function<void(P)> /*forSuccessCase*/, function<void(string)> forFailureCase) {
        return forFailureCase(_errorMessage);
    }

private:
    string _errorMessage;
};
}

#endif  // ALLUNITTESTS_SAFERESULT_H
