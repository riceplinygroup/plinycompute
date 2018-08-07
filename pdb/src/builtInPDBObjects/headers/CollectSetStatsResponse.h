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

#ifndef COLLECT_SET_STATS_RESPONSE_H
#define COLLECT_SET_STATS_RESPONSE_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"
#include "PDBVector.h"
#include "SetIdentifier.h"

// PRELOAD %CollectSetStatsResponse%

namespace pdb {

// encapsulates a request to return all user set information
class CollectSetStatsResponse : public Object {

 public:
  CollectSetStatsResponse() {}
  ~CollectSetStatsResponse() {}

  Handle<SetIdentifier> &getStats() {
    return stats;
  }

  const String &getError() const {
    return error;
  }
  bool isSuccess() const {
    return success;
  }

  void setStats(Handle<SetIdentifier> stats) {
    this->stats = stats;
  }

  void setError(const String &error) {
    CollectSetStatsResponse::error = error;
  }

  void setSuccess(bool success) {
    CollectSetStatsResponse::success = success;
  }

  ENABLE_DEEP_COPY

 private:

  /**
   * The stats of the set
   */
  Handle<SetIdentifier> stats;

  /**
   * The error if any
   */
  String error;

  /**
   * Did we succeed in collecting the stats
   */
  bool success;
};
}

#endif
