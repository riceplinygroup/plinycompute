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

#ifndef PDB_PROFILING_H
#define PDB_PROFILING_H

#include <chrono>

#ifdef PROFILING

/**
 * This macro is used to start profiling the code and it is supposed to be used within the same block as the PROFILER_END macro
 * @param id - the id used to track the profiler
 */
#define PROFILER_START(id) auto ___begin_##id = std::chrono::high_resolution_clock::now();

/**
 * This macro is used to end profiling the code and it is supposed to be used within the same block as the PROFILER_START macro
 * @param id - the id used to track the profiler
 */
#define PROFILER_END(id) std::cout << "Time Duration for " << #id << " : " << std::chrono::duration_cast<std::chrono::duration<float>>(std::chrono::high_resolution_clock::now() - ___begin_##id).count() << " seconds.\n";

/**
 * This macro is used to end profiling the code and it is supposed to be used within the same block as the PROFILER_START macro
 * This macro also outputs an additional message
 * @param id - the id used to track the profiler
 * @param message - the message we want to print out
 */
#define PROFILER_END_MESSAGE(id, message) std::cout << message << "\n"; PROFILER_END(id)

#else

/**
 * This macro is used to start profiling the code and it is supposed to be used within the same block as the PROFILER_END macro
 * @param the id used to track the profiler
 */
#define PROFILER_START(id)

/**
 * This macro is used to end profiling the code and it is supposed to be used within the same block as the PROFILER_START macro
 * @param the id used to track the profiler
 */
#define PROFILER_END(id)

/**
 * Profiler end
 */
#define PROFILER_END_MESSAGE(id, message)

#endif // PROFILING

#endif //PDB_PROFILING_H
