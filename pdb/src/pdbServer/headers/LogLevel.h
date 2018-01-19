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

/*
 * LogLevel.h
 *
 *  Created on: Jul 1, 2016
 *      Author: kia
 */


// Following the same concept of logging as log4j

// OFF	The highest possible rank and is intended to turn off logging.
// FATAL	Severe errors that cause premature termination.
// ERROR	Other runtime errors or unexpected conditions.
// WARN	    Use of deprecated APIs, poor use of API, 'almost' errors.
// INFO	    Interesting runtime events (startup/shutdown).
// DEBUG	Detailed information on the flow through the system.
// TRACE	Most detailed information.

#ifndef SRC_UTILITIES_HEADERS_LOGLEVEL_H_
#define SRC_UTILITIES_HEADERS_LOGLEVEL_H_

enum LogLevel { OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE };

#endif /* SRC_UTILITIES_HEADERS_LOGLEVEL_H_ */
