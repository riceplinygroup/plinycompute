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
 * File:   DataTypes.h
 * Author: Jia
 *
 * Created on September 27, 2015, 1:23 PM
 */

#ifndef PDB_DATA_TYPES_H
#define PDB_DATA_TYPES_H

#include <cstddef>

typedef unsigned int PageIteratorsID;
typedef unsigned int UserTypeID;
typedef unsigned int PageID;
typedef unsigned int NodeID;
typedef unsigned int HashPartitionID;
typedef unsigned int DatabaseID;
typedef unsigned int ServerID;
typedef unsigned int SetID;
typedef unsigned int FilePartitionID;
typedef size_t MemPoolOffset;
typedef size_t PageOffset;
typedef unsigned long long ticks_t;
typedef unsigned int OperatorID;
typedef unsigned int JobStageID;

// Priority level for keeping in memory
// from low to high
typedef enum {
    TransientLifetimeEnded,
    PersistentLifetimeEnded,
    PersistentLifetimeNotEnded,
    TransientLifetimeNotEndedPartialData,
    TransientLifetimeNotEndedShuffleData,
    TransientLifetimeNotEndedHashData
} PriorityLevel;

typedef enum { JobData, ShuffleData, HashPartitionData, PartialAggregationData } LocalityType;

typedef enum { LRU, MRU, Random } LocalitySetReplacementPolicy;

typedef enum { UnifiedLRU, UnifiedMRU, UnifiedIntelligent } CacheStrategy;


typedef enum { Read, RepeatedRead, Write } OperationType;

typedef enum { TryCache, CacheThrough } DurabilityType;

typedef enum {
    StraightSequential,
    LoopingSequential,
    SmallSequential

} AccessPattern;

typedef enum { Transient, Persistent } PersistenceType;

typedef enum { Direct, Recreation, DeepCopy } ObjectCreationMode;

typedef enum { SequenceFileType, PartitionedFileType } FileType;

typedef enum { PeriodicTimer, OneshotTimer } TimerType;

typedef enum {
    UserSetType,
    SharedHashSetType,  // the input for first phase scan join, or the input for second phase
                        // probing of shuffle join
    PartitionedHashSetType,  // the input for second phase of shuffle join and aggregation
} SetType;


typedef struct {
    DatabaseID dbId;
    UserTypeID typeId;
    SetID setId;
    PageID pageId;
} CacheKey;

typedef struct {
    bool inCache;
    FilePartitionID partitionId;
    unsigned int pageSeqInPartition;
} FileSearchKey;

typedef struct {
    FilePartitionID partitionId;
    unsigned int pageSeqInPartition;
} PageIndex;

typedef struct {
    DatabaseID dbId;
    UserTypeID typeId;
    SetID setId;
} SetKey;

#endif
