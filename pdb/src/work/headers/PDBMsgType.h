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
 * File:   PDBMsgType.h
 * Author: Chris
 *
 * slightly modified by Jia to add more message types
 * Created on September 25, 2015, 5:15 PM
 */

#ifndef PDBMSGTYPE_H
#define PDBMSGTYPE_H


// this lists all of the identifiers for the different messages that can be received by a
// PDBCommunicator
// object.  See PDBCommunicator.h for more info.

/*
enum PDBMsgType_v1 {
        QueryMsg,
        ErrMsg,
        BuildFeaturesMsg,
        InfoMsg,
        InfoReceivedMsg,
        ShutDownMsg,
        StoreDataNewFileMsg,
        NoMsg,
        CloseConnection,
        LoadedCodeForPDBMetricMsg,
        LoadedCodeForPDBQueryExecMsg,
        LoadedCodeForPDBFeatureMsg,
        LoadedCodeForPDBStoredDataMsg,
        LoadedCodeForFeatureBuilderMsg,
        CodeForPDBMetricMsg,
        CodeForPDBQueryExecMsg,
        CodeForPDBFeatureMsg,
        CodeForPDBStoredDataMsg,
        CodeForFeatureBuilderMsg,
        StoreDataNewFileDoneMsg,
        LoadDataDoneMsg,
        ShutDownDoneMsg,
        LoadDataMsg,
        FeatureBuildTaskDoneMsg,
        QueryResultMsg

};
 */

enum PDBMsgType {
    // Connection operations
    CloseConnection,  // 0
    ErrMsg,           // 1
    NoMsg,            // 2
    InfoMsg,          // 3
    InfoReceivedMsg,  // 4
    // Server operations
    AbortMsg,         // 5
    ShutDownMsg,      // 6
    ShutDownDoneMsg,  // 7
    // Database operations
    /*
     * Needs to integrate with Manager/Catalog for adding meta-data
     */
    /*
     * MsgType: 4 bytes
     */
    ListDatabasesMsg,  // 8

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     * name length: 4 bytes
     * name: varying length
     */
    AddDatabaseMsg,  // 9

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * info: varying length
     */
    AddDatabaseSuccessMsg,  // 10

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     */
    RemoveDatabaseMsg,  // 11

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * info: varying length
     */
    RemoveDatabaseSuccessMsg,  // 12

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     */
    OpenDatabaseMsg,  // 13

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * info: varying length
     */
    OpenDatabaseSuccessMsg,  // 14

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     */
    CloseDatabaseMsg,  // 15

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * info: varying length
     */
    CloseDatabaseSuccessMsg,  // 16


    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     */
    ListTypesMsg,  // 17

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     * UserTypeID: 4 bytes
     * type name length: 4 bytes
     * type name: varying length
     */
    AddTypeMsg,  // 18

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * info: varying length
     */
    AddTypeSuccessMsg,  // 19

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     * UserTypeID: 4 bytes
     */
    RemoveTypeMsg,  // 20

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * info: varying length
     */
    RemoveTypeSuccessMsg,  // 21

    /*
     * Data Manipulations
     */
    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     * UserTypeID: 4 bytes
     */
    ListObjectsMsg,  // 22

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     * TypeID: 4 bytes
     * NumObjects: 4 bytes
     * 1st Object length: 4 bytes
     * 1st Object data: varying length
     * 2nd Object length: 4 bytes
     * 2nd Object data: varying length
     * ...
     */
    LoadDataMsg,  // 23

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * info: varying length
     */
    LoadDataDoneMsg,  // 24

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     * TypeID: 4 bytes
     * SetID: 4 bytes
     * DataLength: 4 bytes
     * Data: varying length
     */
    AddObjectMsg,  // 25

    /* MsgType: 4 bytes
     * PageID: 4 bytes
     * MiniPageID: 4 bytes
     */
    AddObjectSuccessMsg,  // 26

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * NodeID: 4 bytes
     * DatabaseID: 4 bytes
     * TypeID: 4 bytes
     * SetID: 4 bytes
     * PageID: 4 bytes
     * MiniPageID: 4 bytes
     */
    GetObjectMsg,  // 27

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * Object Length: 4 bytes
     * Object Data: varying length
     */
    SendObjectMsg,  // 28

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * NodeID: 4 bytes
     * DatabaseID: 4 bytes
     * TypeID: 4 bytes
     * PageID: 4 bytes
     */
    GetPageMsg,  // 29


    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * PageSize: 4 bytes
     * Data Offset: 4 bytes
     */
    GetPageSuccessMsg,  // 30

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * NodeID: 4 bytes
     * DatabaseID: 4 bytes
     * TypeID: 4 bytes
     * SetID: 4 bytes
     */
    GetSetPagesMsg,  // 31

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * NumPageIDs: 4 bytes
     * 1st PageID: 4 bytes
     * 2nd PageID: 4 bytes
     * ...
     */
    PageLoadedMsg,      // 32
    AckPageLoadedMsg,   // 33
    PageLoadedDoneMsg,  // 34

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * NumPageIDs: 4 bytes
     * 1st PageID: 4 bytes
     * 2nd PageID: 4 bytes
     * ...
     */
    PageFinishedBatchMsg,     // 35
    AckPageFinishedBatchMsg,  // 36
    PageFinishedDoneMsg,      // 37

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * info: varying length
     */
    NoMorePageMsg,  // 38

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * NodeID: 4 bytes
     * DatabaseID: 4 bytes
     * TypeID: 4 bytes
     */
    GetTypePagesMsg,  // 39


    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     * UserTypeID: 4 bytes
     * SetID: 4 bytes
     * set name length: 4 bytes
     * set name: varying length
     */
    AddSetMsg,  // 40

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * info: varying length
     */
    AddSetSuccessMsg,  // 41

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     * UserTypeID: 4 bytes
     * SetID: 4 bytes
     */
    RemoveSetMsg,  // 42

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * info: varying length
     */
    RemoveSetSuccessMsg,  // 43

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     * UserTypeID: 4 bytes
     * SetID: 4 bytes
     */
    GetPageNumMsg,  // 44

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * PageNum: 4 bytes
     */
    GetPageNumSuccessMsg,  // 45

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     * UserTypeID: 4 bytes
     * SetID: 4 bytes
     */
    TestSetIteratorsMsg,  // 46

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * info: varying length
     */
    TestSetIteratorsDoneMsg,  // 47

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * SetID: 4 bytes
     */
    AddTempSetSuccessMsg,  // 48

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * SetID: 4 bytes
     */
    AddTempPageMsg,  // 49

    // Catalog-related messages

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     * name length: 4 bytes
     * binaryCode: varying length
     */
    CatalogAddDatabaseMsg,  // 50

    /* MsgType: 4 bytes
     * text: 4 bytes
     */
    CatalogAddDatabaseSuccessMsg,  // 51

    /* MsgType: 4 bytes
     * Total length: 4 bytes
     * DatabaseID: 4 bytes
     * name length: 4 bytes
     * content: varying length
     */
    CatalogRegisterPDBTypeMsg,  // 52

    /* MsgType: 4 bytes
     * text: 4 bytes
     */
    CatalogRegisterPDBTypeSuccessMsg,  // 53


};


#endif /* PDBMSGTYPE_H */
