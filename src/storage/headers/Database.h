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
 * File:   Database.h
 * Author: Jia
 *
 * Created on September 27, 2015, 12:57 PM
 */

#ifndef DATABASE_H
#define	DATABASE_H

#include "PDBObject.h"
#include "UserType.h"
#include "PDBPage.h"
#include "DataTypes.h"

#include <vector>
using namespace std;

/**
 * This class wraps the interface for implementing a database.
 * A database contains one or more types, and a type contains one or more sets.
 */
class DatabaseInterface {
public:
	/*
	 * To support polymorphism.
	 */
	virtual ~DatabaseInterface() {}

	/**
	 * Add a new type to database
	 */
    virtual void addType(TypePtr type) = 0;

    /**
     * Remove a type from database, all directories and files
     * associated with the type will be deleted.
     */
    virtual bool removeType(UserTypeID typeID) = 0;

    /**
     * Dump all in-memory data from the database.
     */
    virtual void dump() = 0;

    /**
     * Return the DatabaseID of this database.
     */
    virtual DatabaseID getDatabaseID() = 0;

    /**
     * Return the name of this database.
     */
    virtual string getDatabaseName() = 0;

    /**
     * Add a new object to the database.
     * The object contains fields that specifies the type and set.
     * PageId and minipageId will be automatically assigned.
     * Return true if successful, return false
     * if typeId or setId specified in the object doesn't exists.
     */
    virtual bool addObject(PDBObjectPtr object, PageID &pageId, MiniPageID &miniPageId) = 0;

    /**
     * Get an object from the database, miniPageID must be specified instead of offset.
     */
    virtual PDBObjectPtr getObject(UserTypeID typeID, SetID setID, PageID pageID, MiniPageID miniPageID) = 0;

    /**
     * Get an object from the database, offset must be specified instead of miniPageID.
     */
    virtual PDBObjectPtr getObjectByOffset(UserTypeID typeID, SetID setID, PageID pageID, size_t offset) = 0;

    /**
     * Get a page from the database.
     */
    virtual PDBPagePtr getPage(UserTypeID typeID, SetID setID, PageID pageID) = 0;

    /**
     * Get total number of pages in a specified Set.
     */
    virtual int getNumPages(UserTypeID typeId, SetID setId) = 0;

    /**
     * Get a type from the database.
     */
    virtual TypePtr getType(UserTypeID typeId) = 0;

    /**
     * Flush data in input buffer into disk files.
     */
    virtual void flush () = 0;
};



#endif	/* DATABASE_H */

