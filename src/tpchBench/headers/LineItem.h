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
#ifndef LINEITEM_H
#define LINEITEM_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"

#include "Supplier.h"
#include "Part.h"


// This class represents a LineItem Object according to the TPC-H Database Benchmark


// CREATE TABLE [dbo].[LINEITEM](
// [L_ORDERKEY] [int] NOT NULL,
// [L_PARTKEY] [int] NOT NULL,
// [L_SUPPKEY] [int] NOT NULL,
// [L_LINENUMBER] [int] NOT NULL,
// [L_QUANTITY] [decimal](15, 2) NOT NULL,
// [L_EXTENDEDPRICE] [decimal](15, 2) NOT NULL,
// [L_DISCOUNT] [decimal](15, 2) NOT NULL,
// [L_TAX] [decimal](15, 2) NOT NULL,
// [L_RETURNFLAG] [char](1) NOT NULL,
// [L_LINESTATUS] [char](1) NOT NULL,
// [L_SHIPDATE] [date] NOT NULL,
// [L_COMMITDATE] [date] NOT NULL,
// [L_RECEIPTDATE] [date] NOT NULL,
// [L_SHIPINSTRUCT] [char](25) NOT NULL,
// [L_SHIPMODE] [char](10) NOT NULL,
// [L_COMMENT] [varchar](44) NOT NULL
// );

class LineItem: public pdb::Object {
public:

	pdb::Handle<pdb::String> name;
	int orderKey;

	pdb::Handle<Supplier> supplier;
	pdb::Handle<Part> part;

	int lineNumber;
	double quantity;
	double extendedPrice;
	double discount;
	double tax;
	pdb::Handle<pdb::String> returnFlag;
	pdb::Handle<pdb::String> lineStatus;

	pdb::Handle<pdb::String> shipDate;
	pdb::Handle<pdb::String> commitDate;
	pdb::Handle<pdb::String> receiptDate;

	pdb::Handle<pdb::String> shipinStruct;
	pdb::Handle<pdb::String> shipMode;
	pdb::Handle<pdb::String> comment;



	ENABLE_DEEP_COPY

	LineItem() {
	}

	~LineItem() {
	}

	//Constructor with arguments:
	LineItem(std::string name, int orderKey,
			 pdb::Handle<Supplier> supplier, pdb::Handle<Part> part,
			 int lineNumber, double quantity, double extendedPrice,
			 double discount, double tax,
			 std::string returnFlag, std::string lineStatus,
			 std::string shipDate, std::string commitDate,
			 std::string receiptDate, std::string shipinStruct,
			 std::string shipMode, std::string comment) {
		this->name=pdb::makeObject<pdb::String>(name);
		this->orderKey=orderKey;
		this->supplier=supplier;
		this->part=part;
		this->lineNumber=lineNumber;
		this->quantity=quantity;
		this->extendedPrice=extendedPrice;
		this->discount=discount;
		this->tax=tax;
		this->returnFlag=pdb::makeObject<pdb::String>(returnFlag);
		this->lineStatus=pdb::makeObject<pdb::String>(lineStatus);
		this->shipDate=pdb::makeObject<pdb::String>(shipDate);
		this->commitDate=pdb::makeObject<pdb::String>(commitDate);
		this->receiptDate=pdb::makeObject<pdb::String>(receiptDate);
		this->shipinStruct=pdb::makeObject<pdb::String>(shipinStruct);
		this->shipMode=pdb::makeObject<pdb::String>(shipMode);
		this->comment=pdb::makeObject<pdb::String>(comment);
	}

	const pdb::Handle<pdb::String>& getComment() const {
		return comment;
	}

	void setComment(const pdb::Handle<pdb::String>& comment) {
		this->comment = comment;
	}

	const pdb::Handle<pdb::String>& getCommitDate() const {
		return commitDate;
	}

	void setCommitDate(const pdb::Handle<pdb::String>& commitDate) {
		this->commitDate = commitDate;
	}

	double getDiscount() const {
		return discount;
	}

	void setDiscount(double discount) {
		this->discount = discount;
	}

	double getExtendedPrice() const {
		return extendedPrice;
	}

	void setExtendedPrice(double extendedPrice) {
		this->extendedPrice = extendedPrice;
	}

	int getLineNumber() const {
		return lineNumber;
	}

	void setLineNumber(int lineNumber) {
		this->lineNumber = lineNumber;
	}

	const pdb::Handle<pdb::String>& getLineStatus() const {
		return lineStatus;
	}

	void setLineStatus(const pdb::Handle<pdb::String>& lineStatus) {
		this->lineStatus = lineStatus;
	}

	const pdb::Handle<pdb::String>& getName() const {
		return name;
	}

	void setName(const pdb::Handle<pdb::String>& name) {
		this->name = name;
	}

	int getOrderKey() const {
		return orderKey;
	}

	void setOrderKey(int orderKey) {
		this->orderKey = orderKey;
	}

	const pdb::Handle<Part>& getPart() const {
		return part;
	}

	void setPart(const pdb::Handle<Part>& part) {
		this->part = part;
	}

	double getQuantity() const {
		return quantity;
	}

	void setQuantity(double quantity) {
		this->quantity = quantity;
	}

	const pdb::Handle<pdb::String>& getReceiptDate() const {
		return receiptDate;
	}

	void setReceiptDate(const pdb::Handle<pdb::String>& receiptDate) {
		this->receiptDate = receiptDate;
	}

	const pdb::Handle<pdb::String>& getReturnFlag() const {
		return returnFlag;
	}

	void setReturnFlag(const pdb::Handle<pdb::String>& returnFlag) {
		this->returnFlag = returnFlag;
	}

	const pdb::Handle<pdb::String>& getShipDate() const {
		return shipDate;
	}

	void setShipDate(const pdb::Handle<pdb::String>& shipDate) {
		this->shipDate = shipDate;
	}

	const pdb::Handle<pdb::String>& getShipinStruct() const {
		return shipinStruct;
	}

	void setShipinStruct(const pdb::Handle<pdb::String>& shipinStruct) {
		this->shipinStruct = shipinStruct;
	}

	const pdb::Handle<pdb::String>& getShipMode() const {
		return shipMode;
	}

	void setShipMode(const pdb::Handle<pdb::String>& shipMode) {
		this->shipMode = shipMode;
	}

	const pdb::Handle<Supplier>& getSupplier() const {
		return supplier;
	}

	void setSupplier(const pdb::Handle<Supplier>& supplier) {
		this->supplier = supplier;
	}

	double getTax() const {
		return tax;
	}

	void setTax(double tax) {
		this->tax = tax;
	}
};
#endif
