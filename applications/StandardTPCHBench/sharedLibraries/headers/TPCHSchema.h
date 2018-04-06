#ifndef TPCH_SCHEMA_H
#define TPCH_SCHEMA_H


//this file describes the schema of TPCH benchmark


using namespace pdb;

namespace tpch {

class TPCHCustomer : public Object {

public:

   ENABLE_DEEP_COPY 

   int c_custkey;
   String c_name;
   String c_address;
   int c_nationkey;
   String c_phone;
   double c_acctbal;
   String c_mktsegment;
   String c_comment;

   TPCHCustomer () {}

   TPCHCustomer (int c_custkey, std::string c_name, std::string c_address, int c_nationkey,
       std::string c_phone, double c_acctbal, std::string c_mktsegment, std::string c_comment) {

      this->c_custkey = c_custkey;
      this->c_name = c_name;
      this->c_address = c_address;
      this->c_nationkey = c_nationkey;
      this->c_phone = c_phone;
      this->c_acctbal = c_acctbal;
      this->c_mktsegment = c_mktsegment;
      this->c_comment = c_comment;

   }
    

};


class TPCHLineItem : public Object {

public:

   ENABLE_DEEP_COPY

   int l_orderkey;
   int l_partkey;
   int l_suppkey;
   int l_linenumber;
   double l_quantity;
   double l_extendedprice;
   double l_discount;
   double l_tax;
   String l_returnflag;
   String l_linestatus;
   String l_shipdate;
   String l_commitdate;
   String l_receiptdate;
   String l_shipinstruct;
   String l_shipmode;
   String l_comment;

   TPCHLineItem () {}

   TPCHLineItem (int l_orderkey, int l_partkey, int l_suppkey, int l_linenumber,
       double l_quantity, double l_extendedprice, double l_discount, double l_tax,
       std::string l_returnflag, std::string l_linestatus, std::string l_shipdate, std::string l_commitdate,
       std::string l_receiptdate, std::string l_shipinstruct, std::string l_shipmode, std::string l_comment) {
        this->l_orderkey = l_orderkey;
        this->l_partkey = l_partkey;
        this->l_suppkey = l_suppkey;
        this->l_linenumber = l_linenumber;
        this->l_quantity = l_quantity;
        this->l_extendedprice = l_extendedprice;
        this->l_discount = l_discount;
        this->l_tax = l_tax;
        this->l_returnflag = l_returnflag;
        this->l_linestatus = l_linestatus;
        this->l_shipdate = l_shipdate;
        this->l_commitdate = l_commitdate;
        this->l_receiptdate = l_receiptdate;
        this->l_shipinstruct = l_shipinstruct;
        this->l_shipmode = l_shipmode;
        this->l_comment = l_comment;
   }

};


class TPCHNation : public Object {

public:

   ENABLE_DEEP_COPY

   int n_nationkey;
   String n_name;
   int n_regionkey;
   String n_comment;

   TPCHNation() {}

   TPCHNation(int n_nationkey, std::string n_name, int n_regionkey, std::string n_comment) {

      this->n_nationkey = n_nationkey;
      this->n_name = n_name;
      this->n_regionkey = n_regionkey;
      this->n_comment = n_comment;

   }

};


class TPCHOrder : public Object {

public:

   ENABLE_DEEP_COPY

   int o_orderkey;
   int o_custkey;
   String o_orderstatus;
   double o_totalprice;
   String o_orderdate;
   String o_orderpriority;
   String o_clerk;
   int o_shippriority;
   String o_comment;

   TPCHOrder () {}

   TPCHOrder(int o_orderkey, int o_custkey, std::string o_orderstatus, double o_totalprice, 
      std::string o_orderdate, std::string o_orderpriority, std::string o_clerk, 
      int o_shippriority, std::string o_comment) {

      this->o_orderkey = o_orderkey;
      this->o_custkey = o_custkey;
      this->o_orderstatus = o_orderstatus;
      this->o_totalprice = o_totalprice;
      this->o_orderdate = o_orderdate;
      this->o_orderpriority = o_orderpriority;
      this->o_clerk = o_clerk;
      this->o_shippriority = o_shippriority;
      this->o_comment = o_comment;

   }


};


class TPCHPart : public Object {

public:

   ENABLE_DEEP_COPY

   int p_partkey;
   String p_name;
   String p_mfgr;
   String p_brand;
   String p_type;
   int p_size;
   String p_container;
   double p_retailprice;
   String p_comment;

   TPCHPart () {}

   TPCHPart (int p_partkey, std::string p_name, std::string p_mfgr, std::string p_brand,
      std::string p_type, int p_size, std::string p_container, double p_retailprice,
      std::string p_comment) {

       this->p_partkey = p_partkey;
       this->p_name = p_name;
       this->p_mfgr = p_mfgr;
       this->p_brand = p_brand;
       this->p_type = p_type;
       this->p_size = p_size;
       this->p_container = p_container;
       this->p_retailprice = p_retailprice;
       this->p_comment = p_comment;
   }

};

class TPCHTPCHPartSupp : public Object {

public:

   ENABLE_DEEP_COPY

   int ps_partkey;
   int ps_suppkey;
   int ps_availqty;
   double ps_supplycost;
   String ps_comment;

   TPCHTPCHPartSupp () {}

   TPCHTPCHPartSupp (int ps_partkey, int ps_suppkey, int ps_availqty, double ps_supplycost,
      std::string ps_comment) {

      this->ps_partkey = ps_partkey;
      this->ps_suppkey = ps_suppkey;
      this->ps_availqty = ps_availqty;
      this->ps_supplycost = ps_supplycost;
      this->ps_comment = ps_comment;

   }


};



class TPCHRegion : public Object {

public:

   ENABLE_DEEP_COPY

   int r_regionkey;
   String r_name;
   String r_comment;

   TPCHRegion () {}

   TPCHRegion (int r_regionkey, std::string r_name, std::string r_comment) {
       this->r_regionkey = r_regionkey;
       this->r_name = r_name;
       this->r_comment = r_comment;
   }

};


class TPCHSupplier : public Object {

public:

   ENABLE_DEEP_COPY

   int s_suppkey;
   String s_name;
   String s_address;
   int s_nationkey;
   String s_phone;
   double s_acctbal;
   String s_comment;

   TPCHSupplier () {}

   TPCHSupplier (int s_suppkey, std::string s_name, std::string s_address, int s_nationkey,
      std::string s_phone, double s_acctbal, std::string s_comment) {
       this->s_suppkey = s_suppkey;
       this->s_name = s_name;
       this->s_address = s_address;
       this->s_nationkey = s_nationkey;
       this->s_phone = s_phone;
       this->s_acctbal = s_acctbal;
       this->s_comment = s_comment;
   }

};

}




#endif
