
#ifndef WRITE_USER_SET_H
#define WRITE_USER_SET_H

// PRELOAD %WriteUserSet <Nothing>%

#include "WriteUserSetBase.h"

namespace pdb {

/**
 * This class encapsulates a computation that write objects of OutputClass type to a userset defined by setName and dbName.
 * @tparam OutputClass
 */
template <class OutputClass>
class WriteUserSet : public WriteUserSetBase<OutputClass> {
public:
  /**
   * This constructor is for constructing builtin object
   */
  WriteUserSet () = default;

  /**
   * User should only use following constructor
   * @param dbName
   * @param setName
   */
  WriteUserSet (std :: string dbName, std :: string setName) {
    this->dbName = dbName;
    this->setName = setName;
    this->outputType = getTypeName<OutputClass>();
  }

};
}

#endif
