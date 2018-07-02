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

#ifndef TESTINGCLASSSTUFF_SYMBOLREADER_H
#define TESTINGCLASSSTUFF_SYMBOLREADER_H

#include <libelf.h>
#include <gelf.h>
#include <string>
#include <libdwarf/dwarf.h>
#include <libdwarf/libdwarf.h>
#include <vector>
#include <memory>
#include <functional>

namespace pdb {

struct AttributeType {

  AttributeType() = default;

  /**
   * Constructor with parameters
   * @param name - the name of the type
   * @param size - the size of the type in bytes
   */
  AttributeType(const std::string &name, size_t size) : name(name), size(size) {}

  /**
   * The name of the type
   */
  std::string name;

  /**
   * The size of the type
   */
  size_t size = 0;

};

struct AttributeInfo {

  /**
   * The name of the attribute
   */
  std::string name;

  /**
   * The size of the attribute
   */
  size_t size;

  /**
   * The offset of the attribute
   */
  size_t offset;

  /**
   * The type of the attribute
   */
  std::string type;
};

struct MethodInfo {

  /**
   * The name of the method
   */
  std::string name;

  /**
   * The symbol of the method
   */
  std::string symbol;

  /**
   * The return type of the method
   */
  AttributeType returnType;


  /**
   * The parameters of the method
   */
  std::vector<AttributeType> parameters;

};

struct ClassInfo {

  /**
   * Define the default constructor
   */
  ClassInfo() {
    attributes = std::make_shared<std::vector<AttributeInfo>>();
    methods = std::make_shared<std::vector<MethodInfo>>();
  }

  /**
   * The class name with the full namespace specification
   */
  std::string className;

  /**
   * The attributes of the class
   */
  std::shared_ptr<std::vector<AttributeInfo>> attributes;

  /**
   * The methods of the class
   */
  std::shared_ptr<std::vector<MethodInfo>> methods;

};

class SymbolReader {

 public:

  /**
   * Initialize the SymbolReader
   */
  explicit SymbolReader();

  /**
   * Destroy the SymbolReader
   */
  virtual ~SymbolReader();

  /**
   * Loads the file returns false if it fails or true if it can be loaded
   * @return true if it succeed false otherwise
   */
  bool load(std::string fileName);

  /**
   *
   * @param typeInfo
   * @return
   */
  ClassInfo getClassInformation(const std::type_info &typeInfo);

  /**
   *
   * @param typeSpec
   * @return
   */
  ClassInfo getClassInformation(const std::string &typeSpec);

 private:

  /**
   * Is this loaded
   */
  bool isLoaded;

  /**
   * The name of file we are opening
   */
  std::string fileName;

  /**
   * The file table index of the file we need to load
   */
  int fd = -1;

  /**
   * The dwarf resource
   */
  int dwarfRes = DW_DLV_ERROR;

  /**
   * Lib dwarf variables
   */
  Dwarf_Debug dbg = nullptr;
  Dwarf_Error error;
  Dwarf_Handler errhand = nullptr;
  Dwarf_Ptr errarg = nullptr;

  bool realTypeName(const std::type_info& typeInfo, std::string &typeName);

  ClassInfo analyzeFile(std::vector<std::string> &hierarchy,
                        std::function<void(Dwarf_Debug,
                                           Dwarf_Die,
                                           int,
                                           std::vector<std::string> &,
                                           ClassInfo &)> cuProcessor);

  void searchForHierarchyInCu(Dwarf_Debug dbg,
                              Dwarf_Die in_die,
                              int in_level,
                              std::vector<std::string> &hierarchy,
                              ClassInfo &ret);

  bool searchForTypeInCu(Dwarf_Debug dbg,
                         Dwarf_Die in_die,
                         int in_level,
                         std::vector<std::string> &hierarchy,
                         Dwarf_Die nameDie);

  /**
   *
   * @param dbg
   * @param print_me
   * @return
   */
  std::string getNamespaceOrClassName(Dwarf_Debug dbg, Dwarf_Die print_me);

  /**
   *
   * @param dbg
   * @param print_me
   * @param realName
   * @return
   */
  bool isClassSymbol(Dwarf_Debug dbg, Dwarf_Die print_me, std::string &realName);

  /**
   *
   * @param dbg
   * @param print_me
   * @param realName
   * @return
   */
  bool isNamespaceOrClass(Dwarf_Debug dbg, Dwarf_Die print_me, std::string &realName);

  /**
   *
   * @param print_me
   * @return
   */
  bool isAttributeSymbol(Dwarf_Die print_me);

  /**
   *
   * @param print_me
   * @return
   */
  bool isMethodSymbol(Dwarf_Die print_me);

  /**
   *
   * @param cur_die
   * @param ret
   */
  void parseAttribute(Dwarf_Die cur_die, ClassInfo &ret);

  /**
   * Returns the type for a debugging entry
   * @param curDie - the debugging entry of the type
   * @param nameDie - the debugging entry of the type that was referring to this one
   * @param previousName - the name of one of the types that was referring to this one
   * @param isPointer - the number of times a type that referred to this one is a pointer
   * @return the parsed type
   */
  AttributeType getType(Dwarf_Die curDie, Dwarf_Die nameDie, std::string &previousName, unsigned int isPointer);

  /**
   * Parses the class info from the debugging entry
   * @param in_die - the debug entry for this class
   * @param hierarchy - the hierarchy ex. namespace1::namespace2::className
   * @param ret - the output info of the class
   */
  void extractClassInfo(Dwarf_Die in_die, std::vector<std::string> &hierarchy, ClassInfo &ret);

  /**
   * Parses the method from the debugging entry
   * @param curDie - the debugging entry
   * @param info - the class info of the method the class belongs to
   */
  void parseMethod(Dwarf_Die curDie, ClassInfo &info);

  /**
   * For a given die it returns the full qualified name ex. namespace1::namespace2::class
   * //TODO this does not work with private types only with public types
   * @param typeDie - the type die
   * @param outTypeName - the output type name
   * @return true if we got a type false if we failed to have one
   */
  bool getFullTypeName(Dwarf_Die typeDie, std::string &outTypeName);
};

}



#endif //TESTINGCLASSSTUFF_SYMBOLREADER_H
