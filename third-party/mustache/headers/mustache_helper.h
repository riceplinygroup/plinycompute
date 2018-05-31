//
// Created by dimitrije on 3/12/18.
//

#ifndef PDB_MUSTACHE_HELPER_H
#define PDB_MUSTACHE_HELPER_H

#include <mustache.h>
#include <vector>
#include <map>

namespace mustache {

/**
 * Converts a std::vector to mustache data
 * @tparam T the type of the std vector
 * @param in the input vector
 * @return the resulting data
 */
template<typename T>
auto from_vector(const std::vector<T> &in) {

  // create the data for the input map
  data ret = data::type::list;
  for(int i = 0; i < in.size(); i++) {

    data valueData;

    // fill in the column data
    valueData.set("value", in[i]);
    valueData.set("isLast", i == in.size()-1);

    ret.push_back(valueData);
  }

  return ret;
}

/**
 * Converts a std::map to mustache data
 * @tparam K the key type
 * @tparam V the value type
 * @return the resulting data
 */
template<typename K, typename V>
auto from_map(const std::map<K, V> &in){

  // create the data for the input map
  data ret = data::type::list;

  // the counter to know how many we processed
  int counter = 0;

  // fill up the data
  for(auto i : in) {

    data valueData;

    // fill in the column data
    valueData.set("key", i.first);
    valueData.set("value", i.second);
    valueData.set("isLast", counter == in.size() - 1);

    // add the data
    ret.push_back(valueData);

    counter++;
  }

  return ret;
}

}

#endif //PDB_MUSTACHE_HELPER_H
