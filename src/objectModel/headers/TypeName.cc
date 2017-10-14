
#ifndef TYPE_NAME_CC
#define TYPE_NAME_CC

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <memory>
#include <cstring>
#ifndef _MSC_VER
#include <cxxabi.h>
#endif

namespace pdb {

// helper for getTypeName bleow... from
// http://stackoverflow.com/questions/25103885/how-to-demangle-stdstring-as-stdstring
inline void filter(std::string& r, const char* b) {
    const char* e = b;
    for (; *e; ++e)
        ;
    const char* pb = "std::__1::";
    const int pl = strlen(pb);
    const char* pe = pb + pl;
    while (true) {
        const char* x = std::search(b, e, pb, pe);
        r.append(b, x);
        if (x == e)
            break;
        r += "std::";
        b = x + pl;
    }
}

template <class T>
std::string getTypeName() {
    std::unique_ptr<char, void (*)(void*)> own(
#ifndef _MSC_VER
        __cxxabiv1::__cxa_demangle(typeid(T).name(), nullptr, nullptr, nullptr),
#else
        nullptr,
#endif
        std::free);

    std::string r;
    if (own) {
        filter(r, own.get());
    } else {
        r = typeid(T).name();
    }
    r.erase(remove_if(r.begin(), r.end(), ::isspace), r.end());
    return r;
}
}
#endif
