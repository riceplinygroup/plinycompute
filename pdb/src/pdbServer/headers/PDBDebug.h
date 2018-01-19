#ifndef PDB_DEBUG_H
#define PDB_DEBUG_H

#include <ostream>
#include <iostream>

/*
 * A class used to disable std::cout and output nothing
 * http://stackoverflow.com/questions/1389538/cancelling-stdcout-code-lines-using-preprocessor
 */

class NullStream {
public:
    NullStream() {}
    NullStream& operator<<(std::ostream& (*pf)(std::ostream&)) {
        return *this;
    }
    template <typename T>
    NullStream& operator<<(T const&) {
        return *this;
    }
    template <typename R, typename P>
    NullStream& operator<<(R& (*pf)(P&)) {
        return *this;
    }
};

#ifdef PDB_DEBUG
#define PDB_COUT std::cout
#else
#define PDB_COUT NullStream()
#endif


#endif
