# New Document
***Acknowledgements***:

PlinyCompute's coding style for C
++ is customized from following coding standards:

(1) Peloton coding style: https://github.com/cmu-db/peloton/edit/master/script/coding_style.md

(2) Google C++ coding style: https://google.github.io/styleguide/cppguide.html

***Standards***


**VARIABLE AND FUNCTION NAMES**: Use descriptive variable and function names.

**INDENTATION** We use 2-column tabs for indentation. Don't put multiple assignments on a single line either. Avoid tricky expressions. Get a decent editor and don't leave whitespace at the end of lines.

**BREAKING LONG LINES** Coding style is all about readability and maintainability using commonly available tools. The limit on the length of lines is 80 columns and this is a strongly preferred limit. Statements longer than 80 columns will be broken into sensible chunks, unless exceeding 80 columns significantly increases readability and does not hide information. Descendants are always substantially shorter than the parent and are placed substantially to the right. The same applies to function headers with a long argument list. Make use of vertical spaces to separate different steps of a function.

**COMMENTS** Add comments for functions, function parameters, and logic inside the function. Please remember that code is often written once but read many times. Comments are good, but there is also a danger of over-commenting.  NEVER try to explain HOW your code works in a comment: it's much better to write the code so that the _working_ is obvious, and it's a waste of time to explain badly written code. Generally, you want your comments to tell WHAT your code does, not HOW. The style for comments is the C99-style "// ..." comments in the .cc files, and we use the JavaDoc style, which consist of a C-style comment block starting with two \*'s, like this:
```C++
/**
 * ... text ...
 * @param ... name and decription ...
 * @return ... description ...
 */
```

**FUNCTIONS** Functions should be short and sweet, and do just one thing.  They should fit on one or two screenfuls of text, and do one thing and do that well. The maximum length of a function is inversely proportional to the complexity and indentation level of that function.  So, if you have a conceptually simple function that is just one long (but simple) case-statement, where you have to do lots of small things for a lot of different cases, it's OK to have a longer function. However, if you have a complex function, and you suspect that a less-than-gifted first-year high-school student might not even understand what the function is all about, you should adhere to the maximum limits all the more closely.  Use helper functions with descriptive names (you can ask the compiler to in-line them if you think it's performance-critical, and it will probably do a better job of it than you would have done).

Another measure of the function is the number of local variables.  They shouldn't exceed 5-10, or you're doing something wrong.  Re-think the function, and split it into smaller pieces.  A human brain can generally easily keep track of about 7 different things, anything more and it gets confused.  You know you're brilliant, but maybe you'd like to understand what you did 2 weeks from now.

**TYPEDEFS** Please don't use things like "vps_t". It's a _mistake_ to use typedef for structures and pointers. When you see a	`vps_t a;` in the source, what does it mean? In contrast, if it says `struct virtual_container *a;` you can actually tell what "a" is.

**UNUSED CODE** Avoid commenting out unused code. Remove them.

**PRINTF** Refrain from using `printf` and `std::cout`. Instead use the logging macros, such as LOG_LEVEL_INFO, in `common/logger.h`.

**DON'T REINVENT THE MACROS** This codebase contains a number of macros that you should use, rather than explicitly coding some variant of them yourself.


**THE INLINE DISEASE** Don't use `inline` unless you know what you are doing.  Overuse use of `inline` can lead to larger binaries, can cause `I-cache` thrashing, and can negatively impact `D-cache` behaviour if the inlined function accesses large amounts of data and appears in a hot loop. The decision to inline is usually best left up to the compiler. The only reason you should use the `inline` function is to adhere to the `one-definition-rule`, i.e., if you want to have method definitions in the header file. For more details, see [here](https://isocpp.org/wiki/faq/inline-functions#where-to-put-inline-keyword)

**INLINE MEMBER FUNCTIONS**
While inline-defined member functions are convenient, they can obscure the publically visible interface of the class, thus making it difficult to use. By rule of thumb, member accessor definitions that are one line can appear within the class. All other definitions must exist outside the class body. Example:

```c++
class Foo {
 public:  
  // Inline definition okay
  void GetBar() const { return bar_; }
  
  // Externally definition
  void Bar2() const;
};

inline void Foo::Bar2() const {
  ... implentation ...
}
```

The example above illustrates a valid use of the `inline` keyword for the definition of `Foo::Bar2()`. For more details, see [here](https://isocpp.org/wiki/faq/inline-functions#where-to-put-inline-keyword)

**CONDITIONAL COMPILATION** Wherever possible, don't use preprocessor conditionals (#if, #ifdef) in .cpp files; doing so makes code harder to read and logic harder to follow.  Instead, use such conditionals in a header file defining functions for use in those .cpp files, providing no-op stub versions in the #else case, and then call those functions unconditionally from .cpp files.  The compiler will avoid generating any code for the stub calls, producing identical results, but the logic will remain easy to follow.

**FILE HEADERS** Add the default plinycompute file header if you create a new file.

**EDITOR MODELINES** Some editors can interpret configuration information embedded in source files, indicated with special markers.  For example, emacs interprets lines marked like this:	-*- mode: c -*-. Do NOT include any of these in source files. People have their own personal editor configurations, and your source files should not override them.

**ALLOCATING MEMORY** Always use smart pointers, such as `std::shared_ptr` whenever possible, to simplify memory management.

**PRINTING MESSAGES** Developers like to be seen as literate. Do mind the spelling of messages to make a good impression. Do not use crippled words like "dont"; use "do not" or "don't" instead.  Make the messages concise, clear, and unambiguous. Use appropriate log levels, such as TRACE, in `PDBLogger.h`. Coming up with good debugging messages can be quite a challenge; and once you have them, they can be a huge help for troubleshooting. Do not use `std::cout` statements on server-side code, instead use the logging facilities or `PDB_COUT` for debugging messages.

**MACROS, ENUMS** Names of macros defining constants and labels in enums are capitalized. `#define CONSTANT 0x12345`. Enums are preferred when defining several related constants. CAPITALIZED macro names are appreciated but macros resembling functions may be named in lower case. Generally, inline functions are preferable to macros resembling functions.

**DATA STRUCTURES** Add new reusable data structures in `utility` directory, so that they can be reused across the system.

**CONSTRUCTORS** Constructors should never call virtual functions. If appropriate for your code , terminating the program may be an appropriate error handling response. Otherwise, consider a factory function or Init() method. Avoid Init() methods on objects with no other states that affect which public methods may be called (semi-constructed objects of this form are particularly hard to work with correctly).

**INHERITANCE** All inheritance should be public. If you want to do private inheritance, you should be including an instance of the base class as a member instead.
Do not overuse implementation inheritance. Composition is often more appropriate. Try to restrict use of inheritance to the "is-a" case: Barsubclasses Foo if it can reasonably be said that Bar "is a kind of" Foo.

Make your destructor virtual if necessary. If your class has virtual methods, its destructor should be virtual.

Limit the use of protected to those member functions that might need to be accessed from subclasses.

**sizeof** Prefer sizeof(varname) to sizeof(type).

Use sizeof(varname) when you take the size of a particular variable. sizeof(varname) will update appropriately if someone changes the variable type either now or later. You may use sizeof(type) for code unrelated to any particular variable, such as code that manages an external or internal data format where a variable of an appropriate C++ type is not convenient.

**DEPRECATED INTERFACE** Mark deprecated interface points with DEPRECATED comments.

You can mark an interface as deprecated by writing a comment containing the word DEPRECATED in all caps. The comment goes either before the declaration of the interface or on the same line as the declaration.

After the word DEPRECATED, write your name, e-mail address, or other identifier in parentheses.

A deprecation comment must include simple, clear directions for people to fix their callsites. In C++, you can implement a deprecated function as an inline function that calls the new interface point.

Marking an interface point DEPRECATED will not magically cause any callsites to change. If you want people to actually stop using the deprecated facility, you will have to fix the callsites yourself or recruit a crew to help you.

New code should not contain calls to deprecated interface points. Use the new interface point instead. If you cannot understand the directions, find the person who created the deprecation and ask them for help using the new interface point.


