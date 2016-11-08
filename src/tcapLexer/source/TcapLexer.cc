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
#include "TcapLexer.h"

#include <functional>
#include <map>
#include <memory>
#include <iostream>

using std::function;
using std::make_shared;
using std::map;

namespace pdb_detail
{
    /**
     * Incriment pos to the first value where source[value] is not a whitespace character or, if only
     * whitespace characters remain from pos to the end of the string, the length of source.
     *
     * pos remains unchanged if it is not a valid index of source.
     *
     * @param pos the starting position in source
     * @param source the inptu string
     */
    void advanceThroughWhitespace(string::size_type &pos, const string &source)
    {
        string::size_type zero = 0; // this is to get rid of a warning. compiler is too smart to use a literal in
        if(pos < zero || pos>=source.length()) // if statement because it knows string::size_type is unsigned
            return;                            // when we don't know that in this file.

        // tests if source[pos] is a whitespace char
        function<bool()> isAtWhitespace = [&]
        {
            switch(source[pos])
            {
                case ' ':
                case '\t':
                case '\n':
                case '\v':
                case '\f':
                case '\r':
                    return true;
                default:
                    return false;
            }
        };

        while(pos < source.length() && isAtWhitespace())
            pos++;
    }

    /**
     * If source contains an identifier starting at pos, returns that identifier and advances pos to the value
     * one past the last character of the identifier. Otherwise returns the empty string and leaves pos unchanged.
     *
     * @param pos the starting position in source
     * @param source the input string
     * @return the found identifier, or the empy string if no identifier was found.
     */
    string consumeIdentifier(string::size_type &pos, const string &source)
    {
        string::size_type zero = 0; // this is to get rid of a warning. compiler is too smart to use a literal in
        if(pos < zero || pos>=source.length()) // if statement because it knows string::size_type is unsigned
            return "";                         // when we don't know that in this file.

        // tests if source[pos] is a character used for identifier construction
        function<bool()> isAtIdChar = [&]
        {
            char c = source[pos];
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
        };

        string::size_type start = pos;
        while(pos < source.length() && isAtIdChar())
            pos++;

        return source.substr(start, pos - start);
    }

    /**
     * If source contains symbol starting at pos, returns symbol and advances pos to the value
     * one past the last character of the symbol in source. Otherwise returns the empty string and leaves pos unchanged.
     *
     * @param pos the starting position in source
     * @param source the input string
     * @return the found symbol value, or the empy string if source does not start with symbol
     */
    string match(const string &symbol, string::size_type &pos, const string &source)
    {
        string::size_type zero = 0; // this is to get rid of a warning. compiler is too smart to use a literal in
        if(pos < zero || pos>=source.length()) // if statement because it knows string::size_type is unsigned
            return "";                         // when we don't know that in this file.

        // see if the characters starting at source[pos] match the string symbol one at a time
        for(int j = 0; j<symbol.length(); j++)
        {
            if(pos + j >= source.length()) // is symbol longer than the substring starting at source[pos]?
                return ""; // can't be a match

            if(symbol[j] != source[pos+j]) // do the characters not line up at j?
                return ""; // can't be a match
        }

        // is a match!

        pos+=symbol.length();
        return symbol;
    }

    /**
     * If source contains a string literal starting at pos, returns the string literal and advances pos to the value
     * one past the terminating " charecter in source. Otherwise returns the empty string and leaves pos unchanged.
     *
     * @param pos the starting position in source
     * @param source the input string
     * @return the found string literal (including " chars), or the empty string if source does not start
     *         with a string literal including terminating character.
     */
    string consumeStringLiteral(string::size_type &pos, const string &source)
    {
        string::size_type zero = 0; // this is to get rid of a warning. compiler is too smart to use a literal in
        if(pos < zero || pos>=source.length()) // if statement because it knows string::size_type is unsigned
            return "";                         // when we don't know that in this file.

        if(source[pos] != '"') // string literals must start with "
            return "";

        string::size_type start = pos;
        pos++;

        while(pos < source.length() && source[pos] != '"')
            pos++;

        if(pos == source.length()) // did we run out of source before we saw the terminating "?
        {
            pos = start; // contract says pos is unchanged if not a match on the string literal
            return ""; // unterminated string, don't return anything
        }


        pos++; // advance past the terminating "
        string literal = source.substr(start, pos - start);

        return literal;


    }

    // contract from .h
    TcapTokenStream lexTcap(const string &source)
    {
        // tokenize the given source string and store the order of tokens and token types here
        shared_ptr<vector<TcapToken>> tokensAccum = make_shared<vector<TcapToken>>();

        map<string,TcapTokenType> reservedLexemeToTokenType;
        reservedLexemeToTokenType["("]      =  TcapTokenType::LPAREN_TYPE;
        reservedLexemeToTokenType[")"]      =  TcapTokenType::RPAREN_TYPE;
        reservedLexemeToTokenType["="]      =  TcapTokenType::EQ_TYPE;
        reservedLexemeToTokenType["load"]   =  TcapTokenType::LOAD_TYPE;
        reservedLexemeToTokenType["apply"]  =  TcapTokenType::APPLY_TYPE;
        reservedLexemeToTokenType["to"]     =  TcapTokenType::TO_TYPE;
        reservedLexemeToTokenType["["]      =  TcapTokenType::LBRACKET_TYPE;
        reservedLexemeToTokenType["]"]      =  TcapTokenType::RBRACKET_TYPE;
        reservedLexemeToTokenType["retain"] =  TcapTokenType::RETAIN_TYPE;
        reservedLexemeToTokenType["all"]    =  TcapTokenType::ALL_TYPE;
        reservedLexemeToTokenType[","]      =  TcapTokenType::COMMA_TYPE;
        reservedLexemeToTokenType["by"]     =  TcapTokenType::BY_TYPE;
        reservedLexemeToTokenType["store"]  =  TcapTokenType::STORE_TYPE;
        reservedLexemeToTokenType["filter"] =  TcapTokenType::FILTER_TYPE;
        reservedLexemeToTokenType["none"]   =  TcapTokenType::NONE_TYPE;
        reservedLexemeToTokenType["@"]      =  TcapTokenType::AT_SIGN_TYPE;
        reservedLexemeToTokenType["func"]   =  TcapTokenType::FUNC_TYPE;
        reservedLexemeToTokenType["method"] =  TcapTokenType::METHOD_TYPE;
        reservedLexemeToTokenType["hoist"]  =  TcapTokenType::HOIST_TYPE;
        reservedLexemeToTokenType["from"]   =  TcapTokenType::FROM_TYPE;
        reservedLexemeToTokenType[">"]      =  TcapTokenType::GREATER_THAN_TYPE;

        string::size_type pos = 0; // the current read position in source

        while(true) // each loop adds one (or zero if done) token to tokensAccum. returns when source is fully scanned.
        {
            outerLoop:

            // skip any whitespace at the current read position.
            advanceThroughWhitespace(pos, source);  // no problem invoking if we are already at end of source

            if(pos>=source.length()) // end of input, done.
                return TcapTokenStream(tokensAccum);

            /*
             * Check if next lexeme is a string literal
             */
            if (source[pos] == '"') // if start of string literal
            {
                string lexeme = consumeStringLiteral(pos, source);

                if (lexeme != "") // if no errors consuming the string
                {
                    tokensAccum->push_back(TcapToken(lexeme, TcapTokenType::STRING_LITERAL_TYPE));
                }
                else // e.g. unterminated string literal
                {
                    tokensAccum->push_back(TcapToken(lexeme, TcapTokenType::UNKNOWN_TYPE));
                    pos++; // pos unchanged by consumeStringLiteral.  must make progress on pos or will loop forever.
                }

                continue; // read next token
            }

            /*
             * Check if next lexeme is a reserved token
             */
            for(const auto& kv : reservedLexemeToTokenType)
            {
                string reservedTokenSymbol = kv.first;
                string lexeme = match(reservedTokenSymbol, pos, source);

                if (lexeme != "") // a match for some reserved token!
                {
                    TcapTokenType reservedType = kv.second;
                    tokensAccum->push_back(TcapToken(lexeme, reservedType));
                    goto outerLoop; // read next token
                }
            }

            /*
             * Check if next lexeme is identifier
             */
            string lexeme = consumeIdentifier(pos, source);

            if(lexeme != "") // if well formed identifier
            {
                tokensAccum->push_back(TcapToken(lexeme, TcapTokenType::IDENTIFIER_TYPE));
                continue; // read next token
            }

            /*
             * Otherwise, an unknown lexeme
             */
            string token = source.substr(pos, 1); // malformed input. eat one char and keep going
            pos++;
            tokensAccum->push_back(TcapToken(token, TcapTokenType::UNKNOWN_TYPE));

        }
    }

}
