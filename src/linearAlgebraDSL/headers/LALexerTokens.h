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
#ifndef LA_LEX_TOKENS_H
#define LA_LEX_TOKENS_H


#ifdef END
	#undef END 
	#undef TOKEN_ZEROS				
	#undef TOKEN_ONES				
	#undef TOKEN_IDENTITY			
	#undef TOKEN_LOAD              
	#undef TOKEN_TRANSPOSE 		
	#undef TOKEN_INV				
	#undef TOKEN_MULTIPLY			
	#undef TOKEN_TRANSPOSEMULTIPLY	
	#undef TOKEN_ADD				
	#undef TOKEN_MINUS			
	#undef TOKEN_LEFT_BRACKET		
	#undef TOKEN_RIGHT_BRACKET		
	#undef TOKEN_COMMA				
	#undef TOKEN_ASSIGN			
	#undef TOKEN_MAX				
	#undef TOKEN_MIN 				
	#undef TOKEN_ROWMAX			
	#undef TOEKN_ROWMIN			
	#undef TOKEN_COLMAX			
	#undef TOKEN_COLMIN				
	#undef DOUBLE					
	#undef INTEGER					
	#undef IDENTIFIER				
	#undef STRINGLITERAL       	
#endif



#define END    				0
//Initializer 
#define TOKEN_ZEROS				701
#define TOKEN_ONES				702
#define TOKEN_IDENTITY			703
#define TOKEN_LOAD              704

//postfix operator
#define TOKEN_TRANSPOSE 		711
#define TOKEN_INV				712

//multiplicative operator
#define TOKEN_MULTIPLY			721
#define TOKEN_TRANSPOSEMULTIPLY	722


//additive operator
#define TOKEN_ADD				731
#define TOKEN_MINUS				732

//Ohers
#define TOKEN_LEFT_BRACKET		741
#define TOKEN_RIGHT_BRACKET		742
#define TOKEN_COMMA				743
#define TOKEN_ASSIGN			744

//Built in functions
#define TOKEN_MAX				751
#define TOKEN_MIN 				752
#define TOKEN_ROWMAX			753
#define TOKEN_ROWMIN			754
#define TOKEN_COLMAX			755
#define TOKEN_COLMIN			756	


#define DOUBLE					790
#define INTEGER					791
#define IDENTIFIER				792
#define STRINGLITERAL       	793




#endif