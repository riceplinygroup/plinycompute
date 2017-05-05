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

#ifndef C_PLUS_PLUS_LAM_CC
#define C_PLUS_PLUS_LAM_CC

#include <memory>
#include <iostream>
#include <vector>

#define CAST(TYPENAME, WHICH) ((*(((std :: vector <Handle <TYPENAME>> **) args)[WHICH]))[which])

namespace pdb {

template <typename F, typename ReturnType, typename ParamOne, 
					   typename ParamTwo,
					   typename ParamThree,
					   typename ParamFour,
					   typename ParamFive>
typename std :: enable_if <!std :: is_base_of <Nothing, ParamOne> :: value &&
			   std :: is_base_of <Nothing, ParamTwo> :: value &&
			   std :: is_base_of <Nothing, ParamThree> :: value &&
			   std :: is_base_of <Nothing, ParamFour> :: value &&
			   std :: is_base_of <Nothing, ParamFive> :: value, void> :: type callLambda (F &func, std :: vector <ReturnType> &assignToMe, int which, void **args) {
	assignToMe[which] = func (CAST (ParamOne, 0));	
}

template <typename F, typename ReturnType, typename ParamOne, 
					   typename ParamTwo,
					   typename ParamThree,
					   typename ParamFour,
					   typename ParamFive>
typename std :: enable_if <!std :: is_base_of <Nothing, ParamOne> :: value &&
			   !std :: is_base_of <Nothing, ParamTwo> :: value &&
			   std :: is_base_of <Nothing, ParamThree> :: value &&
			   std :: is_base_of <Nothing, ParamFour> :: value &&
			   std :: is_base_of <Nothing, ParamFive> :: value, void> :: type callLambda (F &func, std :: vector <ReturnType> &assignToMe, int which, void **args) {
	assignToMe[which] = func (CAST (ParamOne, 0), CAST (ParamTwo, 1));	
}

template <typename F, typename ReturnType, typename ParamOne, 
					   typename ParamTwo,
					   typename ParamThree,
					   typename ParamFour,
					   typename ParamFive>
typename std :: enable_if <!std :: is_base_of <Nothing, ParamOne> :: value &&
			   !std :: is_base_of <Nothing, ParamTwo> :: value &&
			   !std :: is_base_of <Nothing, ParamThree> :: value &&
			   std :: is_base_of <Nothing, ParamFour> :: value &&
			   std :: is_base_of <Nothing, ParamFive> :: value, void> :: type callLambda (F &func, std :: vector <ReturnType> &assignToMe, int which, void **args) {
	assignToMe[which] = func (CAST (ParamOne, 0), CAST (ParamTwo, 1), CAST (ParamThree, 2));	
}

template <typename F, typename ReturnType, typename ParamOne, 
					   typename ParamTwo,
					   typename ParamThree,
					   typename ParamFour,
					   typename ParamFive>
typename std :: enable_if <!std :: is_base_of <Nothing, ParamOne> :: value &&
			   !std :: is_base_of <Nothing, ParamTwo> :: value &&
			   !std :: is_base_of <Nothing, ParamThree> :: value &&
			   !std :: is_base_of <Nothing, ParamFour> :: value &&
			   std :: is_base_of <Nothing, ParamFive> :: value, void> :: type callLambda (F &func, std :: vector <ReturnType> &assignToMe, int which, void **args) {
	assignToMe[which] = func (CAST (ParamOne, 0), CAST (ParamTwo, 1), CAST (ParamThree, 2), CAST (ParamFour, 3));	
}

template <typename F, typename ReturnType, typename ParamOne, 
					   typename ParamTwo,
					   typename ParamThree,
					   typename ParamFour,
					   typename ParamFive>
typename std :: enable_if <!std :: is_base_of <Nothing, ParamOne> :: value &&
			   !std :: is_base_of <Nothing, ParamTwo> :: value &&
			   !std :: is_base_of <Nothing, ParamThree> :: value &&
			   !std :: is_base_of <Nothing, ParamFour> :: value &&
			   !std :: is_base_of <Nothing, ParamFive> :: value, void> :: type callLambda (F &func, std :: vector <ReturnType> &assignToMe, int which, void **args) {
	assignToMe[which] = func (CAST (ParamOne, 0), CAST (ParamTwo, 1), CAST (ParamThree, 2), CAST (ParamFour, 3), CAST (ParamFive, 4));	
}

template <typename F, typename ReturnType, typename ParamOne = Nothing,
					   typename ParamTwo = Nothing,
					   typename ParamThree = Nothing,
					   typename ParamFour = Nothing,
					   typename ParamFive = Nothing>
class CPlusPlusLambda : public TypedLambdaObject <ReturnType> {

private:

	F myFunc;
        int numInputs = 0;
public:

        //JiaNote: I changed CPlusPlusLambda constructor interface to obtain input information for query graph analysis.
	CPlusPlusLambda (F arg, Handle<ParamOne>  & input1, Handle<ParamTwo> & input2, Handle<ParamThree> & input3,
                  Handle<ParamFour> & input4, Handle<ParamFive> & input5) : myFunc (arg) {

            std :: cout << "CPlusPlusLambda:" << std :: endl;
            if (getTypeName<ParamOne>() != "pdb::Nothing") {
                 std :: cout << getTypeName<ParamOne>() << std :: endl;
                 std :: cout << "input1 type code is " << input1.getExactTypeInfoValue() << std :: endl;
                 this->numInputs ++;
                 this->setInputIndex(0, -((input1.getExactTypeInfoValue()+1)));
            }
            if (getTypeName<ParamTwo>() != "pdb::Nothing") {
                 std :: cout << getTypeName<ParamTwo>() << std :: endl;
                 std :: cout << "input2 type code is " << input2.getExactTypeInfoValue() << std :: endl;
                 this->numInputs ++;
                 this->setInputIndex(1, -((input2.getExactTypeInfoValue()+1)));
            }
            if (getTypeName<ParamThree>() != "pdb::Nothing") {
                 std :: cout << getTypeName<ParamThree>() << std :: endl;
                 std :: cout << "input3 type code is " << input3.getExactTypeInfoValue() << std :: endl;
                 this->numInputs ++;
                 this->setInputIndex(2, -((input3.getExactTypeInfoValue()+1)));
            }
            if (getTypeName<ParamFour>() != "pdb::Nothing") {
                 std :: cout << getTypeName<ParamFour>() << std :: endl;
                 std :: cout << "input4 type code is " << input4.getExactTypeInfoValue() << std :: endl;
                 this->numInputs ++;
                 this->setInputIndex(3, -((input4.getExactTypeInfoValue()+1)));
            }
            if (getTypeName<ParamFive>() != "pdb::Nothing") {
                 std :: cout << getTypeName<ParamFive>() << std :: endl;
                 std :: cout << "input5 type code is " << input5.getExactTypeInfoValue() << std :: endl;
                 this->numInputs ++;
                 this->setInputIndex(4, -((input5.getExactTypeInfoValue()+1)));
            }


        }


        unsigned int getNumInputs() override {
            return this->numInputs;
        }

	std :: string getTypeOfLambda () override {
		return std :: string ("native_lambda");
	}

        GenericLambdaObjectPtr getChild (int which) override {
                return nullptr;
        }

	int getNumChildren () override {
		return 0;
	}
	
	~CPlusPlusLambda () {}


	ComputeExecutorPtr getExecutor (TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput) override {

		// create the output tuple set
		TupleSetPtr output = std :: make_shared <TupleSet> ();

		// create the machine that is going to setup the output tuple set, using the input tuple set
		TupleSetSetupMachinePtr myMachine = std :: make_shared <TupleSetSetupMachine> (inputSchema, attsToIncludeInOutput);

		// this is the list of input attributes that we need to match on
		std :: vector <int> matches = myMachine->match (attsToOperateOn);

		// fix this!!  Use a smart pointer
		std :: shared_ptr <std :: vector <void *>> inputAtts = std :: make_shared <std :: vector <void *>> ();
		for (int i = 0; i < matches.size (); i++) {
			inputAtts->push_back (nullptr);
		}

		// this is the output attribute
		int outAtt = attsToIncludeInOutput.getAtts ().size ();

		return std :: make_shared <SimpleComputeExecutor> (
			output, 
			[=] (TupleSetPtr input) {
				
				// set up the output tuple set
				myMachine->setup (input, output);

				// get the columns to operate on
				int numAtts = matches.size ();
				void **inAtts = inputAtts->data ();
				for (int i = 0; i < numAtts; i++) {
					inAtts[i] = &(input->getColumn <int> (matches[i]));
				}

				// setup the output column, if it is not already set up
				if (!output->hasColumn (outAtt)) {
					std :: vector <ReturnType> *outputCol = new std :: vector <ReturnType>;
					output->addColumn (outAtt, outputCol, true);
				}

				// get the output column
				std :: vector <ReturnType> &outColumn = output->getColumn <ReturnType> (outAtt);

				// loop down the columns, setting the output
				int numTuples = ((std :: vector <Handle <ParamOne>> *) inAtts[0])->size ();
				outColumn.resize (numTuples);
				for (int i = 0; i < numTuples; i++) {
					callLambda <F, ReturnType, ParamOne, ParamTwo, ParamThree, ParamFour, ParamFive> 
						(myFunc, outColumn, i, inAtts);						
				}

				return output;
			}
		);	
	}
};

}

#endif
