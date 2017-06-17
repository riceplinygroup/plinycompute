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
#include <iostream>
#include <cassert>

#include "LAParser.h"
#include "LAPDBInstance.h"
#include "LAStatementsList.h"




int main(int argc, char **argv){
	
	assert(argc==6);
	bool printResult = (strcmp(argv[1],"Y")==0);
	bool clusterMode = (strcmp(argv[2],"Y")==0);
	int blockSize = atoi(argv[3]);
	int port = 8108;
	std::string masterIP = argv[4];
	FILE * targetCode = fopen(argv[5],"r");
		
	if(!targetCode){
		std::cout<< "No such file ! <" << argv[5] << ">" << std::endl;
		return -1;
	}

	pdb :: PDBLoggerPtr clientLogger = make_shared<pdb :: PDBLogger>("LAclientLog");
	LAPDBInstance instance(printResult,clusterMode,blockSize,masterIP,port,clientLogger);
		
	LAscan_t myscanner;
	LAlex_init(&myscanner);
	LAset_in(targetCode,myscanner);
	std:: cout <<"Get started to parse the code!" << std::endl;
	LAStatementsList * myStatements = new LAStatementsList();
	LAparse(myscanner,&myStatements);
	LAlex_destroy(myscanner);
	std::cout<<"Parsing Done:" <<std::endl;
	for(int i=0; i<myStatements->size();i++){
		std::cout << myStatements->get(i)->toString() << std::endl;
	}
	std::cout<<"Start executation:" <<std::endl;
	for(int i=0; i<myStatements->size();i++){
		std::cout << "Current statement:" << myStatements->get(i)->toString() << std::endl;
		myStatements->get(i)->evaluateQuery(instance);
	}
	instance.clearCachedSets();
	int code = system ("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        std :: cout << "Can't cleanup so files" << std :: endl;
    }
}