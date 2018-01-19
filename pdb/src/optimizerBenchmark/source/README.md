TCAP to Prolog conversion:
==========================

The C++ code that produces Prolog rules is currently here:
https://svn.rice.edu/r/software_0/ObjectQueryModel/src/optimizerBenchmark/source/prologGenerator.cc


Note:
-----
   The API parseTCAPtoProlog(...) is the one that needs to be used.
   It takes a valid TCAP string as argument.

   I am not sure how the Prolog rules are supposed to be returned. 
   Right now I store all the result in a global variable.

   At the end the prolog rules are directly printed on console.
   
   Right now I copy the prolog rules into a *.pl file.
   
   
   
   
Prolog to TCAP conversion:
==========================
	https://svn.rice.edu/r/software_0/ObjectQueryModel/src/optimizerBenchmark/source/prologToTCAP.pl

	You should have the prolog rules generated in the previous step saved in a file,
	lets say "myprologrules.pl".
	
	Import "myprologrules.pl" first.
	Import "prologToTCAP.pl" after that.
	Run "tcapGenerator()."
	The TCAP is printed on console right now.
	
	Note that, the order of the original TCAP is not maintained. 

	