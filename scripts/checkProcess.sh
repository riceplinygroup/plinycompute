#  Copyright 2018 Rice University                                           
#                                                                           
#  Licensed under the Apache License, Version 2.0 (the "License");          
#  you may not use this file except in compliance with the License.         
#  You may obtain a copy of the License at                                  
#                                                                           
#      http://www.apache.org/licenses/LICENSE-2.0                           
#                                                                           
#  Unless required by applicable law or agreed to in writing, software      
#  distributed under the License is distributed on an "AS IS" BASIS,        
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
#  See the License for the specific language governing permissions and      
#  limitations under the License.                                           
#  ======================================================================== 
#!/bin/bash
processName=$1
echo "to check $processName"

numProcesses=0

for x in {1..10};
do
   if pgrep -x $processName > /dev/null
   then
       echo "$processName is started!"
       numProcesses=1
       break
   fi
   sleep 15
done

if [ "$numProcesses" -eq 0 ]; then
    echo "$processName hasn't started!"
    exit 1
fi

