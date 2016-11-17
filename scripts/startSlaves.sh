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
# by Jia

pem_file=$1

user=ubuntu
ip_len_valid=3

arr=($(awk '{print $0}' $PDB_HOME/conf/serverlist))
length=${#arr[@]}
echo "There are $length servers"
for (( i=0 ; i<=$length ; i++ ))
do
        ip_addr=${arr[i]}
        if [ ${#ip_addr} -gt "$ip_len_valid" ]
        then
                echo -e "\n+++++++++++ install and start server: $ip_addr"
                ssh -i $pem_file $user@$ip_addr 'rm -rf ~/pdb_temp'
                ssh -i $pem_file $user@$ip_addr 'mkdir ~/pdb_temp'
                ssh -i $pem_file $user@$ip_addr 'mkdir ~/pdb_temp/scripts'
                ssh -i $pem_file $user@$ip_addr 'mkdir ~/pdb_temp/bin'
                ssh -i $pem_file $user@$ip_addr 'mkdir ~/pdb_temp/CatalogDir'
                ssh -i $pem_file $user@$ip_addr 'mkdir ~/pdb_temp/logs'
                scp -i $pem_file -r $PDB_HOME/bin/test603 $user@$ip_addr:~/pdb_temp/bin/
                scp -i $pem_file -r $PDB_HOME/scripts/cleanupNode.sh $user@$ip_addr:~/pdb_temp/scripts/
                ssh -i $pem_file $user@$ip_addr ~/pdb_temp/scripts/cleanupNode.sh
                ssh -i $pem_file $user@$ip_addr ~/pdb_temp/bin/test603 &
                sleep 10
                $PDB_HOME/bin/CatalogServerTests 0 localhost 8108 $ip_addr 8108 worker worker
        fi
done

