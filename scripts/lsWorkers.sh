#!/usr/bin/env bash
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

usage() {
    cat <<EOM
    Usage: scripts/$(basename $0) param1

           param1: <pem_file>
                      Specify the private key to connect to other machines in 
                      the cluster; the default is conf/pdb-key.pem
           
EOM
   exit -1;
}

[ -z $1 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

user=ubuntu
pem_file=$1
ip_len_valid=3
testSSHTimeout=3

if [ ! -f ${pem_file} ]; then
    echo -e "Pem file ""\033[33;31m""'$pem_file'""\e[0m"" not found, make sure the path and file name are correct!"
    exit -1;
fi

while read line
do
    [[ $line == *#* ]] && continue # skips commented lines
    [[ ! -z "${line// }" ]] && arr[i++]=$line # include only non-empty lines
done < $PDB_HOME/conf/serverlist

length=${#arr[@]}
if [ $length -eq 0 ]; then
   echo -e "[Error] There are no IP addresses in file  ""\e[31m""conf/serverlist""\e[0m""."
   echo -e "Make sure it contains at least one entry."
   exit -1;
else
   echo "There are $length worker nodes defined in conf/serverlist"
fi

resultOkHeader="*** Successful results ("
resultFailedHeader="*** Failed results ("
totalOk=0
totalFailed=0

for (( i=0 ; i<=$length ; i++ ))
do
   ip_addr=${arr[i]}
   if [ ${#ip_addr} -gt "$ip_len_valid" ]
   then
      # checks that ssh to a node is possible, times out after 3 seconds
      nc -zw$testSSHTimeout ${ip_addr} 22
      if [ $? -eq 0 ]
      then
         echo -e "\n+++++++++++ ls: $ip_addr"
         sleep 1
         ssh -i $pem_file $ip_addr 'ps aux | grep pdb-worker'
         sleep 1
         ssh -i $pem_file $ip_addr "cat $PDB_INSTALL/logs/log.out"
         resultOk+="Worker node with IP: $ip_addr successfully listed.\n"
         totalOk=`expr $totalOk + 1`
      else
         resultFailed+="Connection to ""\033[33;31m""IP ${ip_addr}""\e[0m"", failed.\n"
         totalFailed=`expr $totalFailed + 1`
      fi
   fi
done

echo -e "\033[33;35m""---------------------------------"
echo -e "Results of script $(basename $0):""\e[0m"
echo -e "$resultFailedHeader$totalFailed/$length) ***\n$resultFailed"
echo -e "$resultOkHeader$totalOk/$length) ***\n$resultOk"
echo -e "\033[33;35m""---------------------------------\n""\e[0m"
