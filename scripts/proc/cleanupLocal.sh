
#!/usr/bin/env bash

usage() {
    cat <<EOM

    Description: This script removes the ~/pdb_temp folder on all worker
    nodes.

    Usage: scripts/$(basename $0) param1 param2

           param1: <pem_file> (e.g. conf/pdb-key.pem)
           param2: <user_name> (username to ssh to worker nodes)

EOM
   exit -1;
}

[ -z $1 ] && [ -z $2 ] && { usage; }

pem_file=$1
user=$2
testSSHTimeout=3
ip_len_valid=3

echo "-------------step2: clean PDB temp directory"

echo "Reading cluster IP addresses from file: $conf_file"

while read line
do
   [[ $line == *#* ]] && continue # skips commented lines
   [[ ! -z "${line// }" ]] && arr[i++]=$line # include only non-empty lines
done < $PDB_HOME/conf/serverlist

if [ $? -ne 0 ]
then
   echo -e "Either ""\033[33;31m""conf/serverlist""\e[0m" or "\033[33;31m""conf/serverlist.test""\e[0m"" files were not found."
   echo -e "If running in standalone mode, make sure ""\033[33;31m""conf/serverlist.test""\e[0m"" exists."
   echo -e "If running in distributed mode, make sure ""\033[33;31m""conf/serverlist""\e[0m"" exists"
   echo -e "with the IP addresses of the worker nodes."
   exit -1
fi

length=${#arr[@]}
echo "There are $length servers defined in $PDB_HOME/conf/serverlist"

for (( i=0 ; i<=$length ; i++ ))
do
   ip_addr=${arr[i]}
   if [ ${#ip_addr} -gt "$ip_len_valid" ]
   then
      # checks that ssh to a node is possible, times out after 3 seconds
      nc -zw$testSSHTimeout ${ip_addr} 22
      if [ $? -eq 0 ]
      then
         echo -e "\n+++++++++++ clear PDB temp directory for server: $ip_addr +++++++++++"
         ssh -i $pem_file $user@$ip_addr 'rm -r -f ~/pdb_temp'
      else
         echo "Connection clear PDB temp directory for IP address:  ${ip_addr} on port 22 timeout after $testSSHTimeout seconds."
      fi
   fi
done



