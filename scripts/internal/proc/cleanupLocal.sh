
#!/usr/bin/env bash

usage() {
    cat <<EOM

    Description: This script removes the ~/pdb_temp folder on all worker
    nodes.

    Usage: scripts/$(basename $0) param1 param2

           param1: <pem_file>
                      Specify the private key to connect to other machines in
                      the cluster; the default is conf/pdb-key.pem
           param2: <user_name>
                      Specify the username to ssh into worker nodes.

EOM
   exit -1;
}

[ -z $1 ] && [ -z $2 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

pem_file=$1
user=$2
testSSHTimeout=3
ip_len_valid=3

if [ ! -f ${pem_file} ]; then
    echo -e "Pem file ""\033[33;31m""'$pem_file'""\e[0m"" not found, make sure the path and file name are correct!"
    exit -1;
fi

echo "-------------step2: clean PDB temp directory"

echo "Reading cluster IP addresses from file: $PDB_HOME/conf/serverlist"

if [ ! -f $PDB_HOME/conf/serverlist ];then
   echo -e "The file ""\033[33;31m""conf/serverlist""\e[0m"" was not found."
   echo -e "Make sure ""\033[33;31m""conf/serverlist""\e[0m"" exists"
   echo -e "and contains the IP addresses of the worker nodes."
   exit -1
fi

while read line
do
   [[ $line == *#* ]] && continue # skips commented lines
   [[ ! -z "${line// }" ]] && arr[i++]=$line # include only non-empty lines
done < $PDB_HOME/conf/serverlist

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



