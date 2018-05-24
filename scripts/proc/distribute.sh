
#!/usr/bin/env bash

usage() {
   
    echo -e "    Description: This script copies scripts needed for collecting"
    echo -e "    information about CPUs and memory of the machines in a cluster."

    cat <<EOM

    Usage: scripts/$(basename $0) param1

           param1: <pem_file>
                      Specify the private key to connect to other machines in
                      the cluster; the default is conf/pdb-key.pem
           param2: <user_name>
                      Specify the username to ssh into worker nodes.

EOM
   exit -1;
}

[ -z $1 ] && [ -z $2 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

local_c_dir=$PDB_HOME/scripts/proc
ip_len_valid=3
pem_file=$1
user=$2
testSSHTimeout=3

if [ -z ${pem_file} ];
    then echo "ERROR: please provide two parameters: one is the path to your pem file and the other is the username to connect.";
    echo "Usage: scripts/proc/distribute.sh #pem_file #username";
    exit -1;
fi

if [ -z ${user} ];
    then echo "ERROR: please provide two parameters: one is the path to your pem file and the other is the username to connect.";
    echo "Usage: scripts/proc/distribute.sh #pem_file #username";
    exit -1;
fi

if [ ! -f ${pem_file} ]; then
    echo -e "Pem file ""\033[33;31m""'$pem_file'""\e[0m"" not found, make sure the path and file name are correct!"
    exit -1;
fi

echo "-------------step1: distribute the shell programs"

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
        echo -e "\n+++++++++++ collect info for server: $ip_addr +++++++++++"
        ssh -i $pem_file $user@$ip_addr 'mkdir ~/pdb_temp'
        scp -i $pem_file -r $local_c_dir/local_sys_collect.sh $user@$ip_addr:~/pdb_temp/
      else
         echo "Cannot collect infor for server with IP address: ${ip_addr} on port 22, times out after $testSSHTimeout seconds."
      fi
   fi
done
