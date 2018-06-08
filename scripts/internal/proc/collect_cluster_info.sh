
#!/usr/bin/env bash
usage() {

    echo -e "    Description: This script collects information about CPUs and memory"
    echo -e "    of the machines in a cluster."

    cat <<EOM
    Usage: scripts/internal/$(basename $0) param1 param2

           param1: <pem_file>
                      Specify the private key to connect to other machines in
                      the cluster; the default is conf/pdb-key.pem
           param2: <user_name>
                      Specify the username to ssh into worker nodes.

EOM
   exit -1;
}

[ -z $1 ] && [ -z $2 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

ip_len_valid=3
pem_file=$1
user_name=$2
testSSHTimeout=3

if [ ! -f ${pem_file} ]; then
    echo -e "Pem file ""\033[33;31m""'$pem_file'""\e[0m"" not found, make sure the path and file name are correct!"
    exit -1;
fi

if [ -z ${pem_file} ];
    then echo "ERROR: please provide two parameters: one is the path to your pem file and the other is the username to connect.";
    echo "Usage: scripts/internal/proc/collect_cluster_info.sh #pem_file #username";
    exit -1;
fi

if [ -z ${user_name} ];
    then echo "ERROR: please provide two parameters: one is the path to your pem file and the other is the username to connect.";
    echo "Usage: scripts/internal/proc/collect_cluster_info.sh #pem_file #username";
    exit -1;
fi

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
echo "There are $length worker nodes defined in conf/serverlist"

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
        echo -e "\n+++++++++++ collect system info for server: $ip_addr"
        ssh -i $pem_file $user_name@$ip_addr '~/pdb_temp/local_sys_collect.sh'
        resultOk+="Worker node with IP $ip_addr successfully collected system info.\n"
        totalOk=`expr $totalOk + 1`
      else
        resultFailed+="Connection to ""\033[33;31m""IP ${ip_addr}""\e[0m"", failed. System info was not collected.\n"
        totalFailed=`expr $totalFailed + 1`
        echo -e "Connection to ""\033[33;31m""IP ${ip_addr}""\e[0m"", failed."
      fi
   fi
done

echo -e "\033[33;35m""---------------------------------"
echo -e "Results of script $(basename $0):""\e[0m"
echo -e "$resultFailedHeader$totalFailed/$length) ***\n$resultFailed"
echo -e "$resultOkHeader$totalOk/$length) ***\n$resultOk"
echo -e "\033[33;35m""---------------------------------\n""\e[0m"

