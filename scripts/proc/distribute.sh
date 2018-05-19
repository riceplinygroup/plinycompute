
#!/usr/bin/env bash

usage() {
    cat <<EOM
    Usage: scripts/$(basename $0) param1

           param1: <pem_file> (e.g. conf/pdb-key.pem)
           param2: <username> (username)

EOM
   exit -1;
}

[ -z $1 ] && [ -z $2 ] && { usage; }

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

echo "-------------step1: distribute the shell programs"
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
        echo -e "\n+++++++++++ collect info for server: $ip_addr +++++++++++"
        ssh -i $pem_file $user@$ip_addr 'mkdir ~/pdb_temp'
        scp -i $pem_file -r $local_c_dir/local_sys_collect.sh $user@$ip_addr:~/pdb_temp/
      else
         echo "Cannot collect infor for server with IP address: ${ip_addr} on port 22, times out after $testSSHTimeout seconds."
      fi
   fi
done
