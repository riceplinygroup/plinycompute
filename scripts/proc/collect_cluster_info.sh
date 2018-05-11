
#!/usr/bin/env bash

ip_len_valid=3
pem_file=$1
user=$2
testSSHTimeout=3

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
        echo -e "\n+++++++++++ collect system info for server: $ip_addr"
        ssh -i $pem_file $user@$ip_addr '~/pdb_temp/local_sys_collect.sh'
      else
         echo "Cannot collect system info for server with IP address: ${ip_addr} on port 22, times out after $testSSHTimeout seconds."
      fi
   fi
done


