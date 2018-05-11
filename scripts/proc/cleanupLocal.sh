
#!/usr/bin/env bash

pem_file=$1
user=$2
testSSHTimeout=3
ip_len_valid=3

echo "-------------step2: clean PDB temp directory"

arr=($(awk '{print $0}' $PDB_HOME/conf/serverlist))
length=${#arr[@]}
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



