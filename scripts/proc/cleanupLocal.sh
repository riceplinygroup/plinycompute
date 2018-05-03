
#!/usr/bin/env bash

pem_file=$1
user=$2

ip_len_valid=3
echo "-------------step2: clean PDB temp directory"

arr=($(awk '{print $0}' $PDB_HOME/conf/serverlist))
length=${#arr[@]}
for (( i=0 ; i<=$length ; i++ ))
do
        ip_addr=${arr[i]}
	if [ ${#ip_addr} -gt "$ip_len_valid" ]
	then
            echo -e "\n+++++++++++ clear PDB temp directory for server: $ip_addr +++++++++++"
            ssh -i $pem_file $user@$ip_addr 'rm -r -f ~/pdb_temp'
	fi
done



