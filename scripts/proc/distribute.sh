
#!/usr/bin/env bash

local_c_dir=$PDB_HOME/scripts/proc
ip_len_valid=3
pem_file=$1
user=$2

echo "-------------step1: distribute the shell programs"
arr=($(awk '{print $0}' $PDB_HOME/conf/serverlist))
length=${#arr[@]}
for (( i=0 ; i<=$length ; i++ ))
do
        ip_addr=${arr[i]}
	if [ ${#ip_addr} -gt "$ip_len_valid" ]
	then
		echo -e "\n+++++++++++ collect info for server: $ip_addr +++++++++++"
                ssh -i $pem_file $user@$ip_addr 'mkdir ~/pdb_temp' 
                scp -i $pem_file -r $local_c_dir/local_sys_collect.sh $user@$ip_addr:~/pdb_temp/
	fi
done



