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
# by Jia


user=ubuntu

arr=($(awk '{print $0}' $HADOOP_HOME/etc/hadoop/slaves))
length=${#arr[@]}
echo "There are $length servers"
for (( i=0 ; i<$length ; i++ ))
do
        host=${arr[i]}
        echo -e "\n+++++++++++ reconfig: $host"
#        ssh $host 'java -version'
#        ssh ubuntu@$host 'mkdir /home/ubuntu/hadoop-data'     
#         scp etc/hadoop/slaves ubuntu@$host:$HADOOP_HOME/etc/hadoop/
#          scp etc/hadoop/yarn-env.sh ubuntu@$host:$HADOOP_HOME/etc/hadoop/
#           scp etc/hadoop/core-site.xml ubuntu@$host:$HADOOP_HOME/etc/hadoop/

#         scp etc/hadoop/hdfs-site.xml ubuntu@$host:$HADOOP_HOME/etc/hadoop/
#           scp etc/hadoop/mapred-site.xml ubuntu@$host:$HADOOP_HOME/etc/hadoop/
         scp etc/hadoop/yarn-site.xml ubuntu@$host:$HADOOP_HOME/etc/hadoop/
#        ssh $host '/home/ubuntu/hadoop-2.7.3/sbin/hadoop-daemons.sh --config /home/ubuntu/hadoop-2.7.3/etc/hadoop --script hdfs stop datanode'
done

