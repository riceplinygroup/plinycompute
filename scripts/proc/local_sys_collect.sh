
#!/usr/bin/env bash


echo "[INFO] local system information collection"
cpu_num=`grep -c 'model name' /proc/cpuinfo`
echo -e "CPUNumber:\t $cpu_num"
grep 'MemTotal' /proc/meminfo
grep 'model name' /proc/cpuinfo
