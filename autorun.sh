#!/bin/bash
#Lib path of libvmem
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
CURR=$(pwd)
export LD_LIBRARY_PATH=$CURR/third/pmdk/src/nondebug:$LD_LIBRARY_PATH

pm="/data/pmem0/" #PM path

for w in ycsba ycsbb ycsbc ycsbd ycsbe ycsbf ycsbg ycsbh PiBench1 PiBench2 PiBench4 PiBench5 PiBench6 PiBench8  
do
    for t in 1 4 8 16 24 36
    do  
        for h in HESH HALO VIPER SOFT DASH CCEH
        do
            if [ $w = ycsba ] || [ $w = ycsbe ] || [ $w = PiBench3 ] || [ $w = PiBench7 ]
            then
                numactl -N 0 ./$h $w $t 0 200 8 
            else
                numactl -N 0 ./$h $w $t 200 200 8 
            fi
            rm "$pm"*.data -rf
            rm "$pm"Halo -rf
            rm "$pm"vmem_test -rf
            rm "$pm"HESH* -rf
            rm "$pm"slm -rf
            echo "-------------------------\n"
        done
    done
done