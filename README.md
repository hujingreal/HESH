# HESH: A Highly Efficient Scalable Hashing for Hybrid DRAM-PM Memory System

**HESH** is a highly efficient and scalable hashing scheme for hybrid DRAM-PM memory systems. Firstly, HESH is designed to fully leverage the parallelism of PM. In a multi-threaded environment, it ensures that each DIMM is accessed by an equal number of threads, an achievement made possible through a well-designed structure and an efficient equalization algorithm. Secondly, HESH minimizes extra writes to PM by reducing critical metadata updates in the PM allocator, and it reduces reads to PM by relocating most accesses to DRAM. Lastly, HESH introduces a new DRAM index based on extendible hashing, providing a stable and highly efficient index for key-value pairs stored on PM.

## Paper
This is the code for our paper **HESH: A Hybrid PMem-DRAM Persistent Hash Index with Fast RecoveryA Highly Efficient Scalable Hashing for Hybrid DRAM-PM Memory System**.

## Dependencies
### For building
#### Required
* `libpmem` in PMDK 
* `libvmem` in PMDK



## How to build
* Call `make` to generate all binaries.

## To generate YCSB workloads
```sh
cd YCSB
wget https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar -xvf ycsb-0.17.0.tar.gz
mv ycsb-0.17.0 YCSB
#Then run workload generator
mkdir workloads
./generate_all_workloads.sh
```
## To generate PiBench workloads
```sh
cd PiBench
make
./auto_gene.sh
```
## How to run
See `autorun.sh`

## Note
1. You are required to modify the path of the PM, identified as ‘index_pool_name’, in the ‘hash_api.h’ file to match the path in your specific environment. For SOFT, you also need to set the path named ‘PMEM_LOC1’ in the ‘ssmem.cpp’ file. Additionally, for the 'autorun.sh' script, you must configure the path for the 'pm' variable.
2. Due to hardware constraints, our experimental setup only uses four Optane DIMMs. If your system is equipped with six Optane DIMMs, you must adjust the value of ‘kDimmNum’ in the ‘hesh_pm_allocator.h’ file to six. This adjustment ensures that the number of Optane DIMMs in your environment is accurately reflected.
