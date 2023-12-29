#!/bin/sh

#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/yeast_fsm -freq 280 -thread 32 -maxNodes 12
mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/gse1730_fsm -freq 280 -thread 32 -maxNodes 14
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/new_patent.lg -freq 24000 -thread 32 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/amazon.lg -freq 17000 -thread 32 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/dblp/dblp_fsm.lg.lg -freq 24000 -thread 32 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/youtube.lg -freq 2000 -thread 32 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/citationCiteseer/citationCiteseer.lg -freq 2000 -thread 32 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/CL-100K-1d8-L9/CL-100K-1d8-L9.lg -freq 2000 -thread 32 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/tech-as-skitter.lg -freq 2000 -thread 32 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/PROTEINS-full/PROTEINS-full.lg -freq 2000 -thread 32 -lb 1