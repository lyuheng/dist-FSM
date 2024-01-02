#!/bin/sh

#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/yeast_fsm -freq 270 -thread 32 -maxNodes 12
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/gse1730_fsm -freq 280 -thread 32 -maxNodes 12
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/new_patent.lg -freq 20000 -thread 2 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/amazon.lg -freq 15000 -thread 32 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/dblp/dblp_fsm.lg -freq 1500 -thread 32 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/youtube.lg -freq 1600 -thread 8 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/citationCiteseer/citationCiteseer.lg -freq 2000 -thread 32 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/CL-100K-1d8-L9/CL-100K-1d8-L9.lg -freq 1700 -thread 32 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/tech-as-skitter.lg -freq 6000 -thread 1 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/PROTEINS-full/PROTEINS-full.lg -freq 7000 -thread 32 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/new_patent.lg -freq 24000 -thread 1 -lb 1
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/UKPOI_fsm.lg -freq 7500 -thread 32 -lb 0
#mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/web-wiki-ch-internal.lg -freq 5700 -thread 1 -lb 1