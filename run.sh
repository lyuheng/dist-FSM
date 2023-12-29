#!/bin/sh

mpiexec -hostfile $PBS_NODEFILE -n $1 ./run -file ../datasets/yeast_fsm -freq 280 -thread 32 -maxNodes 12