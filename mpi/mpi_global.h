#pragma once

#include <mpi.h>

//worker info
#define MASTER_RANK 0

#define SLEEP_PARAM 2000 // POLLING_TIME = SLEEP_PARAM * _num_workers

int _my_rank;
int _num_workers;
inline int get_worker_id()
{
    return _my_rank;
}
inline int get_num_workers()
{
    return _num_workers;
}

int POLLING_TIME; //unit: usec, user-configurable, used by sender
//set in init_worker()
static clock_t polling_ticks; // = POLLING_TIME * CLOCKS_PER_SEC / 1000000;

void init_worker(int * argc, char*** argv)
{
	int provided;
	MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided); //Multiple threads may call MPI, with no restrictions
	if(provided != MPI_THREAD_MULTIPLE)
	{
	    printf("MPI do not Support Multiple thread\n");
	    exit(0);
	}
	MPI_Comm_size(MPI_COMM_WORLD, &_num_workers);
	MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
    POLLING_TIME = SLEEP_PARAM * _num_workers;
    polling_ticks = POLLING_TIME * CLOCKS_PER_SEC / 1000000;
}

void worker_finalize()
{
    MPI_Finalize();
}

void worker_barrier()
{
    MPI_Barrier(MPI_COMM_WORLD); //only usable before creating threads
}


enum MPICHANNEL
{
    GRAPH_LOAD_CHANNEL = 200,
    REQ_CHANNEL = 201,
    RESP_CHANNEL = 202,
    STATUS_CHANNEL = 203,
    AGG_CHANNEL = 204,
    PROGRESS_CHANNEL = 205,
    RESULT_CHANNEL = 206
}
