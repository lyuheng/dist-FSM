#pragma once

#include <thread>
#include "global.h"
#include "mpi_global.h"
#include "cache_table.h"

template <typename KeyT, typename ValueT>
class RespServer
{
public:
    typedef CacheTable<KeyT, ValueT> CacheTableT;
    CacheTableT & cache_table;
    std::thread main_thread;

    void thread_func(char * buf, int size, int src)
    {
        obinstream m(buf, size);
        RespondMsg value;
        while (m.end() == false)
        {
            m >> value;
            cache_table.insert(value->qid, value->candidates);
            // temporarily don't notify
        }
    }

    void run()
    {
        bool first = true;
    	std::thread t;
    	//------
    	while(global_end_label == false) //otherwise, thread terminates
    	{
    		int has_msg;
    		MPI_Status status;
    		MPI_Iprobe(MPI_ANY_SOURCE, RESP_CHANNEL, MPI_COMM_WORLD, &has_msg, &status);
    		if(!has_msg) 
                usleep(WAIT_TIME_WHEN_IDLE);
    		else
    		{
    			int size;
    			MPI_Get_count(&status, MPI_CHAR, &size); // get size of the msg-batch (# of bytes)
    			char * buf = new char[size]; //space for receiving this msg-batch, space will be released by obinstream in thread_func(.)
    			MPI_Recv(buf, size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    			if(!first) t.join(); //wait for previous CPU op to finish; t can be extended to a vector of threads later if necessary
    			t = std::thread(&RespServer::thread_func, this, buf, size, status.MPI_SOURCE);
    			first = false;
    		}
    	}
    	if(!first) t.join();
    }

    RespServer(CacheTableT & cache_table): cache_table(cache_table)
    {
    	main_thread = std::thread(&RespServer::run, this);
    }

    ~RespServer()
	{
        if(main_thread.joinable())
    	    main_thread.join();
	}
};