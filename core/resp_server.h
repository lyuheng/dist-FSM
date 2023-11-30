#pragma once

#include <thread>
#include "global.h"
#include "mpi_global.h"
#include "cache_table.h"


template <typename KeyT, typename ValueT, typename PendingType, typename ReadyType>
class RespServer
{
public:
    typedef CacheTable<KeyT, ValueT> CacheTableT;
    CacheTableT & cache_table;
    std::thread main_thread;

    PendingType & pending_patterns;
    ReadyType & ready_patterns;

    void thread_func(char * buf, int size, int src)
    {
        obinstream m(buf, size);
        RespondMsg value;
        while (m.end() == false)
        {
            m >> value;
            std::vector<KeyT> qid_collector;
            cache_table.insert(value.qid, value.candidates, qid_collector);
            cout << &value << " Receive value of key = " << value.qid << endl;

            // notify those patterns
            for (auto iter = qid_collector.begin(); iter < qid_collector.end(); ++iter)
            {
                KeyT key = *iter;
                auto & bucket = pending_patterns.get_bucket(key);
                bucket.lock();
                auto & kvmap = bucket.get_map();
                auto it = kvmap.find(key);
                assert(it != kvmap.end());
                task_container * tc_new = it->second;
                kvmap.erase(it);
                bucket.unlock();
                ready_patterns.enqueue(tc_new);
                pending_patterns_num--;
            }
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

    RespServer(CacheTableT & cache_table, PendingType & pending_patterns, ReadyType & ready_patterns): 
        cache_table(cache_table),
        pending_patterns(pending_patterns),
        ready_patterns(ready_patterns)
    {
    	main_thread = std::thread(&RespServer::run, this);
    }

    ~RespServer()
	{
        if(main_thread.joinable())
    	    main_thread.join();
        
        cout << "RespServer done at rank" << _my_rank << endl;
	}
};