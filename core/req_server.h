#pragma once

#include <thread>
#include "global.h"
#include "mpi_global.h"

#include "resp_queue.h"


class ReqServer
{
public:
    std::thread main_thread;
    RespQueue<RespondMsg> q_resp;

    void thread_func(char *buf, int size, int src)
    {
        obinstream m(buf, size);
		RequestMsg key;
		while(m.end() == false)
		{
		    m >> key;
            auto & bucket = g_pattern_prog_map.get_bucket(key.parent_qid);
            bucket.lock();
            auto & kvmap = bucket.get_map();
            auto it = kvmap.find(key.parent_qid);
            assert(it != kvmap.end());
            bucket.unlock();
            cout << "send value of key = " << key.parent_qid << endl;
            q_resp.add(RespondMsg{key.parent_qid, &(it->second->candidates)}, src);
		}
    }

    void run()
    {
        bool first = true;
    	std::thread t;

        while (global_end_label == false)
        {
            int has_msg;
            MPI_Status status;
    		MPI_Iprobe(MPI_ANY_SOURCE, REQ_CHANNEL, MPI_COMM_WORLD, &has_msg, &status); 
            if(!has_msg) 
                usleep(WAIT_TIME_WHEN_IDLE);
            else
    		{
    			int size;
    			MPI_Get_count(&status, MPI_CHAR, &size); // get size of the msg-batch (# of bytes)
    			char * buf = new char[size]; //space for receiving this msg-batch, space will be released by obinstream in thread_func(.)
    			MPI_Recv(buf, size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    			if(!first) t.join(); //wait for previous CPU op to finish; t can be extended to a vector of threads later if necessary
    			t = std::thread(&ReqServer::thread_func, this, buf, size, status.MPI_SOURCE);
    			first = false;
    		}
        }
        if(!first && t.joinable())
            t.join();
    }

    ReqServer()
    {
        main_thread = std::thread(&ReqServer::run, this);
    }

    ~ReqServer()
    {
        if(main_thread.joinable())
            main_thread.join();

        cout << "ReqServer done at rank" << _my_rank << endl;
    }
};

