#pragma once

#include <vector>
#include <thread>

#include "conque.h"
#include "serialization.h"
#include "mpi_global.h"

template <typename T>
class RespQueue
{
public:
    typedef conque<T> Buffer;
    typedef vector<Buffer> Queue;

    Queue q;
    std::thread main_thread;

    void get_msgs(int dst, ibinstream & m)
	{
    	if(dst == _my_rank) return;
    	Buffer & buf = q[dst];
		T temp; //for fetching VertexT items
        bool succ = buf.dequeue(temp);
		while(succ) //fetching till reach list-head
		{
			m << temp;
            succ = buf.dequeue(temp);
		}
	}

    void thread_func()
    {
        int i = 0; //target worker to send
    	bool sth_sent = false; //if sth is sent in one round, set it as true
    	ibinstream* m0 = new ibinstream;
    	ibinstream* m1 = new ibinstream;
    	std::thread t(&RespQueue::get_msgs, this, 0, ref(*m0)); //assisting thread
    	bool use_m0 = true; //tag for alternating
    	while(global_end_label == false) //otherwise, thread terminates
    	{
			t.join();//m0 or m1 becomes ready to send
			int j = i+1;
			if(j == _num_workers) j = 0;
			if(use_m0) //even
			{
				//use m0, set m1
				t = std::thread(&RespQueue::get_msgs, this, j, ref(*m1));
				if(m0->size() > 0)
				{
					sth_sent = true;
					//send reqs to tgt
					MPI_Send(m0->get_buf(), m0->size(), MPI_CHAR, i, RESP_CHANNEL, MPI_COMM_WORLD);
					delete m0;
					m0 = new ibinstream;
				}
				use_m0 = false;
			}
			else
			{
				//use m1, set m0
				t = std::thread(&RespQueue::get_msgs, this, j, ref(*m0));
				if(m1->size() > 0)
				{
					sth_sent = true;
					//send reqs to tgt
					MPI_Send(m1->get_buf(), m1->size(), MPI_CHAR, i, RESP_CHANNEL, MPI_COMM_WORLD);
					//------
					delete m1;
					m1 = new ibinstream;
				}
				use_m0 = true;
			}
			//------------------------
			i = j;
			if(j == 0)
			{
				if(!sth_sent) usleep(WAIT_TIME_WHEN_IDLE);
				else sth_sent = false;
			}
    	}
    	t.join();
		delete m0;
		delete m1;
    }

    void add(T msg, int src)
    {
        Buffer & buf = q[src];
        buf.enqueue(msg);
    }

    RespQueue()
    {
        q.resize(_num_workers);
        main_thread = std::thread(&RespQueue::thread_func, this);
    }

    ~RespQueue()
    {
        if(main_thread.joinable())
            main_thread.join();

		cout << "RespQueue done at rank" << _my_rank << endl;
    }
};