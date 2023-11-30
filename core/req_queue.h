#pragma once

#include <thread>
#include <vector>
#include "conque.h"
#include "mpi_global.h"
#include "global.h"

template <typename KeyT, MPICHANNEL CHANNEL>
class ReqQueue
{
public:
    typedef conque<KeyT> Buffer;
    typedef std::vector<Buffer> Queue;

    Queue q;
    std::thread main_thread;

    void get_msgs(int dst, ibinstream & m)
    {
        if (dst == _my_rank) return;
        Buffer & buf = q[dst];
        KeyT temp;
        bool succ = buf.dequeue(temp);
        while (succ)
        {
            m << temp;
            if(m.size() > MAX_BATCH_SIZE) {
				cout << "m.size() = " << m.size() << endl;
			}
			cout << "send key = " << temp.parent_qid << endl;
			succ = buf.dequeue(temp);
        }
    }

    void thread_func()
    {
        int i = 0; //target worker to send
    	bool sth_sent = false; //if sth is sent in one round, set it as true
    	ibinstream* m0 = new ibinstream;
    	ibinstream* m1 = new ibinstream;
    	//m0 and m1 are alternating
    	std::thread t(&ReqQueue::get_msgs, this, 0, ref(*m0)); //assisting thread
    	bool use_m0 = true; //tag for alternating
        clock_t last_tick = clock();
    	while(global_end_label == false) //otherwise, thread terminates
    	{
    		t.join(); //m0 or m1 becomes ready to send
    		int j = i+1;
    		if(j == _num_workers) j = 0;
    		if(use_m0) //even
    		{
    			//use m0, set m1
    			t = thread(&ReqQueue::get_msgs, this, j, ref(*m1));
				if(m0->size() > 0)
				{
					sth_sent = true;
					//send reqs to tgt
					MPI_Send(m0->get_buf(), m0->size(), MPI_CHAR, i, CHANNEL, MPI_COMM_WORLD);
					//------
					delete m0;
					m0 = new ibinstream;
				}
				use_m0 = false;
    		}
    		else //odd
    		{
    			//use m1, set m0
    			t = thread(&ReqQueue::get_msgs, this, j, ref(*m0));
				if(m1->size() > 0)
				{
					sth_sent = true;
					//send reqs to tgt
					MPI_Send(m1->get_buf(), m1->size(), MPI_CHAR, i, CHANNEL, MPI_COMM_WORLD);
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

	void add(KeyT key)
    {
    	int tgt = GET_WORKER_ID(key.parent_qid);
    	Buffer & buf = q[tgt];
    	buf.enqueue(key);
		cout << "enqueue once key.pqid = " << key.parent_qid << endl;
    }
    
    ReqQueue()
    {
    	q.resize(_num_workers);
    	main_thread = std::thread(&ReqQueue::thread_func, this);
    }

    ~ReqQueue()
    {  
        if(main_thread.joinable())
    	    main_thread.join();
		cout << "ReqQueue done at rank" << _my_rank << endl;
    }
};