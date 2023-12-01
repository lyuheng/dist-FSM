#include "worker.h"
// #include "systemI.h"
#include "setting.h"
#include "mpi_global.h"
#include "communication.h"

#include <numeric>

using namespace std::chrono;

int Settings::maxNumNodes;

int main(int argc, char *argv[])
{   
    // launch a thread to record memory

    // char outputfilePeakMem[1000];
    // sprintf(outputfilePeakMem, "maxmem.txt");
    // ofstream foutPeakMem(outputfilePeakMem);
    // GetCurrentPid();
	// thread t = thread(info, GetCurrentPid(), ref(foutPeakMem));

    init_worker(&argc, &argv);

    cout << "Rank: " << _my_rank << endl; 

    string fileName;
    int support, thread_num = 32;

    // load graph file
	char * argfilename = getCmdOption(argv, argv + argc, "-file");
	if(argfilename)
		fileName = string(argfilename);

    // get user-given support threshold
	char * argSupport = getCmdOption(argv, argv + argc, "-freq");
	if(argSupport)
		support = atoi(argSupport);

    // parameter to set the maximum subgraph size (in terms of the number of vertices)
	char * argMaxNodes = getCmdOption(argv, argv + argc, "-maxNodes");
	if(argMaxNodes)
		Settings::maxNumNodes = atoi(argMaxNodes);
	else
		Settings::maxNumNodes = -1;
    
    // get user-given number of threads
    char * argThreads = getCmdOption(argv, argv + argc, "-thread");
	if(argThreads)
		thread_num = atoi(argThreads);

    auto time1 = steady_clock::now();

    grami.nsupport_ = support;
    grami.pruned_graph.nsupport_ = support;
   
    Worker worker(thread_num);

    worker.load_data(support, argv[2]);

    auto time2 = steady_clock::now();

    worker.run();

    auto time3 = steady_clock::now();

    if (_my_rank == MASTER_RANK)
    {
        ui ttl_result = std::accumulate(results_counter.begin(), results_counter.end(), 0);
        cout << "Results by each worker: {" << ttl_result; 
        for (int i=0; i<_num_workers; ++i)
        {
            if (i != MASTER_RANK)
            {
                ui found = recv_data<ui>(i, RESULT_CHANNEL);
                cout << ", " << found;
                ttl_result += found;
            }
        }
        cout << "}\n";
        cout << "[TIME] Loading Graph Time: " << (float)duration_cast<milliseconds>(time2 - time1).count() / 1000 << " s" << endl;
        cout << "[TIME] Mining Time: " << (float)duration_cast<milliseconds>(time3 - time2).count() / 1000 << " s" << endl;
        cout << "[TIME] Total Elapsed Time: " << (float)duration_cast<milliseconds>(time3 - time1).count() / 1000 << " s" << endl;
        cout << "[INFO] # Frequent Patterns: " << ttl_result << endl;
    }
    else
    {
        for (int i=0; i<_num_workers; ++i)
        {
            if (i != MASTER_RANK)
                send_data(std::accumulate(results_counter.begin(), results_counter.end(), 0), MASTER_RANK, RESULT_CHANNEL);
        }
    }
    // global_end_label_mem = false;
    // t.join();
	// foutPeakMem.close();

    deletion_phase = true;
        
    for(int i=0; i<CONMAP_BUCKET_NUM; i++)
    {
        auto & bucket = g_pattern_prog_map.pos(i);
        bucket.lock();
        auto & kvmap = bucket.get_map();
        
        for(auto it = kvmap.begin(); it != kvmap.end(); it++)
        {
            cout << "(" << it->first << ", " << it->second->children_cnt << ")"<< endl;
        }
        bucket.unlock();
    }

    worker_finalize();

    return 0;
}
