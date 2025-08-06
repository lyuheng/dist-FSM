#include "worker.h"
#include "systemI.h"
#include "setting.h"
#include "mpi_global.h"
#include "communication.h"

#include <numeric>

using namespace std::chrono;

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

    load_core_binding();

    string fileName;
    int support, thread_num = 32;

    // load graph file
	char * argfilename = getCmdOption(argv, argv + argc, "-file");
	if(argfilename)
		fileName = string(argfilename);
    else 
        throw std::runtime_error("Input filename (-file) is empty!");

    // get user-given support threshold
	char * argSupport = getCmdOption(argv, argv + argc, "-freq");
	if(argSupport)
		support = atoi(argSupport);
    else 
        throw std::runtime_error("Input support frequency (-freq) is empty!");

    // parameter to set the maximum subgraph size (in terms of the number of vertices)
	char * argMaxNodes = getCmdOption(argv, argv + argc, "-maxNodes");
	if(argMaxNodes)
		Settings::maxNumNodes = atoi(argMaxNodes);
    
    // get user-given number of threads
    char * argThreads = getCmdOption(argv, argv + argc, "-thread");
	if(argThreads)
		thread_num = atoi(argThreads);

    if (thread_num > core_bindings.size())
    {
        if (_my_rank == MASTER_RANK)
        {
            stringstream iss;
            iss << "Input number of threads " << thread_num << " exceeds number of CPU cores " << core_bindings.size() << "!";
            throw std::runtime_error(iss.str());
        }
    }

    char * argUseLB = getCmdOption(argv, argv + argc, "-lb");
    if(argUseLB)
        Settings::useLB = atoi(argUseLB);

    auto time1 = steady_clock::now();

    grami.nsupport_ = support;
    grami.pruned_graph.nsupport_ = support;
   
    Worker worker(thread_num);

    worker.load_data(support, fileName);

    auto time2 = steady_clock::now();

    worker.run();

    auto time3 = steady_clock::now();

    if (_my_rank == MASTER_RANK)
    {
        ui ttl_result = std::accumulate(results_counter.begin(), results_counter.end(), 0);
        ui ttl_maxsize = *std::max_element(results_maximum_nodes.begin(), results_maximum_nodes.end());;
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
        for (int i=0; i<_num_workers; ++i)
        {
            if (i != MASTER_RANK)
            {
                ui found = recv_data<ui>(i, MAXSIZE_CHANNEL);
                ttl_maxsize = ttl_maxsize > found ? ttl_maxsize : found;
            }
        }

#ifdef COMM_STATS
        float ttl_comm_data_size = comm_data_size[0] + comm_data_size[1];
        double ttl_comm_data_time = comm_data_time;

        float ttl_additional_comm_data_size = additional_comm_size;
        double ttl_additional_comm_data_time = additional_comm_time;

        for (int i=0; i<_num_workers; ++i)
        {
            if (i != MASTER_RANK)
            {
                float comm_size = recv_data<float>(i, COMM_STATS_CHANNEL);
                ttl_comm_data_size += comm_size;

                double comm_time = recv_data<double>(i, COMM_STATS_CHANNEL);
                ttl_comm_data_time += comm_time;

                float additional_comm_size = recv_data<float>(i, COMM_STATS_CHANNEL);
                ttl_additional_comm_data_size += additional_comm_size;

                double additional_comm_time = recv_data<double>(i, COMM_STATS_CHANNEL);
                ttl_additional_comm_data_time += additional_comm_time;
            }
        }
#endif
        cout << "}\n";
        cout << "[TIME] Loading Graph Time: " << (float)duration_cast<milliseconds>(time2 - time1).count() / 1000 << " s" << endl;
        cout << "[TIME] Mining Time: " << (float)duration_cast<milliseconds>(time3 - time2).count() / 1000 << " s" << endl;
        cout << "[TIME] Total Elapsed Time: " << (float)duration_cast<milliseconds>(time3 - time1).count() / 1000 << " s" << endl;
        cout << "[INFO] # Frequent Patterns: " << ttl_result << endl;
        cout << "[INFO] Maximum number of vertices: " << ttl_maxsize << endl;
        
#ifdef COMM_STATS
        cout << "[STAT] Communication Data Size (MB): " << ttl_comm_data_size << endl;
        cout << "[STAT] Average Communication Data Time (sec): " << ttl_comm_data_time / _num_workers  << endl;

        cout << "[STAT] Additional Communication Data Size (MB): " << ttl_additional_comm_data_size << endl;
        cout << "[STAT] Average Additional Communication Data Time (sec): " << ttl_additional_comm_data_time / _num_workers  << endl;
#endif
    }
    else
    {
        for (int i=0; i<_num_workers; ++i)
        {
            if (i != MASTER_RANK)
            {
                send_data(std::accumulate(results_counter.begin(), results_counter.end(), 0), MASTER_RANK, RESULT_CHANNEL);
                send_data(*std::max_element(results_maximum_nodes.begin(), results_maximum_nodes.end()), MASTER_RANK, MAXSIZE_CHANNEL);

#ifdef COMM_STATS
                send_data(comm_data_size[0] + comm_data_size[1], MASTER_RANK, COMM_STATS_CHANNEL);
                send_data(comm_data_time, MASTER_RANK, COMM_STATS_CHANNEL);

                send_data(additional_comm_size, MASTER_RANK, COMM_STATS_CHANNEL);
                send_data(additional_comm_time, MASTER_RANK, COMM_STATS_CHANNEL);
#endif
            }
        }
    }

    // global_end_label_mem = false;
    // t.join();
	// foutPeakMem.close();

    worker.release_pattern_prog_map();

    worker_finalize();
    return 0;
}
