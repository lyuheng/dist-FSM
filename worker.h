#pragma once

#include <unistd.h>
#include <algorithm>

#include "comper.h"
#include "communication.h"
#include "serialization.h"

#include "cache_table.h"
#include "cache_gc.h"
#include "resp_server.h"
#include "req_server.h"

#include "setting.h"

struct steal_plan
{
    int id;  // steal from/to which server. negative number means receive
    int num; // steal batch size
    friend obinstream &operator>>(obinstream &m, steal_plan &s)
    {
        m >> s.id;
        m >> s.num;
        return m;
    }
    friend ibinstream &operator<<(ibinstream &m, const steal_plan &s)
    {
        m << s.id;
        m << s.num;
        return m;
    }
};

class Worker
{
public:
    typedef Comper::CacheTableT CacheTableT;
    Comper *compers = nullptr;

    DataStack *data_stack;

    Qlist *activeQ_list;

    CacheTableT *cache_table;

    PendingMap *pending_patterns; // <parent patternID, pending task containers>
    PatternQueue *ready_patterns;

    ReqQueue<RequestMsg, DELETE_CHANNEL> *delete_queue;

    //TODO:
    vector<task_container *> end_delete;

    Worker(int comper_num)
    {
        global_data_stack = data_stack = new DataStack;
        global_activeQ_list = activeQ_list = new Qlist;

        num_compers = comper_num;
        global_end_label = false;

        results_counter.assign(comper_num, 0);
        results_maximum_nodes.assign(comper_num, 2);

        comm_data_size[0] = 0.0f;
        comm_data_size[1] = 0.0f;

        global_cache_table = cache_table = new CacheTableT;

        global_pending_patterns = pending_patterns = new PendingMap;
        global_ready_patterns = ready_patterns = new PatternQueue;

        global_delete_queue = delete_queue = new ReqQueue<RequestMsg, DELETE_CHANNEL>;

        // fout = new ofstream[32];
        // for(int i=0; i<32; i++)
        // {
        //     char file[200];
        //     sprintf(file, "./log_%d", i);
        //     fout[i].open(file);
        // }
    }

    ~Worker()
    {
        if (compers)
            delete[] compers;

        delete activeQ_list;
        delete data_stack;

        delete[] grami.pruned_graph.nlf;

        delete cache_table;
        delete pending_patterns;
        delete ready_patterns;

        delete delete_queue;

        // for(ui i=0; i<32; i++)
        //     fout[i].close();
    }

    void load_data(int support, const string &file_path)
    {
        Graph graph(support);
        grami.nsupport_ = support;

        graph.load_graph(grami.pruned_graph, file_path, " ", num_compers); // separated by space

        initialize_pattern();
    }

    void initialize_pattern()
    {
        int init_res = grami.initialize();
        if (_my_rank == MASTER_RANK)
            results_counter[0] += init_res;

        vector<task_container *> sep_results;

        // vector<PatternMap::iterator> parallel_vec;

        // for (auto it = grami.init_pattern_map.begin(); it != grami.init_pattern_map.end(); ++it)
        // {
        //     parallel_vec.push_back(it);
        // }

        // #pragma omp parallel for schedule(dynamic, 1) num_threads(num_compers)
        for (auto it = grami.init_pattern_map.begin(); it != grami.init_pattern_map.end(); ++it)
        // for (ui i = 0; i < parallel_vec.size(); ++i)
        {
            // auto it = parallel_vec[i];
            // int tid = omp_get_thread_num();

            // cout<<"~~~~~"<<endl;
            // cout << it->second->toString();
            // cout<<"~~~~~"<<endl;

            PatternPVec ext_pattern_vec;

            grami.extend(*(it->second), ext_pattern_vec);

            for (auto it2 = ext_pattern_vec.begin(); it2 != ext_pattern_vec.end(); ++it2)
            {
                if ((*it2)->hash() != _my_rank)
                    continue; // if this pattern isn't supposed be computed by me, then skip

                task_container * new_tc = new task_container(global_qid++);
                new_tc->pattern = *it2;
                new_tc->pattern->non_candidates.resize(new_tc->pattern->size());

                g_pattern_prog_map.insert(new_tc->qid, new_tc->pattern->prog);

                sep_results.push_back(new_tc);
            }

            delete it->second->prog;
            delete it->second;
        }

        // for(ui i = 0; i < num_compers; ++i)
        // {
        //     total += sep_results[i].size();
        //     data_stack->enstack(sep_results[i]);
        // }
        data_stack->enstack(sep_results);

        cout << "In Initialization, size of data_stack: " << sep_results.size() << endl;
    }

    void release_pattern_prog_map()
    {
        for (int i=0; i<CONMAP_BUCKET_NUM; i++)
        {
            auto & bucket = g_pattern_prog_map.pos(i);
            bucket.lock();
            auto & kvmap = bucket.get_map();
            
            for (auto it = kvmap.begin(); it != kvmap.end(); it++)
            {
                // cout << "(" << it->first << ", " << it->second->children_cnt << ")"<< " ";
                delete it->second;
            }
            bucket.unlock();
        }
        // cout << "\n";
    }

    bool steal_planning()
    {
        vector<steal_plan> my_single_steal_list; // if my_single_steal_list[i] = 3, I need to steal a tasks from Worker 3's global queue
        vector<int> my_batch_steal_list;
        int avg_num;

        if (_my_rank != MASTER_RANK)
        {
            send_data(data_stack->size(), MASTER_RANK, STATUS_CHANNEL);
            recv_data<vector<steal_plan>>(MASTER_RANK, STATUS_CHANNEL, my_single_steal_list);
            recv_data<vector<int>>(MASTER_RANK, STATUS_CHANNEL, my_batch_steal_list);
            recv_data<int>(MASTER_RANK, STATUS_CHANNEL, avg_num);
        }
        else
        {
            vector<int> remain_vec(_num_workers);
            for (int i = 0; i < _num_workers; ++i)
            {
                if (i != MASTER_RANK)
                    remain_vec[i] = recv_data<int>(i, STATUS_CHANNEL);
                else
                    remain_vec[i] = data_stack->size();
            }
            priority_queue<max_heap_entry> max_heap;
            priority_queue<min_heap_entry> min_heap;
            avg_num = ceil((float)accumulate(remain_vec.begin(), remain_vec.end(), 0) / remain_vec.size());
            for (int i = 0; i < _num_workers; i++)
            {
                if (remain_vec[i] > avg_num)
                {
                    max_heap_entry en{remain_vec[i], i};
                    max_heap.push(en);
                }
                else if (remain_vec[i] < avg_num)
                {
                    min_heap_entry en{remain_vec[i], i};
                    min_heap.push(en);
                }
            }
            vector<int> steal_num(_num_workers, 0);
            vector<vector<steal_plan>> single_steal_lists(_num_workers);
            vector<vector<int>> batch_steal_lists(_num_workers);
            int total_plans_num = 0;
            while (!max_heap.empty() && !min_heap.empty())
            {
                max_heap_entry max = max_heap.top();
                max_heap.pop();
                min_heap_entry min = min_heap.top();
                min_heap.pop();

                if (avg_num - min.num_remain > MAX_STEAL_PATTERN_NUM && max.num_remain - avg_num > MAX_STEAL_PATTERN_NUM)
                {
                    max.num_remain -= MAX_STEAL_PATTERN_NUM;
                    min.num_remain += MAX_STEAL_PATTERN_NUM;

                    steal_num[min.rank] += MAX_STEAL_PATTERN_NUM;
                    total_plans_num += MAX_STEAL_PATTERN_NUM;
                    // negative tag (-x-1) means receiving
                    batch_steal_lists[min.rank].push_back(-max.rank - 1);
                    batch_steal_lists[max.rank].push_back(min.rank);

                    if (max.num_remain > avg_num)
                        max_heap.push(max);
                    if (steal_num[min.rank] < MAX_STEAL_PATTERN_NUM && min.num_remain < avg_num)
                        min_heap.push(min);
                }
                else
                {
                    int steal_batch = std::min((max.num_remain - avg_num), (avg_num - min.num_remain));
                    max.num_remain -= steal_batch;
                    min.num_remain += steal_batch;
                    steal_num[min.rank] += steal_batch;
                    total_plans_num += steal_batch;

                    single_steal_lists[min.rank].push_back(steal_plan{-max.rank - 1, steal_batch});
                    single_steal_lists[max.rank].push_back(steal_plan{min.rank, steal_batch});

                    if (max.num_remain > avg_num)
                        max_heap.push(max);
                    if (steal_num[min.rank] < MAX_STEAL_PATTERN_NUM && min.num_remain < avg_num)
                        min_heap.push(min);
                }
            }
            if (total_plans_num > 0)
                cout << total_plans_num << " stealing plans generated at the master" << endl;

            for (int i = 0; i < _num_workers; i++)
            {
                if (i == _my_rank)
                {
                    single_steal_lists[i].swap(my_single_steal_list);
                    batch_steal_lists[i].swap(my_batch_steal_list);
                }
                else
                {
                    send_data(single_steal_lists[i], i, STATUS_CHANNEL);
                    send_data(batch_steal_lists[i], i, STATUS_CHANNEL);
                    send_data(avg_num, i, STATUS_CHANNEL);
                }
            }
        }

        if (my_single_steal_list.empty() && my_batch_steal_list.empty())
            return false;

        for (int i = 0; i < my_batch_steal_list.size(); ++i)
        {
            int other = my_batch_steal_list[i];
            if (other < 0)
            {
                vector<task_container *> tc_vec;
                recv_data<vector<task_container *>>(-other - 1, STATUS_CHANNEL, tc_vec);
                if (!tc_vec.empty())
                {
                    data_stack->enstack(tc_vec);
                }
            }
            else
            {
                vector<task_container *> tc_vec;
                data_stack->pop_fronts(tc_vec, MAX_STEAL_PATTERN_NUM, avg_num);
                send_data(tc_vec, other, STATUS_CHANNEL);
                //@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
                // delete every pointer in tc_vec !!!!!!!!!!!!!!!!!
                //@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
                for (auto it = tc_vec.begin(); it != tc_vec.end(); ++it)
                {
                    delete (*it);
                }
            }
        }

        for (int i = 0; i < my_single_steal_list.size(); ++i)
        {
            int other = my_single_steal_list[i].id;
            if (other < 0)
            {
                vector<task_container *> tc_vec;
                recv_data<vector<task_container *>>(-other - 1, STATUS_CHANNEL, tc_vec);
                if (!tc_vec.empty())
                {
                    data_stack->enstack(tc_vec);
                }
            }
            else
            {
                int steal_num = my_single_steal_list[i].num;
                vector<task_container *> tc_vec;
                data_stack->pop_fronts(tc_vec, steal_num, avg_num);
                send_data(tc_vec, other, STATUS_CHANNEL);
                //@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
                // delete every pointer in tc_vec !!!!!!!!!!!!!!!!!
                //@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
                for (auto it = tc_vec.begin(); it != tc_vec.end(); ++it)
                {
                    delete (*it);
                }
            }
        }
        return true;
    }

    void status_sync(bool sth_stealed)
    {
        bool worker_idle = false;

        activeQ_lock.rdlock();
        if (activeQ_num > 0)
        {
            // cout << "Branch A  " << activeQ_num << endl;
            activeQ_lock.unlock();
        }
        else
        {
            // cout << "Branch B" << endl;
            activeQ_lock.unlock();
            if (data_stack->empty() && pending_patterns_num.load(memory_order_relaxed) == 0 && ready_patterns->empty())
            {
                // cout << "Branch C" << endl;
                if (global_num_idle.load(memory_order_relaxed) == num_compers)
                {
                    worker_idle = true;
                }
            }
        }
        worker_idle = worker_idle && !sth_stealed; // nothing to steal and idle

        if (_my_rank != MASTER_RANK)
        {
            send_data(worker_idle, MASTER_RANK, STATUS_CHANNEL);
            bool all_idle = recv_data<bool>(MASTER_RANK, STATUS_CHANNEL);
            if (all_idle)
                global_end_label = true;
        }
        else
        {
            bool all_idle = worker_idle;
            for (int i = 0; i < _num_workers; i++)
            {
                if (i != MASTER_RANK)
                    all_idle = (recv_data<bool>(i, STATUS_CHANNEL) && all_idle);
            }
            if (all_idle)
                global_end_label = true;
            for (int i = 0; i < _num_workers; i++)
            {
                if (i != MASTER_RANK)
                    send_data(all_idle, i, STATUS_CHANNEL);
            }
        }
    }

    void run()
    {
        RespServer<int, vector<Domain>, PendingMap, PatternQueue> server_resp(*cache_table, *pending_patterns, *ready_patterns);
        ReqServer server_req;
        CacheGC<CacheTableT> gc(*cache_table);

        // create compers
        compers = new Comper[num_compers];
        for (int i = 0; i < num_compers; i++)
        {
            compers[i].start(i);
        }

        while (global_end_label == false)
        {
            bool sth_stealed = Settings::useLB ? steal_planning() : false;
            status_sync(sth_stealed);
            mtx_go.lock();
            ready_go = true;
            cv_go.notify_all(); //release threads to compute tasks
            mtx_go.unlock();

            // Avoid busy-checking
            usleep(WAIT_TIME_WHEN_IDLE);
        }

        comm_data_time = server_resp.communication_time;
    }
};
