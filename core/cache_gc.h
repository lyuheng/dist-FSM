#pragma once

#include <thread>
#include <vector>
#include "conque.h"
#include "mpi_global.h"
#include "global.h"

template <typename CacheTable>
class CacheGC 
{
public:
    std::thread main_thread;
    thread_counter counter;
    CacheTable & cache_table;

    void run()
    {
        while (global_end_label == false)
        {
            int oversize = global_cache_size - CACHE_LIMIT;
    		if(oversize > 0) cache_table.shrink(oversize, counter);
    		usleep(WAIT_TIME_WHEN_IDLE); //polling frequency
        }
    }

    CacheGC(CacheTable & cache_table): cache_table(cache_table)
    {
        main_thread = std::thread(&CacheGC::run, this);
    }

    ~CacheGC()
    {
        if (main_thread.joinable())
            main_thread.join();
    }
};