#pragma once

#include "req_queue.h"
#include "conmap_zero.h"
#include "conmap.h"
#include "global.h"
#include <memory>
#include <vector>

struct thread_counter
{
    int count;
    thread_counter(): count(0) {}
    void increment()
    {
        count++;
        if(count >= CACHE_COMMIT_FREQ)
        {
            global_cache_size += count;
            count = 0; //clear local counter
        }
    }

    void decrement()
    {
        count--;
        if(count <= -CACHE_COMMIT_FREQ)
        {
            global_cache_size += count;
            count = 0;
        }
    }
};

template <typename ValueT>
struct CandValue
{
    ValueT * value; //note that it is a pointer !!!
    int counter; //lock-counter, bounded by the number of active tasks in a machine
};

template <class KeyT>
class PullCache
{
public:
    typedef std::vector<KeyT> PatternIDVec;
    typedef conmap<KeyT, PatternIDVec> PCache;

    PCache pcache;

    size_t erase(KeyT key, std::vector<KeyT> & qid_collector)
    {
    	auto & bucket = pcache.get_bucket(key);
    	auto & kvmap = bucket.get_map();
    	auto it = kvmap.find(key);
    	assert(it != kvmap.end()); 
    	PatternIDVec & ids = it->second;
    	size_t ret = ids.size();
		assert(ret > 0);
		qid_collector.swap(ids);
		kvmap.erase(it);
        return ret;
    }

    bool request(KeyT key, KeyT value, thread_counter & counter)
    {
    	auto & bucket = pcache.get_bucket(key);
		auto & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
    	if(it != kvmap.end())
    	{
    		PatternIDVec & ids = it->second;
    		ids.push_back(value);
    		return false;
    	}
    	else
    	{
			counter.increment();
    		kvmap[key].push_back(value);
    		return true;
    	}
    }
};

template <typename KeyT, typename ValueT>
class CacheTable
{
public:
    typedef conmap_zero<KeyT, CandValue<ValueT>> CandCacheT;

    ReqQueue<RequestMsg, REQ_CHANNEL> q_req;
    CandCacheT candcache;
	PullCache<KeyT> pcache;
	int pos = 0;

    ~CacheTable()
    {
        for(int i=0; i<CONMAP_BUCKET_NUM; i++)
    	{
    		auto & bucket = candcache.pos(i);
    		bucket.lock(); 
    		auto & kvmap = bucket.get_map();
    		for(auto it = kvmap.begin(); it != kvmap.end(); it++)
    		{
				assert(it->second.counter == 0);
    			delete it->second.value; // release cached vertices
    		}
    		bucket.unlock();
    	}
    }

    ValueT * lock_and_get(KeyT key, KeyT value, thread_counter & counter)
    {
    	ValueT * ret;
    	auto & bucket = candcache.get_bucket(key);
    	bucket.lock();
    	auto & kvmap = bucket.get_map();
    	auto it = kvmap.find(key);
    	if (it == kvmap.end())
		{
			bool new_req = pcache.request(key, value, counter);
			if(new_req)
			{
                q_req.add(RequestMsg{value, key});  // this worker needs value of this key
			}
			ret = NULL;
		}
    	else
    	{
        	CandValue<ValueT> & cpair = it->second;
        	if(cpair.counter == 0)
			{
                bucket.zeros.erase(key); //zero-cache.remove
			}
        	cpair.counter++;
        	ret = cpair.value;
    	}
    	bucket.unlock();
    	return ret;
    }

    ValueT * get(KeyT key)
    {
        auto & bucket = candcache.get_bucket(key);
		bucket.lock();
		auto & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
		assert(it != kvmap.end());
		ValueT * val = it->second.value;
		bucket.unlock();
		return val;
    }

    void insert(KeyT key, ValueT * value, std::vector<KeyT> & qid_collector)
    {
        auto & bucket = candcache.get_bucket(key);
		bucket.lock();
		CandValue<ValueT> cpair;
		cpair.value = value;
		cpair.counter = pcache.erase(key, qid_collector);
		bool inserted = bucket.insert(key, cpair);
		assert(inserted);
		bucket.unlock();
    }

	void unlock(KeyT key)
	{
		auto & bucket = candcache.get_bucket(key);
		bucket.lock();
		auto & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
		assert(it != kvmap.end());
		it->second.counter--;
		if (it->second.counter == 0) 
			bucket.zeros.insert(key); 
    	bucket.unlock();
	}

	bool find_key(KeyT key)
	{
    	auto & bucket = candcache.get_bucket(key);
    	bucket.lock();
    	auto & kvmap = bucket.get_map();
    	auto it = kvmap.find(key);
    	bool ret = (it != kvmap.end());
		bucket.unlock();
		return ret;
	}

	int shrink(int num_to_delete, thread_counter & counter)
	{
		int start_pos = pos;
		while (num_to_delete > 0)
		{
			auto & bucket = candcache.pos(pos);
			bucket.lock();
			auto iter = bucket.zeros.begin();
			while (iter != bucket.zeros.end())
			{
				KeyT key = *iter;
				auto & kvmap = bucket.get_map();
				auto it = kvmap.find(key);
				assert(it != kvmap.end());
				CandValue<ValueT> & cpair = it->second;
				if (cpair.counter == 0) //to make sure item is not locked
				{
					counter.decrement();
					delete cpair.value;
					kvmap.erase(it);
					iter = bucket.zeros.erase(iter); // update iter
					num_to_delete--;
					if(num_to_delete == 0) // there's no need to look at more candidates
					{
						bucket.unlock();
						pos++; //current bucket has been checked, next time, start from next bucket
						return 0;
					}
				}
				else iter++;
			}
			bucket.unlock();
			pos++;
			if(pos >= CONMAP_BUCKET_NUM) pos -= CONMAP_BUCKET_NUM;
			if(pos == start_pos) break;
		}
		return num_to_delete;
	}
};