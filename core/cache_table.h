#pragma once

#include "req_queue.h"
#include "conmap_zero.h"
#include "conmap.h"
#include <memory>
#include <vector>

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

    bool request(KeyT key, KeyT value)
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

    ReqQueue<RequestMsg> q_req;
    CandCacheT candcache;
	PullCache<KeyT> pcache;

    ~CacheTable()
    {
        for(int i=0; i<CONMAP_BUCKET_NUM; i++)
    	{
    		auto & bucket = candcache.pos(i);
    		bucket.lock(); 
    		auto & kvmap = bucket.get_map();
    		for(auto it = kvmap.begin(); it != kvmap.end(); it++)
    		{
    			delete it->second.value; // release cached vertices
    		}
    		bucket.unlock();
    	}
    }

    ValueT * lock_and_get(KeyT key, KeyT value)
    {
    	ValueT * ret;
    	auto & bucket = candcache.get_bucket(key);
    	bucket.lock();
    	auto & kvmap = bucket.get_map();
    	auto it = kvmap.find(key);
    	if(it == kvmap.end())
		{
			bool new_req = pcache.request(key, value);
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
};