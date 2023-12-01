//########################################################################
//## Copyright 2022 Da Yan http://www.cs.uab.edu/yanda
//##
//## Licensed under the Apache License, Version 2.0 (the "License");
//## you may not use this file except in compliance with the License.
//## You may obtain a copy of the License at
//##
//## //http://www.apache.org/licenses/LICENSE-2.0
//##
//## Unless required by applicable law or agreed to in writing, software
//## distributed under the License is distributed on an "AS IS" BASIS,
//## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//## See the License for the specific language governing permissions and
//## limitations under the License.
//########################################################################

#ifndef CONMAP_ZERO_H
#define CONMAP_ZERO_H

#define CONMAP_BUCKET_NUM 100 //should be proportional to the number of threads on a machine

//idea: 2-level hashing
//1. id % CONMAP_BUCKET_NUM -> bucket_index
//2. bucket[bucket_index] -> give id, get content

//now, we can dump zero-cache, since GC can directly scan buckets one by one

#include <vector>
#include <ext/hash_map>
#include <ext/hash_set>
#include <mutex>
#include <cassert>
#include <bitset>
#include <iostream>
#include <unordered_set>

#define hash_map __gnu_cxx::hash_map
#define hash_set __gnu_cxx::hash_set
using namespace std;

template <typename K, typename V> 
struct conmap_bucket_zero
{
	typedef hash_map<K, V> KVMap;
	typedef unordered_set<K> KSet;
	mutex mtx;
	KVMap bucket;
    KSet zeros;

	void lock()
	{
		mtx.lock();
	}

	void unlock()
	{
		mtx.unlock();
	}

	KVMap & get_map()
	{
		return bucket;
	}

    KSet & get_zero_set()
    {
        return zeros;
    }

	//returns true if inserted
	//false if an entry with this key already exists
	bool insert(K key, V val)
	{
		auto ret = bucket.insert(
			std::pair<K, V>(key, val)
		);
		return ret.second;
	}

	//returns whether deletion is successful
	bool erase(K key)
	{
		size_t num_erased = bucket.erase(key);
		return (num_erased == 1);
	}

	bool empty()
	{
		return bucket.empty();
	}
};

template <typename K, typename V> 
struct conmap_zero
{
public:
	typedef conmap_bucket_zero<K, V> bucket;
	typedef hash_map<K, V> bucket_map;
	bucket* buckets;

	conmap_zero()
	{
		buckets = new bucket[CONMAP_BUCKET_NUM];
	}

	~conmap_zero()
	{
		delete[] buckets;
	}

	bucket & get_bucket(K key)
	{
		return buckets[key % CONMAP_BUCKET_NUM];
	}

	bucket & pos(size_t pos)
	{
		return buckets[pos];
	}

	bool insert(K key, V value)
	{
		bucket & buck = get_bucket(key);
		buck.lock();
		bool ret = buck.insert(key, value);
		assert(ret);
		buck.unlock();
		return ret;
	}

	bool erase(K key)
	{
		bucket & buck = get_bucket(key);
		buck.lock();
		bool ret = buck.erase(key);
		assert(ret);
		buck.unlock();
		return ret;
	}

	bool empty()
	{
		for (int i=0; i<CONMAP_BUCKET_NUM; ++i)
		{
			if (!pos(i).empty()) 
				return false;
		}
		return true;
	}
};

#endif
