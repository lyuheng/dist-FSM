#ifndef CONSTACK_H
#define CONSTACK_H

#include <mutex>
#include <deque>
#include <vector>

using namespace std;

/**
 *      ------------------------
 * <--- front                   <--- back     
 *      ------------------------
 */

template <typename T> 
class constack
{
public:
    deque<T> s;
    mutex mtx;

    void enstack(T & value) 
    {
        mtx.lock();
        s.push_back(value);
        mtx.unlock();
    }

    void enstack(vector<T> & val_vec)
    {
        mtx.lock();
        for(T &el: val_vec)
        {
            s.push_back(el);
        }
        mtx.unlock();
    }

    bool destack(T & to_get) 
    {
        mtx.lock();
        if(!s.empty())
        {
            to_get = s.back();
            s.pop_back();
            mtx.unlock();
            return true;
        }
        else
        {
            mtx.unlock();
            return false;
        }
    }

    bool pop_fronts(vector<T> &to_gets, int count, int above) 
    {
        mtx.lock();
        if(size() >= count + above)
        {
            for (int i=0; i<count; ++i)
            {
                to_gets.push_back(s.front());
                s.pop_front();
            }
            mtx.unlock();
            return true;
        }
        else
        {
            mtx.unlock();
            return false;
        }
    }

    bool empty()
    {
    	mtx.lock();
        bool ret = s.empty();
        mtx.unlock();
        return ret;
    }

    int size()
    {
        mtx.lock();
        int ret = s.size();
        mtx.unlock();
        return ret;
    }

    
};

#endif
