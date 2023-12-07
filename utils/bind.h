#pragma once

#include <sched.h>
#include <stdlib.h>
#include <unistd.h>
#include <vector>
#include <iostream>
#include <unordered_map>

#include "mpi_global.h"

#include <hwloc.h>

std::vector<std::vector<int>> cpu_topo;
int num_cores = 0;

std::vector<int> default_bindings; // bind to core one-by-one
std::unordered_map<int, int> core_bindings; // user-defined core binding

void dump_node_topo(std::vector<std::vector<int>> & topo)
{
    std::cout << "TOPO: " << topo.size() << " nodes" << std::endl;
    for (int nid = 0; nid < topo.size(); nid++) {
        std::cout << "node " << nid << " cores: ";
        for (int cid = 0; cid < topo[nid].size(); cid++)
            std::cout << topo[nid][cid] << " ";
        std::cout << std::endl;
    }
}

void load_node_topo(void)
{
    hwloc_topology_t topology;

    hwloc_topology_init(&topology);
    hwloc_topology_load(topology);

    // Currently, nnodes may return 0 while the NUMANODEs in cpulist is 1
    // (hwloc think there is actually no numa-node).
    // Fortunately, it can detect the number of processing units (PU) correctly
    // when MT processing is on, the number of PU will be twice as #cores
    int nnodes = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_SOCKET);
    if (nnodes != 0) {
        cpu_topo.resize(nnodes);
        for (int i = 0; i < nnodes; i++) {
            hwloc_obj_t obj = hwloc_get_obj_by_type(topology, HWLOC_OBJ_SOCKET, i);
            hwloc_cpuset_t cpuset = hwloc_bitmap_dup(obj->cpuset);

            unsigned int core = 0;
            hwloc_bitmap_foreach_begin(core, cpuset);
            cpu_topo[i].push_back(core);
            default_bindings.push_back(core);
            hwloc_bitmap_foreach_end();

            hwloc_bitmap_free(cpuset);
        }
    } else {
        cpu_topo.resize(1);
        int nPUs = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
        for (int i = 0; i < nPUs; i++) {
            hwloc_obj_t obj = hwloc_get_obj_by_type(topology, HWLOC_OBJ_PU, i);
            hwloc_cpuset_t cpuset = hwloc_bitmap_dup(obj->cpuset);

            unsigned int core = 0;
            hwloc_bitmap_foreach_begin(core, cpuset);
            cpu_topo[0].push_back(core);
            default_bindings.push_back(core);
            hwloc_bitmap_foreach_end();

            hwloc_bitmap_free(cpuset);
        }
    }

    num_cores = default_bindings.size();

    if (_my_rank == MASTER_RANK)
        dump_node_topo(cpu_topo);
}

bool load_core_binding()
{
    load_node_topo();
    int nnodes = cpu_topo.size(), tid = 0;
    for (int i = 0; i < nnodes; ++i)
    {
        int ncores = cpu_topo[i].size();
        for (int j = 0; j < ncores; ++j)
        {
            core_bindings[tid++] = cpu_topo[i][j];
        }
    }
    return true;
}

/*
 * Bind the current thread to a special core (core number)
 */
void bind_to_core(size_t core)
{
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(core, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) != 0)
        std::cout << "Failed to set affinity (core: " << core << ")" << std::endl;
}

/*
 * Bind the current thread to special cores (mask)
 */
void bind_to_core(cpu_set_t mask)
{
    if (sched_setaffinity(0, sizeof(mask), &mask) != 0)
        std::cout << "Fail to set affinity!" << std::endl;
}

/*
 * Bind the current thread to all of cores
 * It would like unbind to special cores
 * and not return the previous core binding
 */
void bind_to_all()
{
    cpu_set_t mask;
    CPU_ZERO(&mask);
    for (int i = 0; i < default_bindings.size(); i++)
        CPU_SET(default_bindings[i], &mask);

    if (sched_setaffinity(0, sizeof(mask), &mask) != 0)
        std::cout << "Fail to set affinity" << std::endl;
}

/*
 * Return the mask of the current core binding
 */
cpu_set_t get_core_binding()
{
    cpu_set_t mask;
    CPU_ZERO(&mask);
    if (sched_getaffinity(0, sizeof(mask), &mask) != 0)
        std::cout << "Fail to get affinity" << std::endl;
    return mask;
}

/*
 * Unbind the current thread to special cores
 * and return the preivous core binding
 */
cpu_set_t unbind_to_core()
{
    cpu_set_t mask;
    mask = get_core_binding(); // record the current core binding

    bind_to_all();
    return mask;
}