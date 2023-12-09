#pragma once

#include <string>
#include <vector>
#include <cassert>
#include <iostream>
#include <sys/time.h>
#include <cstdlib>
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <set>
#include <queue>
#include <utility>
#include <fstream>
#include <algorithm>
#include <cstring>
#include <limits>
#include <cmath>

#define LABEL_SIZE 10

typedef unsigned int uintV;
typedef unsigned long long uintE;
typedef unsigned int ui;
typedef unsigned long long ull;
typedef int labelType;

long unfind(std::vector<long> &un_father, long u)
{
    return un_father[u] == u ? u : (un_father[u] = unfind(un_father, un_father[u]));
}

void ununion(std::vector<long> &un_father, long u, long v)
{
    un_father[u] = v;
}


class Timer {
 public:
  Timer() {}
  ~Timer() {}

  void StartTimer() { start_timestamp_ = wtime(); }
  void EndTimer() { end_timestamp_ = wtime(); }

  double GetElapsedMicroSeconds() const { return end_timestamp_ - start_timestamp_; }

  inline void PrintElapsedMicroSeconds(const std::string& time_tag) const {
    std::cout << std::fixed << "finish " << time_tag << ", elapsed_time=" << (end_timestamp_ - start_timestamp_) / 1000.0 << "ms" << std::endl;
  }

  double wtime() {
    double time[2];
    struct timeval time1;
    gettimeofday(&time1, NULL);

    time[0] = time1.tv_sec;
    time[1] = time1.tv_usec;

    return time[0] * (1.0e6) + time[1];
  }

 private:
  double start_timestamp_;
  double end_timestamp_;
};


class Graph
{
public:
    Graph() {}
    Graph(const std::string &filename);
    Graph(const std::string &filename, const std::string & label_filename);

    virtual ~Graph() {}

    size_t GetEdgeCount() const { return edge_count_; }
    size_t GetVertexCount() const { return vertex_count_; }
    void SetVertexCount(size_t vertex_count) { vertex_count_ = vertex_count; }
    void SetEdgeCount(size_t edge_count) { edge_count_ = edge_count; }

    uintE *GetRowPtrs() const { return row_ptrs_; }
    uintV *GetCols() const { return cols_; }

    void SetRowPtrs(uintE *row_ptrs) { row_ptrs_ = row_ptrs; }
    void SetCols(uintV *cols) { cols_ = cols; }

    void GetMaxDegree();

    void readBinFile(const std::string &filename);
    void readGraphFile(const std::string &filename);
    void readSnapFile(const std::string &filename);
    void readSnapFileComma(const std::string &filename, const std::string &label_filename);
    void readSnapFile(const std::string &filename, const std::string &label_filename);
    void readLVIDFile(const std::string &filename);

    void writeBinFile(const std::string &filename);
    void writeGraphFile(const std::string &filename);
    void writeDistGraphFile(const std::string &filename);

    void Preprocess();

    void ReMapVertexId();

    /** Only used by DistGraph */
    void FennelPartition(const std::string &filename, int part_num, int hop);

private:
    uintE *row_ptrs_;
    uintV *cols_;
    size_t vertex_count_;
    size_t edge_count_;

    std::unordered_map<uintV, labelType> label_map_;
    std::unordered_set<labelType> label_set_;
    labelType * new_labels_ = nullptr;
};

Graph::Graph(const std::string &filename)
{
    std::string suffix = filename.substr(filename.rfind(".") + 1);
    if (suffix == "bin")
        readBinFile(filename);
    else if (suffix == "graph" || suffix == "lg")
        readGraphFile(filename);
    else if (suffix == "txt" || suffix == "edges")
        readSnapFile(filename);
    else if (suffix == "lvid")
        readLVIDFile(filename);
    else {
        std::cout << "Cannot read graph file based on its suffix ..." << std::endl;
        assert(false);
    }
}

Graph::Graph(const std::string &filename, const std::string &label_filename)
{
    std::string suffix = filename.substr(filename.rfind(".") + 1);
    if (suffix == "txt")
        // readSnapFile(filename, label_filename);
        readSnapFileComma(filename, label_filename);
    else {
        std::cout << "Cannot read graph file based on its suffix ..." << std::endl;
        assert(false);
    }
}


void Graph::GetMaxDegree()
{
    ull max_deg = 0;
    for (size_t i = 0; i < vertex_count_; ++i)
    {
        max_deg = std::max(max_deg, row_ptrs_[i + 1] - row_ptrs_[i]);
    }
    std::cout << "max degree=" << max_deg << std::endl;
}

void Graph::ReMapVertexId()
{
    // 1. find the root vertex with largest degree
    size_t max_degree = 0;
    uintV root = 0;
    for (uintV i = 0; i < vertex_count_; i++) {
        if (row_ptrs_[i + 1] - row_ptrs_[i] > max_degree) {
            max_degree = row_ptrs_[i + 1] - row_ptrs_[i];
            root = i;
        }
    }
    // 2. bfs from the root vertex, make sure connected
    //    order: higher degree, more connections to the visited vertices
    std::queue<uintV> queue;
    std::vector<bool> visited(vertex_count_, false);
    queue.push(root);
    visited[root] = true;
    uintV new_vid = 0;
    std::vector<uintV> old_to_new(vertex_count_);
    std::vector<uintV> new_to_old(vertex_count_);
    uintE *new_row_ptrs_ = new uintE[vertex_count_ + 1];
    uintV *new_cols_ = new uintV[edge_count_];

    while (!queue.empty()) {
        size_t size = queue.size();
        std::vector<uintV> same_level_vertices;
        for (size_t i = 0; i < size; i++) {
            uintV front = queue.front();
            same_level_vertices.push_back(front);
            queue.pop();
            for (size_t j = row_ptrs_[front]; j < row_ptrs_[front + 1]; ++j)
            {  
                uintV ne = cols_[j];
                if (!visited[ne]) {
                    visited[ne] = true;
                    queue.push(ne);
                }
            }
        }
        std::vector<std::tuple<size_t, size_t, uintV>> weights;  // degree, connections, vid
        for (size_t i = 0; i < size; i++) {
            uintV v = same_level_vertices[i];
            size_t connections = 0;
            for (size_t j = row_ptrs_[v]; j < row_ptrs_[v + 1]; ++j)
            {
                uintV ne = cols_[j];
                if (visited[ne])
                    connections++;
            }
            weights.emplace_back(row_ptrs_[v + 1] - row_ptrs_[v], connections, v);
        }
        std::sort(weights.begin(), weights.end(), [](const auto& a, const auto& b) {
            if (std::get<0>(a) != std::get<0>(b))
                return std::get<0>(a) > std::get<0>(b);
            else if (std::get<1>(a) != std::get<1>(b))
                return std::get<1>(a) > std::get<1>(b);
            else if (std::get<2>(a) != std::get<2>(b))
                return std::get<2>(a) < std::get<2>(b);
            return false;
        });
        for (const auto& w : weights) {
            old_to_new[std::get<2>(w)] = new_vid;
            new_to_old[new_vid] = std::get<2>(w);
            new_vid++;
        }
    }
    auto offsets = new uintE[vertex_count_ + 1];
    memset(offsets, 0, sizeof(uintE) * (vertex_count_ + 1));

    for (size_t i = 0; i < vertex_count_; ++i)
    {
        offsets[i] = row_ptrs_[new_to_old[i] + 1] - row_ptrs_[new_to_old[i]];
    }
    uintE prefix = 0;
    for (size_t i = 0; i < vertex_count_ + 1; ++i) {
        new_row_ptrs_[i] = prefix;
        prefix += offsets[i];
        offsets[i] = new_row_ptrs_[i];
    }
    for (size_t i = 0; i < vertex_count_; ++i)
    {
        for (size_t j = row_ptrs_[i]; j < row_ptrs_[i + 1] ; ++j)
        {
            uintV ne = cols_[j];
            new_cols_[offsets[old_to_new[i]]++] = old_to_new[ne];
        }
    }
    for (uintV u = 0; u < vertex_count_; ++u) {
        std::sort(new_cols_ + new_row_ptrs_[u], new_cols_ + new_row_ptrs_[u + 1]);
    }
    delete[] offsets;
    delete[] row_ptrs_;
    delete[] cols_;
    row_ptrs_ = new_row_ptrs_;
    cols_ = new_cols_;
}

void Graph::readSnapFile(const std::string &filename)
{
    Timer timer;
    timer.StartTimer();

    vertex_count_ = 0;
    edge_count_ = 0;
    row_ptrs_ = NULL;
    cols_ = NULL;

    uintV min_vertex_id = std::numeric_limits<uintV>::max();
    uintV max_vertex_id = std::numeric_limits<uintV>::min();
    std::ifstream file(filename.c_str(), std::fstream::in);
    std::string line;
    uintV vids[2];
    while (getline(file, line)) {
        if (line.length() == 0 || !std::isdigit(line[0]))
            continue;
        std::istringstream iss(line);
        for (int i = 0; i < 2; ++i) {
            iss >> vids[i];
            min_vertex_id = std::min(min_vertex_id, vids[i]);
            max_vertex_id = std::max(max_vertex_id, vids[i]);
        }
        edge_count_++;
    }
    file.close();

    vertex_count_ = max_vertex_id - min_vertex_id + 1;
    edge_count_ *= 2;
    std::cout << "vertex_count=" << vertex_count_ << ", edge_count=" << edge_count_ << std::endl;

    row_ptrs_ = new uintE[vertex_count_ + 1];
    cols_ = new uintV[edge_count_];
    auto offsets = new uintE[vertex_count_ + 1];
    memset(offsets, 0, sizeof(uintE) * (vertex_count_ + 1));
    
    {
        std::ifstream file(filename.c_str(), std::fstream::in);
        std::string line;
        uintV vids[2];
        while (getline(file, line)) {
            if (line.length() == 0 || !std::isdigit(line[0]))
                continue;
            std::istringstream iss(line);
            for (int i = 0; i < 2; ++i)
            {
                iss >> vids[i];
                vids[i] -= min_vertex_id;
            }
            offsets[vids[0]]++;
            offsets[vids[1]]++;
        }
        file.close();
    }
    
    uintE prefix = 0;
    for (size_t i = 0; i < vertex_count_ + 1; ++i) {
        row_ptrs_[i] = prefix;
        prefix += offsets[i];
        offsets[i] = row_ptrs_[i];
    }

    {
        std::ifstream file(filename.c_str(), std::fstream::in);
        std::string line;
        uintV vids[2];
        while (getline(file, line)) {
            if (line.length() == 0 || !std::isdigit(line[0]))
                continue;
            std::istringstream iss(line);
            for (int i = 0; i < 2; ++i)
            {
                iss >> vids[i];
                vids[i] -= min_vertex_id;
            }
            cols_[offsets[vids[0]]++] = vids[1];
            cols_[offsets[vids[1]]++] = vids[0];
        }
        file.close();
    }
    delete[] offsets;
    offsets = NULL;

    for (uintV u = 0; u < vertex_count_; ++u) {
        std::sort(cols_ + row_ptrs_[u], cols_ + row_ptrs_[u + 1]);
    }

    timer.EndTimer();
    timer.PrintElapsedMicroSeconds("reading CSR Snap file");
}

void Graph::readSnapFile(const std::string &filename, const std::string &label_filename)
{
    Timer timer;
    timer.StartTimer();

    vertex_count_ = 0;
    edge_count_ = 0;
    row_ptrs_ = NULL;
    cols_ = NULL;

    uintV min_vertex_id = std::numeric_limits<uintV>::max();
    uintV max_vertex_id = std::numeric_limits<uintV>::min();
    std::ifstream file(filename.c_str(), std::fstream::in);
    std::string line;
    uintV vids[2];
    while (getline(file, line)) {
        if (line.length() == 0 || !std::isdigit(line[0]))
            continue;
        std::istringstream iss(line);
        for (int i = 0; i < 2; ++i) {
            iss >> vids[i];
            min_vertex_id = std::min(min_vertex_id, vids[i]);
            max_vertex_id = std::max(max_vertex_id, vids[i]);
        }
        edge_count_++;
    }
    file.close();

    vertex_count_ = max_vertex_id - min_vertex_id + 1;
    edge_count_ *= 2;
    std::cout << "vertex_count=" << vertex_count_ << ", edge_count=" << edge_count_ << std::endl;

    row_ptrs_ = new uintE[vertex_count_ + 1];
    cols_ = new uintV[edge_count_];
    auto offsets = new uintE[vertex_count_ + 1];
    memset(offsets, 0, sizeof(uintE) * (vertex_count_ + 1));

    {
        std::ifstream file(label_filename.c_str(), std::fstream::in);
        std::string line;
        uintV vid;
        labelType label;
        int nline = 1;
        while (getline(file, line)) {
            if (line.length() == 0 || !std::isdigit(line[0]))
                continue;
            std::istringstream iss(line);
            iss >> vid;
            iss >> label;
            label_map_[vid - min_vertex_id] = label;
            label_set_.insert(label);
            nline ++;
        }
        file.close();
    }
    
    {
        std::ifstream file(filename.c_str(), std::fstream::in);
        std::string line;
        uintV vids[2];
        while (getline(file, line)) {
            if (line.length() == 0 || !std::isdigit(line[0]))
                continue;
            std::istringstream iss(line);
            for (int i = 0; i < 2; ++i)
            {
                iss >> vids[i];
                vids[i] -= min_vertex_id;
            }
            offsets[vids[0]]++;
            offsets[vids[1]]++;
        }
        file.close();
    }
    
    uintE prefix = 0;
    for (size_t i = 0; i < vertex_count_ + 1; ++i) {
        row_ptrs_[i] = prefix;
        prefix += offsets[i];
        offsets[i] = row_ptrs_[i];
    }

    {
        std::ifstream file(filename.c_str(), std::fstream::in);
        std::string line;
        uintV vids[2];
        while (getline(file, line)) {
            if (line.length() == 0 || !std::isdigit(line[0]))
                continue;
            std::istringstream iss(line);
            for (int i = 0; i < 2; ++i)
            {
                iss >> vids[i];
                vids[i] -= min_vertex_id;
            }
            cols_[offsets[vids[0]]++] = vids[1];
            cols_[offsets[vids[1]]++] = vids[0];
        }
        file.close();
    }
    delete[] offsets;
    offsets = NULL;

    for (uintV u = 0; u < vertex_count_; ++u) {
        std::sort(cols_ + row_ptrs_[u], cols_ + row_ptrs_[u + 1]);
    }

    timer.EndTimer();
    timer.PrintElapsedMicroSeconds("reading CSR Snap file");
}


void Graph::readSnapFileComma(const std::string &filename, const std::string &label_filename)
{
    Timer timer;
    timer.StartTimer();

    vertex_count_ = 0;
    edge_count_ = 0;
    row_ptrs_ = NULL;
    cols_ = NULL;

    uintV min_vertex_id = std::numeric_limits<uintV>::max();
    uintV max_vertex_id = std::numeric_limits<uintV>::min();
    std::ifstream file(filename.c_str(), std::fstream::in);
    std::string line;
    uintV vids[2];
    while (getline(file, line)) {
        // if (line.length() == 0 || !std::isdigit(line[0]))
            // continue;
        std::istringstream iss(line);
        // for (int i = 0; i < 2; ++i) {
        //     iss >> vids[i];
        //     min_vertex_id = std::min(min_vertex_id, vids[i]);
        //     max_vertex_id = std::max(max_vertex_id, vids[i]);
        // }

        iss >> vids[0];
        min_vertex_id = std::min(min_vertex_id, vids[0]);
        max_vertex_id = std::max(max_vertex_id, vids[0]);
        char ch;
        iss >> ch;
        if (ch != ',') assert(false);
        iss >> vids[1];
        min_vertex_id = std::min(min_vertex_id, vids[1]);
        max_vertex_id = std::max(max_vertex_id, vids[1]);

        // std::cout << vids[0] << " " << vids[1] << " ";

        edge_count_++;
    }
    file.close();

    vertex_count_ = max_vertex_id - min_vertex_id + 1;
    edge_count_ *= 2;
    std::cout << "vertex_count=" << vertex_count_ << ", edge_count=" << edge_count_ << std::endl;

    row_ptrs_ = new uintE[vertex_count_ + 1];
    cols_ = new uintV[edge_count_];
    auto offsets = new uintE[vertex_count_ + 1];
    memset(offsets, 0, sizeof(uintE) * (vertex_count_ + 1));

    {
        std::ifstream file(label_filename.c_str(), std::fstream::in);
        std::string line;
        uintV vid;
        labelType label;
        int nline = 0;
        char ch;
        while (getline(file, line)) {
            if (line.length() == 0 || !std::isdigit(line[0]))
                continue;
            std::istringstream iss(line);
            iss >> label;
            // iss >> ch;
            // if (ch != ',') assert(false);
            label_map_[nline++] = label;
            label_set_.insert(label);
        }
        file.close();
    }
    
    {
        std::ifstream file(filename.c_str(), std::fstream::in);
        std::string line;
        uintV vids[2];
        while (getline(file, line)) {
            // if (line.length() == 0 || !std::isdigit(line[0]))
            //     continue;
            std::istringstream iss(line);
            // for (int i = 0; i < 2; ++i)
            {
                iss >> vids[0];
                vids[0] -= min_vertex_id;
                char ch;
                iss >> ch;
                if (ch != ',') assert(false);
                iss >> vids[1];
                vids[1] -= min_vertex_id;
            }
            offsets[vids[0]]++;
            offsets[vids[1]]++;
        }
        file.close();
    }
    
    uintE prefix = 0;
    for (size_t i = 0; i < vertex_count_ + 1; ++i) {
        row_ptrs_[i] = prefix;
        prefix += offsets[i];
        offsets[i] = row_ptrs_[i];
    }

    {
        std::ifstream file(filename.c_str(), std::fstream::in);
        std::string line;
        uintV vids[2];
        while (getline(file, line)) {
            // if (line.length() == 0 || !std::isdigit(line[0]))
            //     continue;
            std::istringstream iss(line);
            // for (int i = 0; i < 2; ++i)
            {
                // iss >> vids[i];
                // vids[i] -= min_vertex_id;

                iss >> vids[0];
                vids[0] -= min_vertex_id;
                char ch;
                iss >> ch;
                if (ch != ',') assert(false);
                iss >> vids[1];
                vids[1] -= min_vertex_id;
            }
            cols_[offsets[vids[0]]++] = vids[1];
            cols_[offsets[vids[1]]++] = vids[0];
        }
        file.close();
    }
    delete[] offsets;
    offsets = NULL;

    for (uintV u = 0; u < vertex_count_; ++u) {
        std::sort(cols_ + row_ptrs_[u], cols_ + row_ptrs_[u + 1]);
    }

    timer.EndTimer();
    timer.PrintElapsedMicroSeconds("reading CSR Snap file");
}

void Graph::readBinFile(const std::string &filename)
{
    vertex_count_ = 0;
    edge_count_ = 0;
    row_ptrs_ = NULL;
    cols_ = NULL;

    Timer timer;
    timer.StartTimer();
    std::cout << "start reading CSR bin file...." << std::endl;
    FILE* file_in = fopen(filename.c_str(), "rb");
    assert(file_in != NULL);
    fseek(file_in, 0, SEEK_SET);
    size_t res = 0;
    size_t uintV_size = 0, uintE_size = 0;
    res += fread(&uintV_size, sizeof(size_t), 1, file_in);
    res += fread(&uintE_size, sizeof(size_t), 1, file_in);
    res += fread(&vertex_count_, sizeof(size_t), 1, file_in);
    res += fread(&edge_count_, sizeof(size_t), 1, file_in);
    assert(uintV_size == sizeof(uintV));
    assert(uintE_size == sizeof(uintE));
    std::cout << "vertex_count=" << vertex_count_ << ", edge_count=" << edge_count_ << std::endl;

    row_ptrs_ = new uintE[vertex_count_ + 1];
    cols_ = new uintV[edge_count_];
    res += fread(row_ptrs_, sizeof(uintE), vertex_count_ + 1, file_in);
    res += fread(cols_, sizeof(uintV), edge_count_, file_in);

    assert(res == 4 + (vertex_count_ + 1) + edge_count_);

    GetMaxDegree();

    fgetc(file_in);
    assert(feof(file_in));
    fclose(file_in);

    timer.EndTimer();
    timer.PrintElapsedMicroSeconds("reading CSR bin file");  
}

void Graph::readGraphFile(const std::string &filename)
{
    vertex_count_ = 0;
    edge_count_ = 0;
    row_ptrs_ = NULL;
    cols_ = NULL;

    std::ifstream file_in(filename);
    if (!file_in.is_open()) 
    {
        std::cout << "Unable to read the graph " << filename << std::endl;
    }

    char type;
    file_in >> type >> vertex_count_ >> edge_count_;
    edge_count_ *= 2;
    std::cout << "vertex_count=" << vertex_count_ << ", edge_count=" << edge_count_ << std::endl;

    row_ptrs_ = new uintE[vertex_count_ + 1];
    cols_ = new uintV[edge_count_];
    row_ptrs_[0] = 0;

    std::vector<uintV> neighbors_offset(vertex_count_, 0);
    
    while (file_in >> type) {
        if (type == 'v') { // Read vertex.
            ui id;
            labelType label;
            ui degree;
            file_in >> id >> label >> degree;
            row_ptrs_[id + 1] = row_ptrs_[id] + degree;

            label_map_[id] = label;
        }
        else if (type == 'e') { // Read edge.
            uintV begin;
            uintV end;
            labelType label;
            file_in >> begin >> end >> label;

            uintV offset = row_ptrs_[begin] + neighbors_offset[begin];
            cols_[offset] = end;
            offset = row_ptrs_[end] + neighbors_offset[end];
            cols_[offset] = begin;
            neighbors_offset[begin] += 1;
            neighbors_offset[end] += 1;
        }
    }

    file_in.close();

    for (ui i = 0; i < vertex_count_; ++i) {
        std::sort(cols_ + row_ptrs_[i], cols_ + row_ptrs_[i + 1]);
    }
}

void Graph::readLVIDFile(const std::string &filename)
{
    vertex_count_ = 0;
    edge_count_ = 0;
    row_ptrs_ = NULL;
    cols_ = NULL;

    std::cout << "start build csr..." << std::endl;
    // const char* kDelimiters = " ,;\t";
    const char* kDelimiters = "0123456789";
    std::unordered_map<std::string, uintV> ids;
    std::vector<uintV> edge_pairs;
    {
        std::ifstream file(filename.c_str(), std::fstream::in);
        std::string line;
        while (getline(file, line)) {
            if (line.length() == 0 || !std::isdigit(line[0]))
                continue;

            std::vector<std::string> num_strs;
            size_t cur_pos = 0;
            while (cur_pos < line.length()) {
                cur_pos = line.find_first_of(kDelimiters, cur_pos, strlen(kDelimiters));
                if (cur_pos < line.length()) {
                    size_t next_pos = line.find_first_not_of(kDelimiters, cur_pos, strlen(kDelimiters));
                    num_strs.push_back(line.substr(cur_pos, next_pos - cur_pos));
                    assert(next_pos > cur_pos);
                    cur_pos = next_pos;
                }
            }

            for (auto& str : num_strs) {
                assert(str.length());
                for (auto ch : str) {
                    assert(std::isdigit(ch));
                }
            }

            for (auto& str : num_strs) {
                if (ids.find(str) == ids.end()) {
                    ids.insert(std::make_pair(str, vertex_count_++));
                }
                edge_pairs.push_back(ids[str]);
            }
        }
        file.close();
    }
    ids.clear();

    std::cout << "edge pairs size=" << edge_pairs.size() << std::endl;
    assert(edge_pairs.size() % 2 == 0);
    edge_count_ = edge_pairs.size(); // / 2;

    std::vector<uintE> offsets(vertex_count_ + 1, 0);
    for (size_t i = 0; i < edge_pairs.size(); i += 2) {
        offsets[edge_pairs[i]]++;
        offsets[edge_pairs[i + 1]]++;
    }

    row_ptrs_ = new uintE[vertex_count_ + 1];
    cols_ = new uintV[edge_count_];

    uintE prefix = 0;
    for (uintV i = 0; i <= vertex_count_; ++i) {
        row_ptrs_[i] = prefix;
        prefix += offsets[i];
        offsets[i] = row_ptrs_[i];
    }

    for (size_t i = 0; i < edge_pairs.size(); i += 2) 
    {
        cols_[offsets[edge_pairs[i]]++] = edge_pairs[i + 1];
        cols_[offsets[edge_pairs[i + 1]]++] = edge_pairs[i];
    }

    offsets.clear();
    edge_pairs.clear();

#pragma omp parallel for schedule(dynamic)
    for (uintV u = 0; u < vertex_count_; ++u) 
    {
        std::sort(cols_ + row_ptrs_[u], cols_ + row_ptrs_[u + 1]);
    }
    std::cout << "finish building CSR" << std::endl;
}


void Graph::writeBinFile(const std::string &filename)
{
    std::string prefix = filename.substr(0, filename.rfind("."));

    Timer timer;
    timer.StartTimer();
    std::cout << "start write CSR bin file...." << std::endl;
    std::string output_filename = prefix + ".bin";
    FILE* file_out = fopen(output_filename.c_str(), "wb");
    assert(file_out != NULL);
    size_t res = 0;
    size_t uintV_size = sizeof(uintV), uintE_size = sizeof(uintE);
    res += fwrite(&uintV_size, sizeof(size_t), 1, file_out);
    res += fwrite(&uintE_size, sizeof(size_t), 1, file_out);
    res += fwrite(&vertex_count_, sizeof(size_t), 1, file_out);
    res += fwrite(&edge_count_, sizeof(size_t), 1, file_out);
    res += fwrite(row_ptrs_, sizeof(uintE), vertex_count_ + 1, file_out);
    res += fwrite(cols_, sizeof(uintV), edge_count_, file_out);

    assert(res == 4 + (vertex_count_ + 1) + edge_count_);
    fclose(file_out);
    timer.EndTimer();
    timer.PrintElapsedMicroSeconds("writing CSR bin file");
}

void Graph::writeGraphFile(const std::string &filename)
{
    std::srand(0);
    std::string prefix = filename.substr(0, filename.rfind("."));

    Timer timer;
    timer.StartTimer();
    std::cout << "start write Graph file...." << std::endl;
    std::string output_filename = prefix + ".lg";
    std::ofstream file_out(output_filename, std::ofstream::out);
    
    file_out << "t " << vertex_count_ << " " << edge_count_/2 << "\n";

    if (!label_map_.empty())
    {
        for (uintV i = 0; i < vertex_count_; ++i)
        {
            // std::cout << new_labels_[i] << " ";
            file_out << "v " << i << " " << new_labels_[i] << " " << row_ptrs_[i+1]-row_ptrs_[i] << "\n";
        }
    }
    else 
    {
        for (uintV i = 0; i < vertex_count_; ++i)
        {
            file_out << "v " << i << " " << std::rand() % LABEL_SIZE << " " << row_ptrs_[i+1]-row_ptrs_[i] << "\n";
        }
    }
    for (uintV i = 0; i < vertex_count_; ++i)
    {
        for (uintE j = row_ptrs_[i]; j < row_ptrs_[i+1]; ++j)
        {
            if (i < cols_[j])
            {
                file_out << "e " << i << " " << cols_[j] << " 1\n"; 
            }
        }
    }
    file_out.close();
    timer.EndTimer();
    timer.PrintElapsedMicroSeconds("writing Graph file");
}

void Graph::Preprocess() {
    // remove dangling nodes
    // self loops
    // parallel edges
    Timer timer;
    timer.StartTimer();
    std::cout << "start preprocess..." << std::endl;
    size_t vertex_count = GetVertexCount();
    size_t edge_count = GetEdgeCount();
    auto row_ptrs = GetRowPtrs();
    auto cols = GetCols();

    auto vertex_ecnt = new uintE[vertex_count + 1];
    memset(vertex_ecnt, 0, sizeof(uintE) * (vertex_count + 1));
    for (uintV u = 0; u < vertex_count; ++u) 
    {
        for (auto j = row_ptrs[u]; j < row_ptrs[u + 1]; ++j) 
        {
            auto v = cols[j];
            bool parallel_edge = (j > row_ptrs[u] && v == cols[j - 1]);
            bool self_loop = u == v;
            if (!parallel_edge && !self_loop) 
            {
                vertex_ecnt[u]++;
            }
        }
    }
    auto nrow_ptrs = new uintE[vertex_count + 1];
    uintE nedge_count = 0;
    for (uintV u = 0; u < vertex_count; ++u) 
    {
        nrow_ptrs[u] = nedge_count;
        nedge_count += vertex_ecnt[u];
    }
    nrow_ptrs[vertex_count] = nedge_count;
    delete[] vertex_ecnt;
    vertex_ecnt = NULL;

    auto ncols = new uintV[nedge_count];
    for (uintV u = 0; u < vertex_count; ++u) 
    {
        auto uoff = nrow_ptrs[u];
        for (uintE j = row_ptrs[u]; j < row_ptrs[u + 1]; ++j) 
        {
            auto v = cols[j];
            bool parallel_edge = j > row_ptrs[u] && v == cols[j - 1];
            bool self_loop = u == v;
            if (!parallel_edge && !self_loop) 
            {
                ncols[uoff++] = v;
            }
        }
    }
    edge_count = nedge_count;
    std::swap(row_ptrs, nrow_ptrs);
    std::swap(cols, ncols);
    delete[] nrow_ptrs;
    nrow_ptrs = NULL;
    delete[] ncols;
    ncols = NULL;

    auto new_vertex_ids = new uintV[vertex_count];
    uintV max_vertex_id = 0;
    for (uintV u = 0; u < vertex_count; ++u) 
    {
        if (row_ptrs[u] == row_ptrs[u + 1]) {
            new_vertex_ids[u] = vertex_count;
        } 
        else
        {
            new_vertex_ids[u] = max_vertex_id++;
            row_ptrs[new_vertex_ids[u]] = row_ptrs[u];
        }
    }
    for (uintE j = 0; j < edge_count; ++j) 
    {
        cols[j] = new_vertex_ids[cols[j]];
    }

    if (!label_map_.empty())
    {
        new_labels_ = new labelType[max_vertex_id];
        for (uintV u = 0; u < vertex_count; ++u)
        {
            if (new_vertex_ids[u] != vertex_count)
            {
                new_labels_[new_vertex_ids[u]] = label_map_[u];
                // std::cout << "("<< new_vertex_ids[u] << ", " << new_labels_[new_vertex_ids[u]] << ") ";
            }
        }
        // for (uintV i = 0; i < max_vertex_id; ++i)
        // {
        //     std::cout << new_labels_[i] << " ";
        //     // file_out << "v " << i << " " << new_labels_[i] << " " << row_ptrs_[i+1]-row_ptrs_[i] << "\n";
        // }
    }

    delete[] new_vertex_ids;
    new_vertex_ids = NULL;
    vertex_count = max_vertex_id;
    row_ptrs[vertex_count] = edge_count;

    timer.EndTimer();
    std::cout << "finish preprocess, time=" << timer.GetElapsedMicroSeconds() / 1000.0 << "ms"
              << ", now vertex_count=" << vertex_count << ", edge_count=" << edge_count 
              << ", label_size=" << label_set_.size() << std::endl;

    SetVertexCount(vertex_count);
    SetEdgeCount(edge_count);
    SetRowPtrs(row_ptrs);
    SetCols(cols);
};

void Graph::writeDistGraphFile(const std::string &filename)
{
    std::string prefix = filename.substr(0, filename.rfind("."));
    Timer timer;
    timer.StartTimer();
    std::cout << "start write Graph file...." << std::endl;
    std::string output_filename = prefix + ".metis";
    std::ofstream file_out(output_filename, std::ofstream::out);

    file_out << vertex_count_ << " " << edge_count_ << " 011\n";
    for (uintV u = 0; u < vertex_count_; ++u)
    {
        file_out << label_map_[u] + 1 << " ";
        for (uintE j = row_ptrs_[u]; j < row_ptrs_[u+1]; ++j)
        {
            file_out << cols_[j] + 1 << " 1 ";
        }
        file_out << '\n';
    }
    file_out.close();
    timer.EndTimer();
}

void Graph::FennelPartition(const std::string &filename, int part_num=10, int hop=1)
{
    std::string prefix = filename.substr(0, filename.rfind("."));
    Timer timer;
    timer.StartTimer();

    size_t vertex_num = GetVertexCount();
    size_t edge_num = GetEdgeCount() / 2;

    uintE *row_ptrs = GetRowPtrs();
    uintV *cols = GetCols();

    float gamma = 1.5;
    float alpha = std::sqrt(part_num) * edge_num / pow(vertex_num, gamma);
    float v = 1.1;
    float miu = v * vertex_num / part_num;

    std::vector<long> part_info = std::vector<long>(vertex_num, 0);
    std::vector<long> vertex_num_part = std::vector<long>(part_num, 0);

    std::vector<long> un_father(vertex_num);
    std::vector<long> un_first(part_num, -1);

    for (long i = 0; i < vertex_num; ++i)
        un_father[i] = i;

    for (uintV v = 0; v < vertex_num; v++)
    {
        if (v % 100000 == 0)
            std::cout << v << std::endl;

        float max_score = -99999.9;
        int max_part = 0;
        for (long i = 0; i < part_num; i++)
        {
            if (vertex_num_part[i] <= miu)
            {
                double delta_c = alpha * (std::pow(vertex_num_part[i] + 1, gamma) - pow(vertex_num_part[i], gamma));
                float score = 0;
                for (long j = row_ptrs[v]; j < row_ptrs[v + 1]; j++)
                {
                    uintV nid = cols[j];
                    // this partition contains nid
                    if (un_first[i] >= 0 && unfind(un_father, un_first[i]) == unfind(un_father, nid))
                        score += 1;

                    if (hop == 2)
                    {
                        for (long k = row_ptrs[nid]; k < row_ptrs[nid + 1]; k++)
                        {
                            uintV nnid = cols[k];
                            if (un_first[i] >= 0 && unfind(un_father, un_first[i]) == unfind(un_father, nnid))
                                score += 1;
                        }
                    }
                }
                score = score - delta_c;
                if (max_score < score)
                {
                    max_score = score;
                    max_part = i;
                }
            }
        }
        if (un_first[max_part] < 0)
            un_first[max_part] = v;
        ununion(un_father, v, un_first[max_part]);
        vertex_num_part[max_part] += 1;
        part_info[v] = max_part;
    }

    timer.EndTimer();
    timer.PrintElapsedMicroSeconds("fennel");

    std::string out_filename = prefix + ".metis.part." + std::to_string(part_num);
    std::ofstream outfile(out_filename.c_str());
    for (uintV v = 0; v < vertex_num; v++)
        outfile << part_info[v] << std::endl;
    outfile.close();
}