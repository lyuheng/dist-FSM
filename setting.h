#pragma once

namespace Settings
{
static int maxNumNodes = -1; // No constraint on maxNodes
static bool useLB = true;    // enable Load Balancing 

} // namespace Settings

char* getCmdOption(char ** begin, char ** end, const std::string & option)
{
    char ** itr = std::find(begin, end, option);
    if (itr != end && ++itr != end)
    {
        return *itr;
    }
    return 0;
}