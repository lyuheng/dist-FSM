#pragma once

namespace Settings
{
// Default settings
static int maxNumNodes = -1; // No constraint on maxNodes
static bool useLB = true;    // Enable load balancing between workers

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