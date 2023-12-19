#include "preprocess.h"

class CommandLine {
 public:
  int argc;
  char** argv;

  CommandLine(int _argc, char** _argv) : argc(_argc), argv(_argv) {}

  void BadArgument() {
    std::cout << "usage: " << argv[0] << " bad argument" << std::endl;
    abort();
  }

  char* GetOptionValue(const std::string& option) {
    for (int i = 1; i < argc; i++)
      if ((std::string)argv[i] == option)
        return argv[i + 1];
    return NULL;
  }

  std::string GetOptionValue(const std::string& option, std::string defaultValue) {
    for (int i = 1; i < argc; i++)
      if ((std::string)argv[i] == option)
        return (std::string)argv[i + 1];
    return defaultValue;
  }

  int GetOptionIntValue(const std::string& option, int defaultValue) {
    for (int i = 1; i < argc; i++)
      if ((std::string)argv[i] == option) {
        int r = atoi(argv[i + 1]);
        return r;
      }
    return defaultValue;
  }

  long GetOptionLongValue(const std::string& option, long defaultValue) {
    for (int i = 1; i < argc; i++)
      if ((std::string)argv[i] == option) {
        long r = atol(argv[i + 1]);
        return r;
      }
    return defaultValue;
  }

  double GetOptionDoubleValue(const std::string& option, double defaultValue) {
    for (int i = 1; i < argc; i++)
      if ((std::string)argv[i] == option) {
        double val;
        if (sscanf(argv[i + 1], "%lf", &val) == EOF) {
          BadArgument();
        }
        return val;
      }
    return defaultValue;
  }
};

int main(int argc, char* argv[])
{
    CommandLine cmd(argc, argv);
    std::string filename = cmd.GetOptionValue("-f", "./data/com-dblp.ungraph.txt");
    std::string label_filename = cmd.GetOptionValue("-l", "");

    if (label_filename == "")
    {
        Graph graph(filename);
        graph.Preprocess();
        graph.writeGraphFile(filename);
        // graph.writeDistGraphFile(filename);
        // graph.FennelPartition(filename, 1);
    }
    else 
    {
        Graph graph(filename, label_filename);
        graph.Preprocess();
        graph.writeGraphFile(filename);
    }
}