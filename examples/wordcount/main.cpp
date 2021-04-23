#include <mapreduce/mapreduce.hpp>

#include <boost/program_options.hpp>

#include <iostream>
#include <thread>
#include <vector>
#include <string>

int main (int argc, char* argv[]) {
    std::cout << "MapReduce Example: Wordcount\n";

    const auto default_num_workers = std::thread::hardware_concurrency();

    namespace po = boost::program_options;

    po::options_description desc("Options");
    desc.add_options()
        ("help", "help information")
        ("input-file,i", po::value<std::vector<std::string>>()->multitoken(), "text files for word count")
        ("num-map-workers,m", po::value<unsigned int>()->default_value(default_num_workers), "number of workers for map task")
        ("num-reduce-workers,r", po::value<unsigned int>()->default_value(default_num_workers), "number of workers for reduce task")
    ;

    po::positional_options_description p;
    p.add("input-file", -1);

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
              options(desc).positional(p).run(), vm);
    po::notify(vm);

    if (vm.count("help") || argc == 1)
    {
        std::cout << "Usage: " << argv[0] << " [options]\n";
        std::cout << desc;
        return 0;
    }

    if (vm.count("input-file") == 0)
    {
        std::cerr << "no input files provided\n";
        return 1;
    }

    const auto num_map_workers = vm["num-map-workers"].as<unsigned int>();
    const auto num_reduce_workers = vm["num-reduce-workers"].as<unsigned int>();
    std::vector<std::string> input_files = vm["input-file"].as<std::vector<std::string>>();

    std::cout << "Configuration:\n";
    std::cout << "number of input files: " << input_files.size() << '\n';
    std::cout << "number of map workers: " << num_map_workers << '\n';
    std::cout << "number of reduce workers: " << num_reduce_workers << '\n';

    
    return 0;
}