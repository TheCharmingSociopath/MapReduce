#include <mapreduce/mapreduce.hpp>

#include <boost/program_options.hpp>

#include <fstream>
#include <iostream>
#include <regex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

class Map : public MapReduce::Map<std::string, std::string> {
    // Template gives output type of Map

    std::vector<std::string> tokenize(const std::string str, const std::regex re)
    {
        std::sregex_token_iterator it { str.begin(), str.end(), re, -1 };
        std::vector<std::string> tokenized { it, {} };

        // Additional check to remove empty strings
        tokenized.erase(std::remove_if(tokenized.begin(),
                            tokenized.end(),
                            [](std::string const& s) {
                                return s.size() == 0;
                            }),
            tokenized.end());

        return tokenized;
    }

    template <typename LocationHandler>
    void map(std::string filename, LocationHandler& handler)
    {
        std::vector<std::pair<std::string, std::string>> payload;
        std::regex re(R"([\s|,|;|!|@|.]+)");
        fstream FILE;
        FILE.open(filename, ios::in);
        if (!FILE.is_open()) {
            throw MapReduce::FileExceptionInMap; // file exception in Map Task.
        }
        string tmp;
        while (getline(FILE, tmp)) {
            std::vector<std::string> tokenized = tokenize(tmp, re);
            for (int i = 0; i < tokenized.size(); ++i) {
                payload.push_back(make_pair(tokenized[i], filename));
            }
        }
        FILE.close();
        handler.emit_intermediate(payload);
    }
};

class Reduce : public MapReduce::Reduce<std::string, std::vector<std::string>> {
    // Template gives output type of reduce_task

    template <typename KeyType, typename Iterator, typename LocationHandler>
    void reduce(KeyType key, Iterator itr, Iterator enditr, LocationHandler& handler)
    {
        std::unordered_set<std::string> uset;
        for (; itr != enditr; ++itr) {
            if (uset.find(*itr) == uset.end()) {
                uset.insert(*itr);
            }
        }
        std::vector<std::string> payload;
        payload.reserve(uset.size());
        for (auto it = uset.begin(); it != uset.end();) {
            payload.push_back(std::move(uset.extract(it++).value()));
        }
        handler.emit_result(key, payload)
    }
};

int main(int argc, char* argv[])
{
    std::cout << "MapReduce Example: invertedIndex\n";

    const auto default_num_workers = std::thread::hardware_concurrency();

    namespace po = boost::program_options;

    po::options_description desc("Options");
    desc.add_options()("help", "help information")("input-directory, i", po::value<std::vector<std::string>>()->multitoken(), "A directory containing text files for the inverted index task")("num-map-workers, m", po::value<unsigned int>()->default_value(default_num_workers), "number of workers for the map task")("num-reduce-workers, r", po::value<unsigned int>()->default_value(default_num_workers), "number of workers for the reduce task");

    po::positional_options_description p;
    p.add("input-directory", -1);

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).options(desc).positional(p).run(), vm);
    po::notify(vm);

    if (vm.count("help") || argc == 1) {
        std::cout << "Usage: " << argv[0] << " [options]\n";
        std::cout << desc;
        return 0;
    }

    if (vm.count("input-directory") == 0) {
        std::cerr << "no input directory provided\n";
        return 1;
    }

    const auto num_map_workers = vm["num-map-workers"].as<unsigned int>();
    const auto num_reduce_workers = vm["num-reduce-workers"].as<unsigned int>();
    std::vector<std::string> input_directories = vm["input-directory"].as<std::vector<std::string>>();

    std::cout << "Configuration:\n";
    std::cout << "number of input directories: " << input_directories.size() << '\n';
    std::cout << "number of map workers: " << num_map_workers << '\n';
    std::cout << "number of reduce workers: " << num_reduce_workers << '\n';

    MapReduce::MapReduceSpecifications specs = MapReduce::MapReduceSpecifications(num_map_workers, num_reduce_workers);

    Map map;
    Reduce reduce;

    MapReduce::Job job = MapReduce::Job(map, reduce);

    for (int i = 0; i < input_directories.size(); ++i) {
        MapReduce::Result result = MapReduce::RunMapReduceJob(job, specs, input_directories[i]);
    }

    return 0;
}
