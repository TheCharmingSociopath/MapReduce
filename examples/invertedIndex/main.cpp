#include <mapreduce/datasource/directory_source.hpp>
#include <mapreduce/mapreduce.hpp>

#include <boost/mpi.hpp>
#include <boost/program_options.hpp>
#include <boost/range/istream_range.hpp>

#include <fstream>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
#include <set>

class Map : public MapReduce::MapBase<std::string, std::string, std::string, std::string> {
public:
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

    template <typename IntermediateStore>
    void map(input_key_t key, input_value_t, IntermediateStore& store)
    {
        std::regex re(R"([\s|,|;|!|@|.]+)");
        // std::vector<std::pair<std::string, std::string>> payload;
        // fstream FILE;
        // FILE.open(ikey, ios::in);
        // if (!FILE.is_open()) {
        //     throw MapReduce::FileExceptionInMap; // file exception in Map Task.
        // }
        // string tmp;
        // while (getline(FILE, tmp)) {
        //     std::vector<std::string> tokenized = tokenize(tmp, re);
        //     for (int i = 0; i < tokenized.size(); ++i) {
        //         payload.push_back(make_pair(tokenized[i], ikey));
        //     }
        // }
        // FILE.close();
        // handler.emit_intermediate(payload);
        std::ifstream ifs(key);
        for (const auto& word : boost::range::istream_range<output_key_t>(ifs)) {
            std::vector<std::string> tokenized = tokenize(word, re);
            for (auto& w : tokenized)
                store.emit(word, key);
        }
    }
};

class Reduce : public MapReduce::ReduceBase<std::string, std::vector<std::string>> {
public:
    template <typename Iterator, typename OutputStore>
    void reduce(key_t key, Iterator start, Iterator end, OutputStore& store)
    {
        std::set<std::string> st(start, end);
        std::list<std::string> payload(st.begin(), st.end());
        // std::sort(payload.begin(), payload.end());
        payload.sort();
        store.emit(key, payload);
    }
    // void reduce(KeyType key, Iterator itr, Iterator enditr, LocationHandler& handler)
    // {
    //     std::unordered_set<std::string> uset;
    //     for (; itr != enditr; ++itr) {
    //         if (uset.find(*itr) == uset.end()) {
    //             uset.insert(*itr);
    //         }
    //     }
    //     std::vector<std::string> payload;
    //     payload.reserve(uset.size());
    //     for (auto it = uset.begin(); it != uset.end();) {
    //         payload.push_back(std::move(uset.extract(it++).value()));
    //     }
    //     handler.emit_result(key, payload);
    // }
};

int main(int argc, char* argv[])
{
    namespace mpi = boost::mpi;

    mpi::environment env;
    mpi::communicator world;

    if (world.rank() == 0)
        std::cout << "MapReduce Example: InvertedIndex\n";

    const auto default_num_workers = std::thread::hardware_concurrency();

    namespace po = boost::program_options;

    po::options_description desc("Options");
    desc.add_options()
        ("help", "help information")
        ("directory,d", po::value<std::string>(), "directory containing text files for running invertedIndex")
        ("num-map-workers,m", po::value<unsigned int>()->default_value(default_num_workers), "number of workers for map task")
        ("num-reduce-workers,r", po::value<unsigned int>()->default_value(default_num_workers), "number of workers for reduce task")
    ;

    po::positional_options_description p;
    p.add("directory", -1);

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
              options(desc).positional(p).run(), vm);
    po::notify(vm);

    if (vm.count("help") || argc == 1)
    {
        if (world.rank() == 0)
        {
            std::cout << "Usage: " << argv[0] << " [options]\n";
            std::cout << desc;
        }
        return 0;
    }

    if (vm.count("directory") == 0)
    {
        if (world.rank() == 0)
            std::cerr << "no input directory provided\n";
        return 1;
    }

    auto input_directory = vm["directory"].as<std::string>();
    const auto num_map_workers = vm["num-map-workers"].as<unsigned int>();
    const auto num_reduce_workers = vm["num-reduce-workers"].as<unsigned int>();

    if (world.rank() == 0)
    {
        std::cout << "Configuration:\n";
        std::cout << "input directory: " << input_directory << '\n';
        std::cout << "number of map workers: " << num_map_workers << '\n';
        std::cout << "number of reduce workers: " << num_reduce_workers << '\n';
    }

    MapReduce::Specifications spec;
    spec.num_map_workers = num_map_workers;
    spec.num_reduce_workers = num_reduce_workers;
    spec.gather_on_master = true;

    MapReduce::DirectorySource datasource(input_directory);
    Map mapfn;
    MapReduce::InMemoryStorage<std::string, std::string> intermediate_store;
    MapReduce::DefaultCombiner<std::string, std::string> combiner;
    Reduce reducefn;
    MapReduce::InMemoryStorage<std::string, std::list<std::string>> output_store;

    MapReduce::Job job(datasource, mapfn, intermediate_store, combiner, reducefn, output_store);
    job.run(spec, world);

    if (world.rank() == 0) {
        std::cout << output_store.get_keys().size() << std::endl;
        for (const auto& key : output_store.get_keys()) {
            const auto& values = output_store.get_key_values(key);
            assert(values.size() == 1);
            std::cout << key << ":\n" << "=================\n";
            for (auto val : values) {
                for (auto id : val)
                    std::cout << id << "\n";
            }
            std::cout << "\n" << "=================" << "\n";
        }
    }
    return 0;
}
