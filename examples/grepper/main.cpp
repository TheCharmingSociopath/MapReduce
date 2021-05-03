#include <mapreduce/mapreduce.hpp>
#include <mapreduce/datasource/directory_source.hpp>

#include <boost/program_options.hpp>
#include <boost/range/istream_range.hpp>
#include <boost/mpi.hpp>

#include <iostream>
#include <fstream>
#include <thread>
#include <vector>
#include <string>
#include <sstream>
#include <numeric>

class Map : public MapReduce::MapBase<std::string, std::string, std::string, int> {
public:
    template <typename IntermediateStore>
    void map(input_key_t key, input_value_t, IntermediateStore& store) {
        std::ifstream ifs(key);
        std::string line;
        int line_no = 0;
        while(getline(ifs, line))
        {
            line_no++;
            if(line.find("hello") != std::string::npos)
                store.emit(key, line_no);
        }
    }
};

class Reduce : public MapReduce::ReduceBase<std::string, std::list<int>> {
public:
    template <typename Iterator, typename OutputStore>
    void reduce(key_t key, Iterator start, Iterator end, OutputStore& store) {
        std::list<int> file_lines(start, end);
        store.emit(key, file_lines);
    }
};

int main (int argc, char* argv[])
{
    namespace mpi = boost::mpi;

    mpi::environment env;
    mpi::communicator world;

    if (world.rank() == 0)
        std::cout << "MapReduce Example: Grepper\n";

    const auto default_num_workers = std::thread::hardware_concurrency();

    namespace po = boost::program_options;

    po::options_description desc("Options");
    desc.add_options()
        ("help", "help information")
        ("directory,d", po::value<std::string>(), "directory containing text files for grep")
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

    auto source_dir = vm["directory"].as<std::string>();
    const auto num_map_workers = vm["num-map-workers"].as<unsigned int>();
    const auto num_reduce_workers = vm["num-reduce-workers"].as<unsigned int>();

    if (world.rank() == 0)
    {
        std::cout << "Configuration:\n";
        std::cout << "source directory: " << source_dir << '\n';
        std::cout << "number of map workers: " << num_map_workers << '\n';
        std::cout << "number of reduce workers: " << num_reduce_workers << '\n';
    }

    MapReduce::Specifications spec;
    spec.num_map_workers = num_map_workers;
    spec.num_reduce_workers = num_reduce_workers;
    spec.gather_on_master = true;

    MapReduce::DirectorySource datasource(source_dir);
    Map mapfn;
    MapReduce::InMemoryStorage<std::string, int> intermediate_store;
    Reduce reducefn;
    MapReduce::InMemoryStorage<std::string, std::list<int>> output_store;

    MapReduce::Job job(datasource, mapfn, intermediate_store, reducefn, output_store);
    job.run(spec, world);


    if (world.rank() == 0)
    {
        std::cout << output_store.get_keys().size() << std::endl;
        for (const auto& key : output_store.get_keys())
        {
            const auto& values = output_store.get_key_values(key);
            assert(values.size() == 1);
            std::cout << key << ' ';
            for(auto value_lst : values) 
            {
                for(auto line_no : value_lst)
                    std::cout << line_no << ' ';
            }
            std::cout << std::endl;
        }
    }
    return 0;
}