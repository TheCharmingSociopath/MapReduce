#pragma once

#include <boost/mpi.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/serialization/list.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>

#include "specification.hpp"
#include "combiner.hpp"

#include <chrono>
#include <utility>
#include <algorithm>
#include <list>
#include <map>
#include <queue>
#include <vector>
#include <thread>

template <class... Args>
void log(boost::mpi::communicator& comm, Args&&... args)
{   
    if (comm.rank() == 0)
        std::cerr << "[master] ";
    else
        std::cerr << "[slave-" << comm.rank() << "] ";
    ((std::cerr << args), ...);
    std::cerr << std::endl;
}

namespace MapReduce
{
    template <typename Datasource, typename MapFunc, typename IntermediateStore, typename CombinerFunc, typename ReduceFunc, typename OutputStore>
    class Job
    {
    public:
        using datasource_t = Datasource;
        using map_func_t = MapFunc;
        using combiner_t = CombinerFunc;
        using intermediate_store_t = IntermediateStore;
        using reduce_func_t = ReduceFunc;
        using output_store_t = OutputStore;

        using input_key_t = typename map_func_t::input_key_t;
        using input_value_t = typename map_func_t::input_value_t;
        using intermediate_key_t = typename intermediate_store_t::key_t;
        using intermediate_value_t = typename intermediate_store_t::value_t;
        using output_key_t = typename output_store_t::key_t;
        using output_value_t = typename output_store_t::value_t;

        Job(datasource_t& ds, map_func_t& map_fn, intermediate_store_t& is, combiner_t& cfn, reduce_func_t& reduce_fn, output_store_t& output_store)
            : input_ds(ds), map_fn(map_fn), combiner(cfn), istore(is), reduce_fn(reduce_fn), output_store(output_store)
        {
        }

        ~Job()
        {
        }

        void run(const Specifications& spec, boost::mpi::communicator& comm)
        {
            // we require at least one worker
            assert(spec.num_map_workers >= 1);
            assert(spec.num_reduce_workers >= 1);
            assert(comm.size() >= spec.num_map_workers + 1);
            assert(comm.size() >= spec.num_reduce_workers + 1);

            comm.barrier();
            run_map_phase(spec, comm);
            comm.barrier();
            run_combine_phase(spec, comm);
            comm.barrier();
            run_shuffle_phase(spec, comm);
            comm.barrier();
            run_reduce_phase(spec, comm);
            comm.barrier();
            run_gather_phase(spec, comm);
            comm.barrier();
        }

    private:
        using map_task_inputs_t = std::map<input_key_t, std::vector<input_value_t>>;

        void run_map_phase(const Specifications& spec, boost::mpi::communicator& comm)
        {
            using std::chrono::steady_clock;

            // workers has the list of ranks of the map processes in `comm`
            std::vector<int> workers(spec.num_map_workers);
            std::iota(std::begin(workers), std::end(workers), 1);
            assert(comm.size() >= spec.num_map_workers + 1);

            if (comm.rank() == 0)
            {
                int map_tasks_pending = 0;
                int failed_tasks = 0;

                for (auto p : workers)
                {
                    log(comm, "sending MapPhaseBegin to ", p);
                    comm.send(p, MapPhaseBegin);
                }

                // adds a task item to task_reports
                // sends task to a worker
                auto assign_map_task = [&](int rank, TaskItem& item) {
                    auto task_id = map_tasks.size();
                    item.status.worker = rank;
                    item.status.completed = false;
                    item.status.start_time = steady_clock::now();
                    item.status.last_ping_time = steady_clock::now();

                    map_tasks_pending++;
                    log(comm, "assigned task ", task_id, " to process ", rank);
                    comm.send(rank, MapTaskAssignment, std::make_pair(task_id, item.inputs));
                    
                    map_tasks.push_back(std::move(item));
                };

                auto reassign_map_task = [&](int rank, int task_id) {
                    TaskItem& item = map_tasks[task_id];

                    item.status.worker = rank;
                    item.status.completed = false;
                    item.status.start_time = steady_clock::now();
                    item.status.last_ping_time = steady_clock::now();

                    log(comm, "reassigned task ", task_id, " to process ", rank);
                    comm.send(rank, MapTaskAssignment, std::make_pair(task_id, item.inputs));
                };

                // dump initial work
                for (auto p : workers)
                {
                    input_key_t key;
                    if (input_ds.getNewKey(key))
                    {
                        input_value_t value;
                        input_ds.getRecord(key, value);

                        TaskItem item;
                        item.inputs[key] = {value};
                        assign_map_task(p, item);
                    }
                }

                while(map_tasks_pending)
                {
                    auto status = comm.iprobe();
                    if (!status)
                    {
                        using namespace std::chrono_literals;
                        std::this_thread::sleep_for(spec.ping_check_frequency);

                        // check for failures
                        for (const auto& task : map_tasks)
                        {
                            auto cur_time = steady_clock::now();
                            auto last_ping_time = task.status.last_ping_time;
                            if (cur_time - last_ping_time > spec.ping_failure_time && task.status.worker != -1 && task.status.completed == false)
                            {
                                std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(cur_time - last_ping_time).count() << "ms delay" << '\n';
                                log(comm, "worker ", task.status.worker, " has failed; saving tasks for re-execution");
                                for (auto& t2 : map_tasks)
                                {
                                    // mark all tasks assigned to that worker as failed
                                    if (t2.status.worker == task.status.worker)
                                    {
                                        t2.status.worker = -1;
                                        failed_tasks++;
                                    }
                                }

                                failed_workers.push_back(task.status.worker);
                            }
                        }
                    }
                    else
                    {
                        auto msg = status.get();
                        if (msg.tag() == MapPhasePing)
                        {
                            std::size_t task_id;
                            comm.recv(msg.source(), MapPhasePing, task_id);
                            map_tasks[task_id].status.last_ping_time = steady_clock::now();
                            log(comm, "recvd MapPhasePing from ", msg.source(), " for task_id ", task_id);
                        }
                        else if (msg.tag() == MapTaskCompletion)
                        {
                            std::size_t task_id;
                            comm.recv(msg.source(), MapTaskCompletion, task_id);
                            log(comm, "recvd MapTaskCompletion from ", msg.source(), " for task_id ", task_id);

                            map_tasks_pending--;
                            map_tasks[task_id].status.completed = true;
                            map_tasks[task_id].status.end_time = steady_clock::now();

                            input_key_t key;
                            if (input_ds.getNewKey(key))
                            {
                                input_value_t value;
                                input_ds.getRecord(key, value);

                                TaskItem item;
                                item.inputs[key] = {value};
                                assign_map_task(msg.source(), item);
                            }
                            else
                            {
                                // reassign failed tasks
                                if (failed_tasks)
                                {
                                    for (int i = 0; i < map_tasks.size(); i++)
                                    {
                                        const auto& task = map_tasks[i];
                                        if (task.status.worker == -1)
                                        {
                                            reassign_map_task(msg.source(), i);
                                            failed_tasks--;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                for (auto p : workers)
                {
                    log(comm, "sending MapPhaseEnd to ", p);
                    comm.send(p, MapPhaseEnd);
                }
            }
            else if (boost::algorithm::any_of_equal(workers, comm.rank()))
            {
                std::atomic<bool> stop_pinger;
                stop_pinger.store(false);

                std::atomic<std::size_t> current_task_id;
                current_task_id.store(-1);

                std::thread pinger([&]() {
                    while(stop_pinger.load() == false)
                    {
                        if (current_task_id.load() != -1)
                        {
                            comm.send(0, MapPhasePing, current_task_id.load());
                            std::this_thread::sleep_for(spec.ping_frequency);
                        }
                    }
                });

                comm.recv(0, MapPhaseBegin);
                log(comm, "recvd MapPhaseBegin");

                while (1)
                {
                    auto msg = comm.probe(0);
                    if (msg.tag() == MapTaskAssignment)
                    {
                        std::pair<std::size_t, map_task_inputs_t> data;
                        comm.recv(0, MapTaskAssignment, data);
                        auto [task_id, inputs] = data;
                        current_task_id.store(task_id);

                        log(comm, "recvd MapTaskAssigment with task id ", task_id);
                        for (const auto& [key, values] : inputs)
                        {
                            log(comm, "exec mapfn on key \"", key, "\" with total of ", values.size(), " values.");
                            for (const auto& value : values)
                                map_fn.map(key, value, istore);
                        }

                        current_task_id.store(-1);
                        log(comm, "sent MapTaskCompletion with task id ", task_id);
                        comm.send(0, MapTaskCompletion, task_id);
                    }
                    else if (msg.tag() == MapPhaseEnd)
                    {
                        comm.recv(msg.source(), MapPhaseEnd);
                        log(comm, "recvd MapPhaseEnd");
                        break;
                    }
                    else
                    {
                        assert(0);
                    }
                }

                stop_pinger.store(true);
                pinger.join();
            }
        }

        void run_combine_phase(const Specifications& spec, boost::mpi::communicator& comm)
        {
            if constexpr (!std::is_same<combiner_t, DefaultCombiner<intermediate_key_t, intermediate_value_t>>::value)
            {
                intermediate_store_t combiner_istore;
                for (const auto& key : istore.get_keys())
                {
                    const auto& values = istore.get_key_values(key);
                    combiner.combine(key, std::begin(values), std::end(values), combiner_istore);
                    log(comm, "exec combiner on key \"", key, "\" with total of ", values.size(), " values.");
                }

                istore = std::move(combiner_istore);
            }
        }


        void run_shuffle_phase(const Specifications& spec, boost::mpi::communicator& comm)
        {
            // workers has the list of ranks of the map processes in `comm`
            std::vector<int> map_workers(spec.num_map_workers);
            std::iota(std::begin(map_workers), std::end(map_workers), 1);
            assert(comm.size() >= spec.num_map_workers + 1);

            // workers has the list of ranks of the map processes in `comm`
            std::vector<int> reduce_workers(spec.num_reduce_workers);
            std::iota(std::begin(reduce_workers), std::end(reduce_workers), 1);
            assert(comm.size() >= spec.num_reduce_workers + 1);

            if (comm.rank() == 0)
            {
                std::map<intermediate_key_t, std::size_t> global_counts;
                for (auto p : map_workers)
                {
                    decltype(std::declval<intermediate_store_t&>().get_key_counts()) counts;
                    comm.recv(p, ShuffleIntermediateCounts, counts);
                    log(comm, "recvd ShuffleIntermediateCounts from ", p);

                    for (const auto& [key, c] : counts)
                    {
                        if (global_counts.count(key))
                            global_counts[key] += c;
                        else
                            global_counts[key] = c;
                    }
                }

                std::vector<std::pair<std::size_t, intermediate_key_t>> key_counts;
                for (const auto& [key, value] : global_counts)
                    key_counts.push_back(std::make_pair(value, key));
                
                std::sort(key_counts.rbegin(), key_counts.rend());

                std::priority_queue<std::pair<std::size_t, int>, 
                                    std::vector<std::pair<std::size_t, int>>, 
                                    std::greater<std::pair<std::size_t, int>>> load_balancer_pq;
                for(int i=0; i<reduce_workers.size(); ++i)
                    load_balancer_pq.push(std::make_pair(0, i));
                
                std::map<intermediate_key_t, int> process_map;
                for(const auto& [count, key] : key_counts)
                {
                    auto[min_makespan, min_reduce_worker_idx] = load_balancer_pq.top();
                    load_balancer_pq.pop();
                    process_map[key] = reduce_workers[min_reduce_worker_idx];
                    load_balancer_pq.push(std::make_pair(min_makespan+count, min_reduce_worker_idx));
                }
                
                for (auto p : map_workers)
                {
                    log(comm, "sent ShuffleDistributionMap to ", p);
                    comm.send(p, ShuffleDistributionMap, process_map);
                }
            }
            else
            {
                intermediate_store_t new_istore;
                if (boost::algorithm::any_of_equal(map_workers, comm.rank()))
                {
                    auto counts = istore.get_key_counts();
                    comm.send(0, ShuffleIntermediateCounts, counts);
                    log(comm, "sent ShuffleIntermediateCounts");

                    std::map<intermediate_key_t, int> process_map;
                    comm.recv(0, ShuffleDistributionMap, process_map);
                    log(comm, "recvd ShuffleDistributionMap");

                    for (const auto& [key, p] : process_map)
                    {
                        if (!istore.is_key_present(key))
                            continue;
 
                        const auto& values = istore.get_key_values(key);
                        if (p != comm.rank())
                        {
                            comm.isend(p, ShufflePayloadDelivery, std::make_pair(key, values));
                            log(comm, "isent ShufflePayloadDelivery with key \"", key, "\" containing ", values.size(), " values to ", p);
                        }
                        else
                        {
                            log(comm, "(fake)sent ShufflePayloadDelivery with key \"", key, "\" containing ", values.size(), " values to self");
                            new_istore.emit(key, values);
                        }
                    }

                    for (auto p : reduce_workers)
                    {
                        //if (p != comm.rank())
                        {
                            comm.isend(p, ShufflePayloadDeliveryComplete);
                            log(comm, "isent ShufflePayloadDeliveryComplete to ", p);
                        }
                    }
                }

                if (boost::algorithm::any_of_equal(reduce_workers, comm.rank()))
                {
                    int awaiting_completion = map_workers.size();
                    while (awaiting_completion)
                    {
                        auto msg = comm.probe();
                        if (msg.tag() == ShufflePayloadDelivery)
                        {
                            std::pair<intermediate_key_t, std::vector<intermediate_value_t>> data;
                            comm.recv(msg.source(), ShufflePayloadDelivery, data);
                            auto& [key, values] = data;
                            new_istore.emit(std::move(key), std::move(values));
                            log(comm, "recvd ShufflePayloadDelivery with key \"", key, "\" from ", msg.source());
                        }
                        else if (msg.tag() == ShufflePayloadDeliveryComplete)
                        {
                            comm.recv(msg.source(), ShufflePayloadDeliveryComplete);
                            log(comm, "recvd ShufflePayloadDeliveryComplete from ", msg.source());
                            awaiting_completion--;
                        }
                        else
                        {
                            assert(0);
                        }
                    }
                }                

                istore = std::move(new_istore);
            }
        }

        void run_reduce_phase(const Specifications& spec, boost::mpi::communicator& comm)
        {
            if (comm.rank() != 0)
            {
                for (const auto& key : istore.get_keys())
                {
                    const auto& values = istore.get_key_values(key);
                    reduce_fn.reduce(key, std::begin(values), std::end(values), output_store);
                    log(comm, "exec reducefn on key \"", key, "\" with total of ", values.size(), " values.");
                }
            }
        }

        void run_gather_phase(const Specifications& spec, boost::mpi::communicator& comm)
        {
            if (!spec.gather_on_master)
                return;

            std::vector<int> workers(spec.num_reduce_workers);
            std::iota(std::begin(workers), std::end(workers), 1);
            assert(comm.size() >= spec.num_reduce_workers + 1);

            if (comm.rank() == 0)
            {
                int awaiting_completion = workers.size();
                while (awaiting_completion)
                {
                    auto msg = comm.probe();
                    if (msg.tag() == GatherPayloadDelivery)
                    {
                        std::pair<output_key_t, std::vector<output_value_t>> data;
                        comm.recv(msg.source(), GatherPayloadDelivery, data);
                        auto& [key, values] = data;
                        output_store.emit(std::move(key), std::move(values));
                        log(comm, "recvd GatherPayloadDelivery with key \"", key, "\" from ", msg.source());
                    }
                    else if (msg.tag() == GatherPayloadDeliveryComplete)
                    {
                        comm.recv(msg.source(), GatherPayloadDeliveryComplete);
                        log(comm, "recvd GatherPayloadDeliveryComplete from ", msg.source());
                        awaiting_completion--;
                    }
                    else if (msg.tag() == MapPhasePing)
                    {
                        std::size_t task_id;
                        comm.recv(msg.source(), MapPhasePing, task_id);
                    }
                    else
                    {
                        assert(0);
                    }
                }
            }
            else if (boost::algorithm::any_of_equal(workers, comm.rank()))
            {
                for (const auto& key : output_store.get_keys())
                {
                    const auto& values = output_store.get_key_values(key);
                    comm.isend(0, GatherPayloadDelivery, std::make_pair(key, values));
                    log(comm, "isent GatherPayloadDelivery with key \"", key, "\" to master");
                }

                comm.isend(0, GatherPayloadDeliveryComplete);
                log(comm, "isent GatherPayloadDeliveryComplete to master");
            }
        }

    private:
        // note that each process has its own copy
        datasource_t& input_ds;
        map_func_t& map_fn;
        intermediate_store_t& istore; // accumulates intermediates from successive tasks
        combiner_t& combiner;
        reduce_func_t& reduce_fn;
        output_store_t& output_store;

        enum {
            MapPhaseBegin,
            MapTaskAssignment,
            MapTaskCompletion,
            MapPhasePing,
            MapPhaseEnd,

            ShufflePhaseBegin,
            ShuffleIntermediateCounts,
            ShuffleDistributionMap,
            ShufflePayloadDelivery,
            ShufflePayloadDeliveryComplete,
            ShufflePhaseEnd,

            GatherPayloadDelivery,
            GatherPayloadDeliveryComplete
        };

        struct TaskItem {
            map_task_inputs_t inputs;

            struct {
                int worker;
                bool completed;
                std::chrono::time_point<std::chrono::steady_clock> start_time, end_time;
                std::chrono::time_point<std::chrono::steady_clock> last_ping_time;
            } status;
        };
        
        // indices are task id
        std::vector<TaskItem> map_tasks;
        std::vector<int> failed_workers;
    };
}