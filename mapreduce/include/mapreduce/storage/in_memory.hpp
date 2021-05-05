#pragma once

#include <iterator>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>

#include "storage.hpp"

#include <map>
#include <vector>

namespace MapReduce
{
    template <typename Key, typename Value>
    class InMemoryStorage : public KeyValueStorageBase<Key, Value>
    {
    public:
        using key_t = Key;
        using value_t = Value;
        using iterator_t = typename std::vector<value_t>::iterator;
        using const_iterator_t = typename std::vector<value_t>::const_iterator;

        void emit(const key_t& key, const value_t& value) override
        {
            if (!store.count(key))
                store[key] = {};
            store[key].push_back(value);
        }

        void emit(const key_t& key, const std::vector<value_t>& values) override
        {
            if (!store.count(key))
                store[key] = {};
            store[key].insert(store[key].end(), std::begin(values), std::end(values));
        }

        auto get_key_counts() const
        {
            std::map<key_t, std::size_t> counts;
            for (const auto& [key, lst] : store)
                counts[key] = lst.size();
            return counts;
        }

        auto get_keys() const
        {
            std::vector<key_t> keys;
            boost::copy(store | boost::adaptors::map_keys, std::back_inserter(keys));
            return keys;
        }

        bool is_key_present(const key_t& key) const { return store.count(key); }

        const auto& get_key_values(const key_t& key) const
        {
            return store.at(key);
        }

    private:
        std::map<key_t, std::vector<value_t>> store;
    };
}