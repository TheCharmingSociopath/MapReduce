#pragma once

#include "datasource.hpp"

#include <filesystem>

namespace MapReduce
{
    // mmap file handlers?
    // abstract away ifstream?

    class DirectorySource : public DataSource<std::string, std::string>
    {
    public:
        DirectorySource(const std::filesystem::path& dirpath) : dit(dirpath) { }

        bool getNewKey(input_key_t& key) override
        {
            while (dit != decltype(dit)())
            {
                const auto& entry = *dit++;
                if (std::filesystem::is_regular_file(entry))
                {
                    key = entry.path();
                    cache[key] = key;
                    return  true;
                } 
            }
            return false;
        }

        bool getRecord(const input_key_t& key, input_value_t& value) override
        {
            if (cache.count(key))
            {
                value = cache[key];
                return true;
            }
            return false;
        }

    private:
        std::filesystem::directory_iterator dit;
        std::map<input_key_t, input_value_t> cache;
    };
}