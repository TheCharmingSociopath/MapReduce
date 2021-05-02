#pragma once

namespace MapReduce
{
    template <typename Key, typename Value>
    class KeyValueStorageBase
    {
    public:
        using key_t = Key;
        using value_t = Value;

        virtual void emit(const key_t& key, const value_t& value) = 0;
        virtual void emit(const key_t& key, const std::list<value_t>& values) = 0;
    };
}