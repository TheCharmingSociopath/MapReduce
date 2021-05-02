#pragma once

namespace MapReduce {
    template <typename KeyType, typename ValueType>
    class ReduceBase {
    public:
        using key_t = KeyType;
        using value_t = ValueType;
    };
}