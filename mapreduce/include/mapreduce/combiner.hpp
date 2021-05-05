#pragma once

namespace MapReduce
{
    template <typename KeyType, typename ValueType>
    class CombinerBase
    {
    public:
        using key_t = KeyType;
        using value_t = ValueType;
    };

    template <typename KeyType, typename ValueType>
    class DefaultCombiner : public CombinerBase<KeyType, ValueType>
    {
    public:
        template <typename IntermediateStore>
        void combine(key_t key, typename IntermediateStore::const_iterator_t start, typename IntermediateStore::const_iterator_t end, IntermediateStore& store) {
            while(start != end)
            {
                store.emit(key, *start);
                start++;
            }
        }
    };
}