#pragma once

namespace MapReduce
{
    template <typename InputKeyType, typename InputValueType, typename OutputKeyType = InputKeyType, typename OutputValueType = InputValueType>
    class MapBase
    {
    public:
        using input_key_t = InputKeyType;
        using input_value_t = InputValueType;
        using output_key_t = OutputKeyType;
        using output_value_t = OutputValueType;        
    };
}