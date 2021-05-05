#pragma once

namespace MapReduce {
    template <typename InputKey, typename InputValue>
    class DataSource {
    public:
        using input_key_t = InputKey;
        using input_value_t = InputValue;

        virtual bool getNewKey(InputKey& key) = 0;
        virtual bool getRecord(const InputKey& key, InputValue& value) = 0; // won't work if objects are not default constructible
    };
}