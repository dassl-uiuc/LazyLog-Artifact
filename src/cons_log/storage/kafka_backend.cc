#include "kafka_backend.h"

#include "../../rpc/common.h"
#include "glog/logging.h"

namespace lazylog {
using namespace cppkafka;

void dr_callback(Producer& producer, const Message& msg) {
    if (msg.get_handle()->_private) {
        auto* errp = (rd_kafka_resp_err_t*)msg.get_handle()->_private;
        *errp = msg.get_error().get_error();
    }
}

KafkaBackend::KafkaBackend() : producer_(nullptr), shard_num_(0) {}

uint64_t KafkaBackend::AppendBatch(const std::vector<LogEntry>& es) {
    auto end = es.end() - 1;
    if (es.size() > 1 && end->log_idx == 0) end = es.end() - 2;

    size_t stripe_count = 0;
    for (auto it = es.begin(); it != end; it++) {
        int shard_id = it->log_idx % shard_num_;
        size_t len = Serializer(*it, buf_);
        std::string payload(reinterpret_cast<char*>(buf_), len);
        producer_->produce(MessageBuilder(topic_).partition(shard_id).payload(payload));
        if (++stripe_count >= stripe_unit_size_ * shard_num_) {
            producer_->flush();
            stripe_count = 0;
        }
    }
    producer_->flush(std::chrono::milliseconds(1000));

    return end->log_idx;
}

void KafkaBackend::InitializeBackend(const Properties& p) {
    Configuration config = {{"bootstrap.servers", p.GetProperty("bootstrap.servers", "localhost:9092")},
                            {"acks", "all"}};
    config.set_delivery_report_callback(dr_callback);

    producer_ = new Producer(config);
    topic_ = p.GetProperty("kafka.topic", "default-topic");
    shard_num_ = std::stoi(p.GetProperty("shard.num", "1"));
    stripe_unit_size_ = std::stoi(p.GetProperty(PROP_SHD_STRIPE_SIZE, PROP_SHD_STRIPE_SIZE_DEFAULT));

    LOG(INFO) << "Kafka backend connected, shards: " << shard_num_;
}

void KafkaBackend::FinalizeBackend() {
    if (producer_) delete producer_;
}

}  // namespace lazylog
