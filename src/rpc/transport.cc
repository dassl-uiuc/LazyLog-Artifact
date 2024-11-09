#include "transport.h"

namespace lazylog {

bool RPCTransport::run_ = true;

void RPCTransport::Stop() { run_ = false; }

RPCTransport::RPCTransport() {}

}  // namespace lazylog
