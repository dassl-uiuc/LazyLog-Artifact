#include "rpc_token.h"

namespace lazylog {

RPCToken::RPCToken() : completed_(false) {}

bool RPCToken::Complete() { return completed_; }

void RPCToken::SetComplete() { completed_ = true; }

void RPCToken::Reset() { completed_ = false; }

} // namespace lazylog
