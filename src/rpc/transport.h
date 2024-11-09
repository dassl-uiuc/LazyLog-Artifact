#pragma once

#include "../utils/properties.h"

namespace lazylog {

class RPCTransport {
   public:
    RPCTransport();

    virtual void InitializeConn(const Properties &p, const std::string &svr_uri, void *param) = 0;
    virtual void Initialize(const Properties &p) = 0;
    virtual void Finalize() = 0;

    static void Stop();

   protected:
    static bool run_;
};

}  // namespace lazylog
