#include "erpc_dl_transport.h"
#include <signal.h>
#include <memory>

#include "../utils/properties.h"
#include "../rpc/rpc_factory.h"

void sigint_handler(int _signum) {
    lazylog::RPCTransport::Stop();
}

int main(int argc, const char *argv[]) {
    using namespace lazylog;

    signal(SIGINT, sigint_handler);
    Properties prop;
    ParseCommandLine(argc, argv, prop);

    std::shared_ptr<RPCTransport> dur_svr = RPCFactory::CreateSvrRPCTransport(prop);
    dur_svr->Initialize(prop);

    dur_svr->Finalize();
    return 0;
};
