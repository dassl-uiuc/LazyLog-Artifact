#include "erpc_cl_transport.h"
#include "../rpc/rpc_factory.h"
#include <signal.h>

void sigint_handler(int _signum) { lazylog::RPCTransport::Stop(); }

int main(int argc, const char *argv[]) {
    using namespace lazylog;

    signal(SIGINT, sigint_handler);
    lazylog::Properties prop;
    lazylog::ParseCommandLine(argc, argv, prop);

    std::shared_ptr<RPCTransport> cons_svr = RPCFactory::CreateSvrRPCTransport(prop);
    cons_svr->Initialize(prop);

    cons_svr->Finalize();
    return 0;
}