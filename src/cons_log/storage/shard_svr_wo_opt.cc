#include "shard_server_wo_opt.h"
#include "signal.h"

void sigint_handler(int _signum) { lazylog::RPCTransport::Stop(); }

int main(int argc, const char *argv[]) {
    using namespace lazylog;

    signal(SIGINT, sigint_handler);

    Properties prop;
    ParseCommandLine(argc, argv, prop);

    ShardServerUnoptimized shd_svr;
    shd_svr.Initialize(prop);
    shd_svr.Finalize();

    return 0;
}