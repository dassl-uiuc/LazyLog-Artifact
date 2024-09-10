#include "datalog.h"
#include "signal.h"

void sigint_handler(int _signum) { lazylog::RPCTransport::Stop(); }

int main(int argc, const char *argv[]) {
    using namespace lazylog;

    signal(SIGINT, sigint_handler);

    Properties prop;
    ParseCommandLine(argc, argv, prop);

    DataLog datalog_svr;
    datalog_svr.Initialize(prop);
    datalog_svr.Finalize();

    return 0;
}