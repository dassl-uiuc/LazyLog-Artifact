#include "dur_log_flat_cli.h"


int main(int argc, const char *argv[]) {
    using namespace lazylog;

    Properties prop;
    ParseCommandLine(argc, argv, prop);
    
    DurabilityLogFlatCli cli;
    cli.InitializeConn(prop, "localhost:31850", nullptr);

    auto tail = cli.GetNumDurEntry();
    std::cout << std::get<0>(tail) << " " <<  std::get<1>(tail) << std::endl;

    std::vector<LogEntry> e;
    std::vector<LogEntry::ReqID> r;
    auto n = cli.FetchUnorderedEntries(e, 100);

    std::cout << n << "B entries" << std::endl;
    for (int i = 0; i < 10; i++) {
        std::cout << e[i] << std::endl;
    }
    cli.ProcessFetchedEntries(e, r);
    cli.DeleteOrderedEntries(r);
    tail = cli.GetNumDurEntry();
    std::cout << std::get<0>(tail) << " " <<  std::get<1>(tail) << std::endl;
    char c;
    std::cin >> c;

    e.clear();
    n = cli.FetchUnorderedEntries(e, 100);

    std::cout << n << " entries" << std::endl;

    return 0;
}