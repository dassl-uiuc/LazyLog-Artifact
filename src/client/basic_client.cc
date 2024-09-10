#include "../utils/properties.h"
#include "lazylog_scalable_cli.h"

int main(int argc, const char *argv[]) {
    using namespace lazylog;

    Properties prop;
    ParseCommandLine(argc, argv, prop);

    LazyLogScalableClient cli;

    cli.Initialize(prop);
    std::string payload(4096, 'A');

    if (prop.GetProperty("mode", "w") == "w") {
        for (int i = 0; i < 10000000; i++) cli.AppendEntryAll(payload);
    } else {
        int yea = 0, nay = 0;
        std::string data;
        for (int i = 15000; i < 45000; i++) {
            if (cli.SpecReadEntry(i, data) > 0)
                yea++;
            else
                nay++;
        }
        std::cout << "Yea: " << yea << ", Nay: " << nay << std::endl;
    }

    return 0;
}