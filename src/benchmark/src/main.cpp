#include <iostream>
#include <cassert>

#include <utils/properties.h>

#include <workload.hpp>
#include <cxxopts.hpp>
#include <producer.hpp>
#include <consumer.hpp>
#include <commons.hpp>
#include <lazylog/producer_lazylog.hpp>
#include <lazylog/consumer_lazylog.hpp>
#include <lazylog/pc_lazylog.hpp>

using namespace OpenMsgCpp;
using namespace lazylog;

const cxxopts::Options forgeParser() {
    cxxopts::Options options("openmsgcpp", "Open Messaging Benchmark rewritten in cpp");
    options.add_options()
        ("f,workload", "Workload file path", cxxopts::value<std::string>())
        ("c,framework", "Is this instance producer(p)? consumer(c)? or both(b) or speculative read (s)?", 
            cxxopts::value<std::string>()->default_value("p"))
        ("t,type", "type of producer/consumer", cxxopts::value<std::string>()->default_value("default"))
        ("s,streamfile", 
            "the spsc queue file for local", cxxopts::value<std::string>()->
            default_value("/users/JiyuHu23/mydata/stream"))
        ("i,clientOffset", 
            "offset of client thread id", cxxopts::value<int>()->default_value("0"))
        ("l,latency", 
            "latency output file (normal read latency for spec read)", cxxopts::value<std::string>()->default_value(""))
        ("L,tailLen", 
            "tail length output file", cxxopts::value<std::string>()->default_value(""))  
        ("o,overallLat", 
            "overall latency for spec read", cxxopts::value<std::string>()->default_value(""))
        ("T,tailLat", 
            "GetTail latency output file (speculate read latency)", cxxopts::value<std::string>()->default_value(""))
        ("v,veriLat", 
            "speculative read verification latency", cxxopts::value<std::string>()->default_value(""))
        ("m,consumeMode", 
            "read mode: r: normal read; b: batch; p: pipelined, n: no consumer, s: synchronous, f: fast read", 
            cxxopts::value<std::string>()->default_value("r"))
        ("P,lazypropertyfile", 
            "lazylog property file", cxxopts::value<std::vector<std::string>>())
        ("p,lazyproperty", 
            "single lazylog property", cxxopts::value<std::vector<std::string>>())
        ("h,help", "help", cxxopts::value<bool>()->default_value("false"));

    return options;
}

const std::vector<std::string> separateString(const std::string& input, const char& separator) {
    std::stringstream ss(input);
    std::string token;
    std::vector<std::string> tokens;

    while (std::getline(ss, token, separator)) {
        tokens.push_back(token);
    }

    return tokens;
}

int main(int argc, const char* argv[]) {
    auto parser = forgeParser();
    auto options = parser.parse(argc, argv);

    if (options.count("help")) {
        std::cout << parser.help() << std::endl;
        return(1);
    }

    auto load = workload(options["workload"].as<std::string>());
    if (options["framework"].as<std::string>() == "p") {
        // start producer
        producer *p;
        if (options["type"].as<std::string>() == "default") {
            p = new producer(load);
        } else if (options["type"].as<std::string>() == "lazylog") {
            Properties prop;
            for (auto pr : options["lazypropertyfile"].as<std::vector<std::string>>()) {
                std::ifstream input(pr);
                log_info("loading property file " + pr);
                try {
                    prop.Load(input);
                } catch (const std::string &message) {
                    log_error(message);
                    exit(0);
                }
            }
            for (auto pr : options["lazyproperty"].as<std::vector<std::string>>()) {
                auto singleP = separateString(pr, '=');
                try {
                    log_info("setting property " + singleP[0] + "=" + singleP[1]);
                    prop.SetProperty(singleP[0], singleP[1]);
                } catch (const std::string &message) {
                    log_error(message);
                    exit(0);
                }
            }
            p = new producerLazylog(load, prop);
        }

        p->setLatLog(options["latency"].as<std::string>());
        p->run();
    } else if (options["framework"].as<std::string>() == "c") {
        // start consumer
        consumer *c;
        if (options["type"].as<std::string>() == "default") {
            c = new consumer(load);
        } else if (options["type"].as<std::string>() == "lazylog") {
            Properties prop;
            for (auto p : options["lazypropertyfile"].as<std::vector<std::string>>()) {
                std::ifstream input(p);
                log_info("loading property file " + p);
                try {
                    prop.Load(input);
                } catch (const std::string &message) {
                    log_error(message);
                    exit(0);
                }
            }
            for (auto p : options["lazyproperty"].as<std::vector<std::string>>()) {
                auto singleP = separateString(p, '=');
                try {
                    log_info("setting property " + singleP[0] + "=" + singleP[1]);
                    prop.SetProperty(singleP[0], singleP[1]);
                } catch (const std::string &message) {
                    log_error(message);
                    exit(0);
                }
            }
            c = new consumerLazylog(load, prop);
        }

        c->setLatLog(options["latency"].as<std::string>());
        c->run();
    } else if (options["framework"].as<std::string>() == "b") {
        Properties prop;
        for (auto pr : options["lazypropertyfile"].as<std::vector<std::string>>()) {
            std::ifstream input(pr);
            log_info("loading property file " + pr);
            try {
                prop.Load(input);
            } catch (const std::string &message) {
                log_error(message);
                exit(0);
            }
        }
        for (auto pr : options["lazyproperty"].as<std::vector<std::string>>()) {
            auto singleP = separateString(pr, '=');
            try {
                log_info("setting property " + singleP[0] + "=" + singleP[1]);
                prop.SetProperty(singleP[0], singleP[1]);
            } catch (const std::string &message) {
                log_error(message);
                exit(0);
            }
        }
        auto pc = new pcLazylog(load, prop);
        pc->setLatLog(options["latency"].as<std::string>());
        pc->setE2eLatLog(options["overallLat"].as<std::string>());
        pc->setTailLatLog(options["tailLat"].as<std::string>());
        pc->setOffset(options["clientOffset"].as<int>());
        // if (options["consumeMode"].as<std::string>() == "p" || options["consumeMode"].as<std::string>() == "f") {  
        //     pc->setE2eLatLog(options["overallLat"].as<std::string>());
        // } else if (options["consumeMode"].as<std::string>() == "s") {
        //     pc->setE2eLatLog(options["overallLat"].as<std::string>());
        // }
        pc->run(options["consumeMode"].as<std::string>());
        delete pc;
    } else {
        log_error("not implemented");
        // Properties prop;
        // for (auto pr : options["lazypropertyfile"].as<std::vector<std::string>>()) {
        //     std::ifstream input(pr);
        //     log_info("loading property file " + pr);
        //     try {
        //         prop.Load(input);
        //     } catch (const std::string &message) {
        //         log_error(message);
        //         exit(0);
        //     }
        // }
        // for (auto pr : options["lazyproperty"].as<std::vector<std::string>>()) {
        //     auto singleP = separateString(pr, '=');
        //     try {
        //         log_info("setting property " + singleP[0] + "=" + singleP[1]);
        //         prop.SetProperty(singleP[0], singleP[1]);
        //     } catch (const std::string &message) {
        //         log_error(message);
        //         exit(0);
        //     }
        // }
        // auto spec = new specLazylog(load, prop);
        // if (options["consumeMode"].as<std::string>() == "p") {
        //     spec->setLatLog(options["latency"].as<std::string>());
        //     spec->setSpecLatLog(options["tailLat"].as<std::string>());
        //     spec->setE2eLatLog(options["overallLat"].as<std::string>());
        //     spec->setVeriLatLog(options["veriLat"].as<std::string>());
        //     spec->setTailLen(options["tailLen"].as<std::string>());
        // } else {
        //     spec->setLatLog(options["latency"].as<std::string>());
        //     spec->setSpecLatLog(options["tailLat"].as<std::string>());
        //     spec->setOverallLatLog(options["overallLat"].as<std::string>());
        //     spec->setVeriLatLog(options["veriLat"].as<std::string>());
        //     spec->setTailLen(options["tailLen"].as<std::string>());
        // }
        // spec->run(options["consumeMode"].as<std::string>());
        // delete spec;
    }

    return 0;
}