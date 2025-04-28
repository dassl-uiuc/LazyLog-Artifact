#pragma once

#include <string>

#include "log_entry.h"

namespace lazylog {

const std::string PROP_DL_SVR_URI = "dur_log.server_uri";
const std::string PROP_DL_SVR_URI_DEFAULT = "localhost:31850";

const std::string PROP_DL_PRI_URI = "dur_log.primary_uri";
const std::string PROP_DL_PRI_URI_DEFAULT = "localhost:31850";

const std::string PROP_DL_CLI_URI = "dur_log.client_uri";
const std::string PROP_DL_CLI_URI_DEFAULT = "localhost:31851";

const std::string PROP_DL_MSG_SIZE = "dur_log.msg_size";
const std::string PROP_DL_MSG_SIZE_DEFAULT = "8192";

const std::string PROP_CL_SVR_URI = "cons_log.server_uri";
const std::string PROP_CL_SVR_URI_DEFAULT = "localhost:31850";

const std::string PROP_CL_CLI_URI = "cons_log.client_uri";
const std::string PROP_CL_CLI_URI_DEFAULT = "localhost:31851";

const std::string PROP_CL_MSG_SIZE = "cons_log.msg_size";
const std::string PROP_CL_MSG_SIZE_DEFAULT = "2048";

const std::string PROP_SHD_SVR_URI = "shard.server_uri";
const std::string PROP_SHD_SVR_URI_DEFAULT = "localhost:31860";

const std::string PROP_SHD_CLI_URI = "shard.client_uri";
const std::string PROP_SHD_CLI_URI_DEFAULT = "localhost:31861";

const std::string PROP_SHD_PRI_URI = "shard.primary_uri";
const std::string PROP_SHD_PRI_URI_DEFAULT = "localhost:31860";

const std::string PROP_SHD_MSG_SIZE = "shard.msg_size";
const std::string PROP_SHD_MSG_SIZE_DEFAULT = "2048";

const std::string PROP_SHD_BACKUP_URI = "shard.backup_uri";
const std::string PROP_SHD_BACKUP_URI_DEFAULT = "localhost:31860";

#ifdef CORFU
const std::string PROP_SHD_BACKUP_URI_2 = "shard.backup_uri_2";
#endif

const std::string PROP_SHD_FOLDER_PATH = "shard.folder_path";
const std::string PROP_SHD_FOLDER_PATH_DEFAULT = "./data/";

const std::string PROP_SHD_STRIPE_SIZE = "shard.stripe_unit_size";
const std::string PROP_SHD_STRIPE_SIZE_DEFAULT = "100000";

const uint8_t DL_CLI_RPCID_OFFSET = 0;
const uint8_t CL_CLI_RPCID_OFFSET = 64;
const uint8_t DL_SVR_RPCID_OFFSET = 128;
const uint8_t CL_SVR_RPCID_OFFSET = 192;
const uint8_t DL_RSV_RPCID = 191;
const uint8_t CL_RSV_RPCID = 254;

/* Durability Log Interfaces */
const uint8_t APPEND_ENTRY = 1;
const uint8_t ORDER_ENTRY = 2;
const uint8_t GET_N_DUR_ENTRY = 3;
const uint8_t FETCH_UNORDERED_ENTRIES = 4;
const uint8_t DEL_ORDERED_ENTRIES = 5;
const uint8_t SPEC_READ = 6;
#ifdef CORFU
const uint8_t GET_GSN = 7;
const uint8_t GET_GSN_BATCH = 8;
#endif

/* Consensus Log Interfaces */
const uint8_t DISPATCH_ENTRY = 11;
const uint8_t DISPATCH_ENTRIES = 12;
const uint8_t READ_ENTRY = 13;
const uint8_t READ_ENTRIES = 14;
const uint8_t GET_N_ORD_ENTRY = 15;

/* Data Shard Interfaces */
const uint8_t APPEND_BATCH = 21;
const uint8_t REP_BATCH = 22;
const uint8_t READ_ENTRY_BE = 23;
const uint8_t UPDATE_GLBL_IDX = 24;
#ifdef CORFU
const uint8_t READ_BATCH = 25;
const uint8_t APPEND_ENTRY_BE = 26;
const uint8_t REP_ENTRY = 27;
#endif


/* KV Application Interfaces */
const uint8_t KV_INSERT = 41;
const uint8_t KV_READ = 42;

const size_t PAGE_SIZE = 4096;
const size_t SAFE_RG_SIZE = 2 * PAGE_SIZE;

enum Status {
    OK = 0,
    ERROR = 1,
    NOENT = 2,
    NOFILE = 3,
};

}  // namespace lazylog
