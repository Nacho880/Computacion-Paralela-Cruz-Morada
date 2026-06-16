#pragma once

#include <string>
#include <vector>

using namespace std;

struct SftpConfig {
    string host;
    int port;
    string user;
    string password;
    string remote_pattern;
    string local_dir;
};

vector<string> sftp_download_reports(const SftpConfig& config);
