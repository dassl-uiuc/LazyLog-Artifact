#pragma once

#include <string.h>

#include <algorithm>
#include <exception>
#include <string>

namespace lazylog {

class Exception : public std::exception {
   public:
    Exception(const std::string &message) : message_(message) {}
    const char *what() const noexcept { return message_.c_str(); }

   private:
    std::string message_;
};

inline bool StrToBool(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    if (str == "true" || str == "1") {
        return true;
    } else if (str == "false" || str == "0") {
        return false;
    } else {
        throw Exception("Invalid bool string: " + str);
    }
}

inline std::string Trim(const std::string &str) {
    auto front = std::find_if_not(str.begin(), str.end(), [](int c) { return std::isspace(c); });
    return std::string(front, std::find_if_not(str.rbegin(), std::string::const_reverse_iterator(front), [](int c) {
                                  return std::isspace(c);
                              }).base());
}

inline bool StrStartWith(const char *str, const char *pre) {
    return strncmp(str, pre, strlen(pre)) == 0;
}

}  // namespace lazylog
