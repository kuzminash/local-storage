#include "kv.pb.h"
#include "log.h"
#include "protocol.h"
#include "rpc.h"

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>
#include <unordered_map>

#include <chrono>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <unistd.h>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <vector>
#include <fstream>
#include <thread>

static_assert(EAGAIN == EWOULDBLOCK);

using namespace NLogging;
using namespace NProtocol;
using namespace NRpc;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int max_events = 32;

////////////////////////////////////////////////////////////////////////////////

auto create_and_bind(std::string const& port)
{
    struct addrinfo hints;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC; /* Return IPv4 and IPv6 choices */
    hints.ai_socktype = SOCK_STREAM; /* TCP */
    hints.ai_flags = AI_PASSIVE; /* All interfaces */

    struct addrinfo* result;
    int sockt = getaddrinfo(nullptr, port.c_str(), &hints, &result);
    if (sockt != 0) {
        LOG_ERROR("getaddrinfo failed");
        return -1;
    }

    struct addrinfo* rp = nullptr;
    int socketfd = 0;
    for (rp = result; rp != nullptr; rp = rp->ai_next) {
        socketfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (socketfd == -1) {
            continue;
        }

        sockt = bind(socketfd, rp->ai_addr, rp->ai_addrlen);
        if (sockt == 0) {
            break;
        }

        close(socketfd);
    }

    if (rp == nullptr) {
        LOG_ERROR("bind failed");
        return -1;
    }

    freeaddrinfo(result);

    return socketfd;
}

////////////////////////////////////////////////////////////////////////////////

auto make_socket_nonblocking(int socketfd)
{
    int flags = fcntl(socketfd, F_GETFL, 0);
    if (flags == -1) {
        LOG_ERROR("fcntl failed (F_GETFL)");
        return false;
    }

    flags |= O_NONBLOCK;
    int s = fcntl(socketfd, F_SETFL, flags);
    if (s == -1) {
        LOG_ERROR("fcntl failed (F_SETFL)");
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

SocketStatePtr accept_connection(
    int socketfd,
    struct epoll_event& event,
    int epollfd)
{
    struct sockaddr in_addr;
    socklen_t in_len = sizeof(in_addr);
    int infd = accept(socketfd, &in_addr, &in_len);
    if (infd == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return nullptr;
        } else {
            LOG_ERROR("accept failed");
            return nullptr;
        }
    }

    std::string hbuf(NI_MAXHOST, '\0');
    std::string sbuf(NI_MAXSERV, '\0');
    auto ret = getnameinfo(
        &in_addr, in_len,
        const_cast<char*>(hbuf.data()), hbuf.size(),
        const_cast<char*>(sbuf.data()), sbuf.size(),
        NI_NUMERICHOST | NI_NUMERICSERV);

    if (ret == 0) {
        LOG_INFO_S("accepted connection on fd " << infd
            << "(host=" << hbuf << ", port=" << sbuf << ")");
    }

    if (!make_socket_nonblocking(infd)) {
        LOG_ERROR("make_socket_nonblocking failed");
        return nullptr;
    }

    event.data.fd = infd;
    event.events = EPOLLIN | EPOLLOUT | EPOLLET;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, infd, &event) == -1) {
        LOG_ERROR("epoll_ctl failed");
        return nullptr;
    }

    auto state = std::make_shared<SocketState>();
    state->fd = infd;
    return state;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct Value
{
    uint64_t X = 0;
    uint64_t V = 0;
};

struct Table
{
    std::string tab_file;
    std::string log_file;
    std::unordered_map<std::string, Value> data;

    std::ofstream log;

    void init(std::string path_prefix)
    {
        tab_file = path_prefix + ".tab";
        log_file = path_prefix + ".log";
    }

    void reopen_log()
    {
        log.open(log_file, std::ofstream::out | std::ofstream::trunc);
    }
};

class MyCoolHashTable {
public:
    static constexpr uint32_t tab_count = 2;
    std::array<Table, tab_count> tables;
    mutable std::mutex mutex;
    uint32_t active_tab_idx = 0;
    uint64_t key_version = 0;

    std::thread syncer;
    bool running = true;

    Table& active()
    {
        return tables[active_tab_idx % tab_count];
    }

    Table& inactive()
    {
        return tables[(active_tab_idx - 1) % tab_count];
    }

    const Table& active() const
    {
        return tables[active_tab_idx % tab_count];
    }

    const Table& inactive() const
    {
        return tables[(active_tab_idx - 1) % tab_count];
    }

    void threadFunc() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));

            {
                std::lock_guard<std::mutex> g(mutex);
                ++active_tab_idx;
            }

            writeTable(inactive());
        }
    }

    /*
     * Восстанавливаем данные, если все упало
     * Сначала читаем таблицу, а после логи
     */
    void restore(Table& tab) {
        std::string line, key;
        uint64_t value;
        uint64_t version;
        std::ifstream tableStreamIn(tab.tab_file);
        if (tableStreamIn.is_open()) {
            while (getline(tableStreamIn, line)) {
                std::stringstream in(line);
                in >> key >> value >> version;
                tab.data[key] = {value, version};
                key_version = std::max(version, key_version);
            }
        }
        tableStreamIn.close();

        std::ifstream logStreamIn(tab.log_file);
        if (logStreamIn.is_open()) {
            while (getline(logStreamIn, line)) {
                std::stringstream in(line);
                in >> key >> value >> version;
                tab.data[key] = {value, version};
                key_version = std::max(version, key_version);
            }
        }
        logStreamIn.close();
    }

    MyCoolHashTable()
    {
        tables[0].init("tab0");
        tables[1].init("tab1");

        for (auto& tab: tables) {
            restore(tab);
        }

        active().reopen_log();

        syncer = std::thread([this] {
            threadFunc();
        });
    }

    /*
     * При вызове деструктора - записываем логи в файл
     */
    ~MyCoolHashTable() {
        running = false;
        syncer.join();
    }

    std::pair<bool, uint64_t> find(const std::string &key) const {
        std::lock_guard<std::mutex> g(mutex);
        Value value;
        for (const auto& table: tables) {
            const auto it = table.data.find(key);
            if (it != table.data.end() && it->second.V > value.V) {
                value = it->second;
            }
        }

        return std::make_pair(value.V != 0, value.X);
    }

    void insert(const std::string &key, const uint64_t &value) {
        std::lock_guard<std::mutex> g(mutex);
        ++key_version;
        auto& tab = active();
        tab.log << key << ' ' << value << ' ' << key_version << '\n';
        // TODO fsync somewhere here or after processing a batch of writes
        tab.data[key] = {value, key_version};
    }

    /*
     * Каждые сколько-то секунд будем записывать таблицу в файл.
     * В таком случае очищаем логи так как они нам больше не нужны
     */
    void writeTable(Table& tab) {
        std::ofstream tableStream;
        tableStream.open(tab.tab_file, std::ofstream::out | std::ofstream::trunc);

        for (auto& it: tab.data) {
            tableStream << it.first
                << ' ' << it.second.X
                << ' ' << it.second.V
                << '\n';
        }

        tableStream.close();
        tab.reopen_log();
    }
};


////////////////////////////////////////////////////////////////////////////////


int main(int argc, const char** argv)
{
    if (argc < 2) {
        return 1;
    }

    /*
     * socket creation and epoll boilerplate
     * TODO extract into struct Bootstrap
     */

    auto socketfd = ::create_and_bind(argv[1]);
    if (socketfd == -1) {
        return 1;
    }

    if (!::make_socket_nonblocking(socketfd)) {
        return 1;
    }

    if (listen(socketfd, SOMAXCONN) == -1) {
        LOG_ERROR("listen failed");
        return 1;
    }

    int epollfd = epoll_create1(0);
    if (epollfd == -1) {
        LOG_ERROR("epoll_create1 failed");
        return 1;
    }

    struct epoll_event event;
    event.data.fd = socketfd;
    event.events = EPOLLIN | EPOLLET;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, socketfd, &event) == -1) {
        LOG_ERROR("epoll_ctl failed");
        return 1;
    }

    /*
     * handler function
     */

    // TODO on-disk storage
    MyCoolHashTable table;

    auto handle_get = [&] (const std::string& request) {
        NProto::TGetRequest get_request;
        if (!get_request.ParseFromArray(request.data(), request.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("get_request: " << get_request.ShortDebugString());

        NProto::TGetResponse get_response;
        get_response.set_request_id(get_request.request_id());

        std::pair<bool, uint64_t> it = table.find(get_request.key());
        if (it.first) {
            get_response.set_offset(it.second);
        }

        std::stringstream response;
        serialize_header(GET_RESPONSE, get_response.ByteSizeLong(), response);
        get_response.SerializeToOstream(&response);

        return response.str();
    };

    auto handle_put = [&] (const std::string& request) {
        NProto::TPutRequest put_request;
        if (!put_request.ParseFromArray(request.data(), request.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("put_request: " << put_request.ShortDebugString());
        table.insert(put_request.key(), put_request.offset());

        NProto::TPutResponse put_response;
        put_response.set_request_id(put_request.request_id());

        std::stringstream response;
        serialize_header(PUT_RESPONSE, put_response.ByteSizeLong(), response);
        put_response.SerializeToOstream(&response);

        return response.str();
    };

    Handler handler = [&] (char request_type, const std::string& request) {
        switch (request_type) {
            case PUT_REQUEST: return handle_put(request);
            case GET_REQUEST: return handle_get(request);
        }

        // TODO proper handling

        abort();
        return std::string();
    };

    /*
     * rpc state and event loop
     * TODO extract into struct Rpc
     */

    std::array<struct epoll_event, ::max_events> events;
    std::unordered_map<int, SocketStatePtr> states;

    auto finalize = [&] (int fd) {
        LOG_INFO_S("close " << fd);

        close(fd);
        states.erase(fd);
    };

    while (true) {
        const auto n = epoll_wait(epollfd, events.data(), ::max_events, -1);

        {
            LOG_INFO_S("got " << n << " events");
        }

        for (int i = 0; i < n; ++i) {
            const auto fd = events[i].data.fd;

            if (events[i].events & EPOLLERR
                    || events[i].events & EPOLLHUP
                    || !(events[i].events & (EPOLLIN | EPOLLOUT)))
            {
                LOG_ERROR_S("epoll event error on fd " << fd);

                finalize(fd);

                continue;
            }

            if (socketfd == fd) {
                while (true) {
                    auto state = ::accept_connection(socketfd, event, epollfd);
                    if (!state) {
                        break;
                    }

                    states[state->fd] = state;
                }

                continue;
            }

            if (events[i].events & EPOLLIN) {
                auto state = states.at(fd);
                if (!process_input(*state, handler)) {
                    finalize(fd);
                }
            }

            if (events[i].events & EPOLLOUT) {
                auto state = states.at(fd);
                if (!process_output(*state)) {
                    finalize(fd);
                }
            }
        }
    }

    LOG_INFO("exiting");

    close(socketfd);

    return 0;
}
