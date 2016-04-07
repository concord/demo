#pragma once
#include <string>
#include <queue>
#include <memory>
#include <cassandra.h>
#include <stdio.h>

namespace concord {
/// Automatic memory management for some common Cass* types
using CassandraFuture =
  std::unique_ptr<CassFuture, decltype(&cass_future_free)>;
using CassandraCluster =
  std::unique_ptr<CassCluster, decltype(&cass_cluster_free)>;
using CassandraSession =
  std::unique_ptr<CassSession, decltype(&cass_session_free)>;
using CassandraStatement =
  std::unique_ptr<CassStatement, decltype(&cass_statement_free)>;

CassandraStatement createStatement(CassStatement *statement) {
  return CassandraStatement(statement, cass_statement_free);
}
CassandraFuture createFuture(CassFuture *future) {
  return CassandraFuture(future, cass_future_free);
}
CassandraCluster createCluster() {
  return CassandraCluster(cass_cluster_new(), cass_cluster_free);
}
CassandraSession createSession() {
  return CassandraSession(cass_session_new(), cass_session_free);
}

class CassandraClient {
  public:
  CassandraClient(const std::string &contactPoints, const std::string &keyspace)
    : keyspace_(keyspace) {
    // Cannot construct connectFuture_ in initializer list
    connectFuture_ = std::move(connectToCluster(contactPoints));
    connected_ = (cass_future_error_code(connectFuture_.get()) == CASS_OK);
  }

  virtual ~CassandraClient() {
    if(connected_) {
      cass_future_wait(createFuture(cass_session_close(session_.get())).get());
    }
  }

  bool isConnected() const { return connected_; }

  /// Blocks on queued futures created by 'asyncInsert'
  void wait() {
    if(!connected_) {
      LOG(ERROR) << "wait failed, establish connection to cassandra first";
      return;
    }
    while(!futures_.empty()) {
      CassFuture *future = futures_.front().get();
      cass_future_wait(future);
      const auto rc = cass_future_error_code(future);
      if(rc != CASS_OK) {
        LOG(ERROR) << "wait failed: " << getFutureError(future);
      }
      futures_.pop(); // Deallocates 'future' var memory
    }
  }

  /// ATM only supports (key, value) inserts
  void asyncInsert(const std::string &table,
                   const std::string &key,
                   const std::string &value) {
    if(!connected_) {
      LOG(ERROR)
        << "asyncInsert failed, establish connection to cassandra first";
      return;
    } else if(futures_.size() > kMaxNumQueuedFutures) {
      VLOG(0) << "asyncInsert, queue has reached capacity, draining...";
      wait();
    }
    const auto tableName = keyspace_ + "." + table;
    const auto query("INSERT INTO " + tableName + " (key, value) "
                     + "VALUES (?, ?);");
    auto statement = createStatement(cass_statement_new(query.c_str(), 2));
    cass_statement_bind_string(statement.get(), 0, key.data());
    cass_statement_bind_string(statement.get(), 1, value.data());

    futures_.emplace(cass_session_execute(session_.get(), statement.get()),
                     cass_future_free);
  }

  private:
  static std::string getFutureError(CassFuture *future) {
    const char *message;
    size_t message_length;
    cass_future_error_message(future, &message, &message_length);
    return std::string(message, message_length);
  }

  CassandraFuture connectToCluster(const std::string &contactPoints) const {
    /* Add contact points */
    cass_cluster_set_contact_points(cluster_.get(), contactPoints.c_str());

    /* Provide the cluster object as configuration to connect the session */
    return createFuture(cass_session_connect(session_.get(), cluster_.get()));
  }

  // Queued INSERT statements which may or may not have completed
  std::queue<CassandraFuture> futures_;

  // Could be const if connectFuture_ could be constructed in initlist
  bool connected_{false};
  CassandraFuture connectFuture_{createFuture(NULL)};

  const std::string keyspace_;
  const CassandraCluster cluster_{createCluster()};
  const CassandraSession session_{createSession()};
  static const size_t kMaxNumQueuedFutures = 10000;
};
}
