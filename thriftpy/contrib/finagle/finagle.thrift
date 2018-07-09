include "tracing/tracing.thrift"

service FinagleUpgradeService {
    tracing.UpgradeReply __can__finagle__trace__v3__(1: tracing.ConnectionOptions ConnectionOptions)
}
