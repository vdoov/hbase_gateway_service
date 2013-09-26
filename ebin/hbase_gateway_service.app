{application, hbase_gateway_service,
 [
  {description, "Apache Thrift Gateway to HBase"},
  {vsn, "1"},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {modules, [
    hbase_gateway_service_app,
    hbase_gateway_service_sup,
    hbase_gateway_service,
    hbase_thrift,
    hbase_types
  ]},
  {mod, { hbase_gateway_service_app, []}},
  {env, []}
 ]}.
