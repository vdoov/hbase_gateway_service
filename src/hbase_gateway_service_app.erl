-module(hbase_gateway_service_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    hbase_gateway_service_sup:start_link().

stop(_State) ->
    ok.
