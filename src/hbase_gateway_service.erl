%%%-------------------------------------------------------------------
%%% @author Alex Vdovin <2v2odmail@gmail.com>
%%% @copyright 2013 Alex Vdovin
%%% @doc HBase Gateway Service
%%% Gateway to HBase DB via Apache Thrift
%%% @end
%%%-------------------------------------------------------------------
-module(hbase_gateway_service).

-behaviour(gen_server).

-include("hbase_types.hrl").

%% API
-export([
    start_link/0,
    stop/0,
    batch_insert/5,
    direct_write/5,
    get_records_cnt/0,
    initialize_connection/2,
    ensure_table_exists/2
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
  terminate/2, code_change/3]).
  
-record(state, {transport_factory=undefined, protocol_factory=undefined, protocol=undefined, client=undefined, records_since_last_metrics_call=0}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Establish initial connection to thrift gateway server
%% @spec initialize_connection(ThriftHost::binary_stirng, ThriftPort::integer) -> ok
%% @end
%%--------------------------------------------------------------------
initialize_connection(ThriftHost, ThriftPort) ->
  gen_server:call(?MODULE, {initialize_connection, ThriftHost, ThriftPort}),
  ok.

%%--------------------------------------------------------------------
%% @doc Returns the number of records stored in HBase since last metrics read.
%% Resets the counter after this reading.
%% @spec get_records_cnt() -> Num::integer()
%% @end
%%--------------------------------------------------------------------
get_records_cnt()->
  gen_server:call(?MODULE, get_records_cnt).

%%--------------------------------------------------------------------
%% @doc Store multiple number of messages in the same transaction
%%
%% @spec batch_insert(Messages::list(), Table::binary(), Column::list(), KeyGenFun::fun(), ValueGenFun::fun()) -> ok
%% @end
%%--------------------------------------------------------------------

batch_insert(Messages, Table, Column, KeyGenFun, ValueGenFun) ->
  gen_server:call(?MODULE, {batch_insert, Messages, Table, Column, KeyGenFun, ValueGenFun}),
  ok.

direct_write(Message, Table, Column, KeyGenFun, ValueGenFun) ->
  gen_server:call(?MODULE, {direct_write, Message, Table, Column, KeyGenFun, ValueGenFun}).

%%--------------------------------------------------------------------
%% @doc Starts the server.
%%
%% @spec start_link() -> {ok, Pid}
%% where
%% Pid = pid()
%% @end
%%--------------------------------------------------------------------

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc Stops the server.
%% @spec stop() -> ok
%% @end
%%--------------------------------------------------------------------
stop() ->
  gen_server:cast(?MODULE, stop).

%%--------------------------------------------------------------------
%% @doc Check if the specified table exists and create it if not. 
%% Column Family needs to be supplied
%% @spec ensure_table_exists(Table::binary(), ColumnFamily::list) -> ok
%% @end
%%--------------------------------------------------------------------
ensure_table_exists(Table, ColumnFamily) ->
  gen_server:call(?MODULE, {ensure_table_exists, Table, ColumnFamily}).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

ensure_list(Val) when is_list(Val) ->
  Val;
ensure_list(Val) when is_binary(Val) ->
  binary_to_list(Val).

create_table(ThriftClient, Table, ColumnFamily) ->
  ColumnFamilies = [
    #columnDescriptor{name=ColumnFamily}
  ],
  {TC1, _Result} = thrift_client:call(ThriftClient, createTable, [ensure_list(Table), ColumnFamilies]),
  TC1.
  
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
  State = #state{},
  {ok, State, infinity}.

handle_call({initialize_connection, ThriftHost, ThriftPort}, _From, State) ->
  lager:info("Establishing connection to Thrift Gateway on ~p:~p", [ThriftHost, ThriftPort]),
  {ok, TFactory} = thrift_socket_transport:new_transport_factory(ThriftHost, ThriftPort, []),
  {ok, PFactory} = thrift_binary_protocol:new_protocol_factory(TFactory, []),
  {ok, Protocol} = PFactory(),
  {ok, Client} = thrift_client:new(Protocol, hbase_thrift),
  NewState = State#state{transport_factory=TFactory, protocol_factory=PFactory, protocol=Protocol, client=Client},
  {reply, ok, NewState};

handle_call({ensure_table_exists, Table, ColumnFamily}, _From, #state{client=ThriftClient} = State) ->
  %%Query hbase for the list of known tables:
  {TC1, {ok, Tables}} = thrift_client:call(ThriftClient, getTableNames, []),
  NewClient = case lists:member(Table, Tables) of
    true ->
      %%Table already exists, nothing to do here:
      TC1;
    false ->
      %% DLog table does not exists in the HBase. Need to create it:
      create_table(TC1, Table, ColumnFamily)
  end,
  {reply, ok, State#state{client=NewClient}};

handle_call({batch_insert, Messages, Table, Column, KeyGenFun, ValueGenFun}, _From, #state{client=ThriftClient, records_since_last_metrics_call=OldRecordsCnt} = State) ->
  
  Batch = lists:map(fun(Message)-> 
    Key = KeyGenFun(Message),
    Value = ValueGenFun(Message),
    #batchMutation{row=Key, mutations=[#mutation{isDelete=false, column=Column, value=Value}]}
  end, Messages),
  
  {UpdatedThriftClient, _Result} = thrift_client:call(ThriftClient, mutateRows, [ensure_list(Table), Batch, dict:new()]),
  NewState = State#state{client=UpdatedThriftClient, records_since_last_metrics_call=OldRecordsCnt + length(Messages)},  
  {reply, ok, NewState};
  
handle_call({direct_write, Message, Table, Column, KeyGenFun, ValueGenFun }, _From, #state{client=ThriftClient, records_since_last_metrics_call=OldRecordsCnt} = State) ->
  Key = KeyGenFun(Message),
  Value = ValueGenFun(Message),
  {UpdatedThriftClient, _Result} = thrift_client:call(ThriftClient, mutateRow, [ensure_list(Table), Key, [#mutation{isDelete=false,column=Column, value=Value}], dict:new()]),
  NewState = State#state{client=UpdatedThriftClient, records_since_last_metrics_call=OldRecordsCnt + 1},  
  {reply, ok, NewState};

handle_call(get_records_cnt, _From, #state{records_since_last_metrics_call=RecordsCnt}=State) ->
  NewState = State#state{records_since_last_metrics_call=0},
  {reply, {ok, RecordsCnt}, NewState};

handle_call(_Request, _From, State) ->
  {noreply, State}.

handle_cast(stop, State) ->
  {stop, normal, State};
    
handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(timeout, State) ->
  {noreply, State, infinity}.
  
terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.