-module(neo_cache_sup).
-include("cache.hrl").
-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(WORKER_CHILD(I, Type, Args), {I, {Type, start_link, Args}, permanent, 5000, worker, [Type]}).
-define(SUP_CHILD(I, Type, Args), {I, {Type, start_link, Args}, permanent, 5000, supervisor, [Type]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    DBSpec = [?WORKER_CHILD(cache_db:database(Index), cache_db, [Index]) || Index <- lists:seq(0, ?DB_COUNT-1)],
    TimeSpec = ?WORKER_CHILD(cache_time, cache_time, []),
    ClientSpec = ?SUP_CHILD(cache_client_sup, cache_client_sup, []),
    CSpec = ?WORKER_CHILD(neo_counter, neo_counter, [[neo_cache, cache_db]]),

    Port = application:get_env(neo_cache, port, 6379),
    LSpec = ?WORKER_CHILD(cache_listener, cache_listener, [Port]),

    {ok, {{one_for_one, 5, 10}, DBSpec ++ [TimeSpec,  ClientSpec, CSpec, LSpec]}}.

