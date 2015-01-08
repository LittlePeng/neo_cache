-module(neo_cache_app).

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() ->
    application:start(neo_cache).

start(_StartType, _StartArgs) ->
    neo_cache_sup:start_link().

stop(_State) ->
    ok.
