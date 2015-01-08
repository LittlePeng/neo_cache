-module(cache_client_sup).

-behaviour(supervisor).
-export([start_link/0, start_child/2, init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(PeerEp, Socket) ->
  supervisor:start_child(?MODULE, [PeerEp, Socket]).

init([]) ->
    Specs = {?MODULE, {cache_client, start_link, []},
             temporary, brutal_kill, worker, [?MODULE]},
  {ok, {{simple_one_for_one, 0, 1}, [Specs]}}.
