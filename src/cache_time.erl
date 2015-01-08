-module(cache_time).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([terminate/2, code_change/3]).
-export([now/0]).

-record(state, {}).
%%% 不是很精确的unix 时间(高效)

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

now() ->
    case ets:lookup(?MODULE, now) of
        [{now, Now}] -> Now;
        _ -> cache_util:now()
    end.

init([]) ->
    process_flag(priority, high),
    ets:new(?MODULE, [named_table, set, protected, {read_concurrency, true}]),
    loop_update_time(),
    {ok, #state{}}.

handle_call(_Req, _From, State) ->
    {reply, not_support, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(update_time, State) ->
    loop_update_time(),
    {noreply, State};
handle_info(_Req, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

loop_update_time() ->
    ets:insert(?MODULE, {now, cache_util:now()}),
    erlang:send_after(500, self(), update_time).
