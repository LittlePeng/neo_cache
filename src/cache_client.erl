-module(cache_client).
-include("cache.hrl").
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([terminate/2, code_change/3]).

-record(state, {peer, socket}).

start_link(PeerEp, Socket) ->
    gen_server:start_link(?MODULE, [PeerEp, Socket], []).

init([PeerEp, Socket]) ->
    cache_parser:init(),
    neo_counter:inc(neo_cache, connection),
    {ok, #state{peer=PeerEp, socket=Socket}}.

handle_call(info, _From, State) ->
    {reply, State, State};
handle_call(_Req, _From, State) ->
    {reply, not_support, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data}, #state{socket=Socket}=State) ->
    case process_packet(Data, State) of
        {ok, State1} ->
            inet:setopts(Socket, [{active, once}]),
            {noreply, State1};
        {error, Reason} ->
            lager:warning("~p", [Reason]),
            {stop, normal, State}
    end;
handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};
handle_info({tcp_error, _Socket, _Reason}, State) ->
    {stop, normal, State};
handle_info(_Req, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

process_packet(Bin, State) ->
    lager:debug("Bin:~p", [Bin]),
    case cache_parser:parser(Bin) of
        {ok, []} ->
            {ok, State};
        {ok, [[Cmd | ReqArgs]]} ->
            neo_counter:inc(neo_cache, reqs),
            process_request([cache_util:upper(Cmd) | ReqArgs], State);
        {ok, Reqs} ->
            % pipeline process
            put(pipeline, []),
            neo_counter:inc(neo_cache, reqs, length(Reqs)),
            [process_request([cache_util:upper(Cmd) | ReqArgs], State) ||
                [Cmd | ReqArgs] <- Reqs],
            tcp_flush(State);
           PErr -> PErr
    end.

process_request([], State) ->
    {ok, State};
process_request([<<"QUIT">>], _State) ->
    {error, quit};
process_request([<<"APPEND">>, Key, Field, Value, Version0, PerVersion0], State) ->
    Version = binary_to_integer(Version0),
    PerVersion = binary_to_integer(PerVersion0),
    Len = cache_db:append(Key, Field, Value, Version, PerVersion),
    tcp_string(integer_to_list(Len), State);
process_request([<<"FETCH">>, Key], State) ->
    process_request([<<"FETCH">>, Key, <<"0">>, infinite], State);
process_request([<<"FETCH">>, Key, Vmin], State) ->
    process_request([<<"FETCH">>, Key, Vmin, infinite], State);
process_request([<<"FETCH">>, Key, Vmin0, Vmax0], State) ->
    neo_counter:inc(neo_cache, fetch),
    Vmin = binary_to_integer(Vmin0),
    Vmax = case is_binary(Vmax0) of
        true -> binary_to_integer(Vmax0);
        false -> Vmax0
    end,
    %[[Field, IsFull, [[Version, PreVersion,  Val] | ...]] | ....]
    DList = cache_db:fetch(Key, Vmin, Vmax),
    tcp_multi_bulk(DList, State);
process_request([<<"DEL">>, Key], State) ->
    neo_counter:inc(neo_cache, del),
    OK = cache_db:del(Key),
    tcp_string(integer_to_list(OK), State);
process_request([<<"LLEN">>, Key], State) ->
    Len = cache_db:llen(Key),
    tcp_string(integer_to_list(Len), State);
process_request([<<"TTL">>, Key], State) ->
    Ttl = cache_db:ttl(Key),
    tcp_string(integer_to_list(Ttl), State);
process_request([<<"DUMP">>, Key], State) ->
    Obj = cache_db:dump(Key),
    R = io_lib:format("~p", [Obj]),
    tcp_string(R, State);
process_request([<<"RANDOMKEY">>], State) ->
    tcp_string(cache_db:random_key(), State);
process_request([<<"FLUSHALL">>], State) ->
    cache_db:flush_all(),
    tcp_ok(State);
process_request([<<"INFO">>], State) ->
    Mem = erlang:memory(total) div (1024*1024),
    DBInfos =
    lists:foldl(fun(DB, Dict0) ->
                lists:foldl(fun({Key, Val}, Dict) ->
                            dict:update_counter(Key, Val, Dict)
                    end, Dict0, DB)
        end, dict:new(),  cache_db:info()),
    Info = io_lib:format("~p~n~4096p~n~4096p~n",
                         [neo_counter:i(), dict:to_list(DBInfos), [{mem, Mem}]]),
    tcp_string(Info, State);

process_request([<<"INFO">>, <<"db">>], State) ->
    Dbs = io_lib:format("~4096p~n", [cache_db:info()]),
    tcp_string(Dbs, State);
%% test cmd
process_request([<<"GET">>, Key], State) ->
    tcp_string(Key, State);
process_request([<<"SET">>, _Key, _Value], State) ->
    tcp_ok(State);

process_request(Req0, State) ->
    Req = [[<<" ">>, Arg] || Arg <- Req0],
    tcp_err(["unsupport: " | Req], State).

tcp_err(Message, State) ->
    tcp_send(["-ERR ", Message, "\r\n"], State).

tcp_ok(State) ->
    tcp_string("OK", State).

tcp_string(undefined, State) ->
    tcp_send(["$-1\r\n"], State);
tcp_string(Message, State) ->
    tcp_send(["+", Message, "\r\n"], State).

tcp_multi_bulk(Messages, State) ->
    tcp_send(create_multi_bulk(Messages), State).

create_multi_bulk([]) ->
    ["*0\r\n"];
create_multi_bulk(Messages) ->
    Bulk1 = lists:map(
        fun(Val) when is_integer(Val) ->
                [":", integer_to_list(Val), "\r\n"];
           (Val) when is_list(Val) ->
                create_multi_bulk(Val);
           (Val) ->
                ["$", integer_to_list(iolist_size(Val)), "\r\n", Val, "\r\n"]
        end, Messages),
    ["*", integer_to_list(length(Messages)), "\r\n" | Bulk1].

tcp_flush(State) ->
    case erase(pipeline) of
        undefined ->
            {ok, State};
        Pipes ->
            tcp_send(lists:reverse(Pipes), State)
    end.

tcp_send(Message, State) ->
    case get(pipeline) of
        undefined ->
            lager:debug("~p << ~s~n", [State#state.peer, Message]),
            case gen_tcp:send(State#state.socket, Message) of
                ok -> {ok, State};
                Error ->
                    lager:error("rsp error", [Error]),
                    {error, Error}
            end;
        Pipes ->
            put(pipeline, [Message | Pipes]),
            {ok, State}
    end.
