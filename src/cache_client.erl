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
    case cache_parser:parser(Bin) of
        {ok, Reqs} ->
           lager:debug("bin:~p.req:~p", [Bin, Reqs]),
           lists:foldl(
                fun([Cmd | ReqArgs], {ok, State0}) ->
                        Req = [cache_util:upper(Cmd) | ReqArgs],
                        process_request(Req, State0);
                   (_Req, Err) ->
                        Err
                end, {ok, State}, Reqs);
        PErr ->
            PErr
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
process_request([<<"FETCH">>, Key, Vmin0, Vmax0], State) ->
    Vmin = binary_to_integer(Vmin0),
    Vmax = binary_to_integer(Vmax0),
    %format Full:1 Field\r\n Version:8 PreVersion:8 ValLen:4 Val(... list)
    DList = cache_db:fetch(Key, Vmin, Vmax),
    Rsp = lists:flatmap(fun({Field0, Full, List}) ->
                        ListBin = lists:map(fun({Value, Version, PerVersion}) ->
                                        <<Version:64/little, PerVersion:64/little,
                                          (size(Value)):32/little, Value/binary>>
                                end, List),
                        Field = <<(neo_util:to_binary(Full))/binary,
                                  (neo_util:to_binary(Field0))/binary>>,
                        [Field, ListBin]
                end, DList),
    tcp_multi_bulk(Rsp, State);
process_request([<<"DEL">>, Key], State) ->
    OK = cache_db:del(Key),
    tcp_string(integer_to_list(OK), State);
process_request([<<"INFO">>], State) ->
    Info = io_lib:format("~p~n~4096p", [neo_counter:i(), cache_db:info()]),
    tcp_string(Info, State);

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
    tcp_string("OK\r\n", State).

tcp_string(Message, State) ->
    tcp_send(["+", Message, "\r\n"], State).

tcp_multi_bulk([], State) ->
    tcp_send("*0\r\n", State);
tcp_multi_bulk(Rsp, State) ->
    Len = integer_to_list(length(Rsp)),
    Rsp1 = [["$", integer_to_list(iolist_size(Field)), "\r\n", Field, "\r\n"] || Field <- Rsp],
    Rsp2 = ["*", Len, "\r\n" | Rsp1],
    io:format("~p", [Rsp2]),
    tcp_send(Rsp2, State).

tcp_send(Message, State) ->
    lager:debug("~p << ~s~n", [State#state.peer, Message]),
    case gen_tcp:send(State#state.socket, Message) of
        ok -> {ok, State};
        Error ->
            lager:error("rsp error", [Error])
    end.
