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
                fun(Req, {ok, State0}) ->
                        process_request(Req, State0);
                   (_Req, Err) ->
                        Err
                end, {ok, State}, Reqs);
        PErr ->
            PErr
    end.

process_request([], State) ->
    {ok, State};
process_request([<<"quit">>], _State) ->
    {error, quit};
process_request([<<"fetch">>, _Vmin, _Vmax], State) ->
    {ok, State};
process_request([<<"get">>, Key], State) ->
    tcp_string(Key, State);
process_request([<<"set">>, _Key, _Value], State) ->
    tcp_ok(State);
process_request([<<"info">>], State) ->
    Info = io_lib:format("~p~n~4096p", [neo_counter:i(), cache_db:info()]),
    tcp_string(Info, State);
process_request(Req, State) ->
    tcp_err(["not support request: " | Req], State).

tcp_err(Message, State) ->
      tcp_send(["-ERR ", Message], State).

tcp_ok(State) ->
    tcp_string("OK", State).

tcp_string(Message, State) ->
    tcp_send(["+", Message], State).

tcp_send(Message, State) ->
    lager:debug("~p << ~s~n", [State#state.peer, Message]),
    try gen_tcp:send(State#state.socket, [Message, "\r\n"]) of
        ok -> {ok, State};
        {error, closed} ->
            lager:debug("Connection closed~n", []),
            {error, closed};
        {error, Error} ->
            lager:error("Couldn't send msg through TCP~n\tError: ~p~n", [Error]),
            {error, Error}
    catch
        _:Exception ->
            lager:error("Couldn't send msg through TCP~n\tError: ~p~n", [Exception]),
            {error, Exception}
    end.
