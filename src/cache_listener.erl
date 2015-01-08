-module(cache_listener).

-behaviour(gen_server).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(TCP_OPTIONS,[binary,
                     {packet, raw},
                     {keepalive, true},
                     {active, false},
                     {reuseaddr, true},
                     {nodelay, true},
                     {backlog, 128000},
                     {send_timeout, 5000},
                     {send_timeout_close, true}
                    ]).

-record(state, {listener, acceptor}).
start_link(Port) -> 
    gen_server:start_link( {local, ?MODULE}, ?MODULE, Port, []).

init(Port) ->
    process_flag(priority, high),
    case gen_tcp:listen(Port, ?TCP_OPTIONS) of
        {ok, Socket} ->
            {ok, Ref} = prim_inet:async_accept(Socket, -1),
            lager:info("init ok port:~p", [Port]),
            {ok, #state{listener = Socket, acceptor = Ref}};
        {error, Reason} ->
            lager:error("init failed port ~p: ~p", [Port, Reason]),
            {stop, Reason}
    end.

handle_call(_Request, _From, State) ->
    {reply, not_support, State}.

handle_cast(_Msg, State) ->
    {noreply, State, hibernate}.

handle_info({inet_async, ListSock, Ref, {ok, CliSocket}},
            #state{listener = ListSock, acceptor = Ref} = State) ->
    try
        PeerEp = case inet:peername(CliSocket) of
            {ok, Ep} -> Ep;
            PeerErr -> PeerErr
        end,
        case set_sockopt(ListSock, CliSocket) of
            ok -> ok;
            {error, Reason} ->
                exit({set_sockopt, Reason})
        end,
        {ok, Pid} = cache_client_sup:start_child(PeerEp, CliSocket),
        ok = gen_tcp:controlling_process(CliSocket, Pid),
        inet:setopts(CliSocket, [{active, once}]),

        lager:debug("Client ~p connected ok", [PeerEp]),
        NewRef = case prim_inet:async_accept(ListSock, -1) of
            {ok, NR} -> NR;
            {error, Err} ->
                lager:error("Couldn't accept: ~p", [inet:format_error(Err)]),
                exit({async_accept, inet:format_error(Err)})
        end,
        {noreply, State#state{acceptor = NewRef}}
    catch
        exit:Error ->
            lager:error("Error in async accept: ~p", [Error]),
            {stop, Error, State}
    end;
handle_info({inet_async, ListSock, Ref, Error},
            #state{listener = ListSock, acceptor = Ref} = State) ->
    lager:error("Error in socket acceptor: ~p", [Error]),
    {stop, Error, State};
handle_info(_Info, State) ->
    {noreply, State, hibernate}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

set_sockopt(ListSock, CliSocket) ->
    true = inet_db:register_socket(CliSocket, inet_tcp),
    case prim_inet:getopts(ListSock, [active, nodelay, keepalive, delay_send, priority, tos]) of
        {ok, Opts} ->
            case prim_inet:setopts(CliSocket, Opts) of
                ok -> ok;
                Error ->
                    gen_tcp:close(CliSocket),
                    Error
            end;
        Error ->
            gen_tcp:close(CliSocket),
            Error
    end.
