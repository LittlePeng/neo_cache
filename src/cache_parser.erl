-module(cache_parser).
-export([init/0, parser/1]).

init() ->
    put(pstate, <<>>).

parser(Bin0) ->
    Bin =
    case get(pstate) of
        <<>> -> Bin0;
        Rst  ->
            <<Rst/binary, Bin0/binary>>
    end,
    case do_parser(Bin) of
        {ok, Pks, Rst1} ->
            put(pstate, Rst1),
            {ok, Pks};
        Error ->
            {error, Error}
    end.

do_parser(Bin) ->
    do_parser(Bin, []).

do_parser(<<>>, Acc) ->
    {ok, lists:reverse(Acc), <<>>};
do_parser(<<"\r\n", Rst/binary>>, Acc) ->
    do_parser(Rst, Acc);
do_parser(<<"\n", Rst/binary>>, Acc) ->
    do_parser(Rst, Acc);
do_parser(<<$*, Rst/binary>> = Bin, Acc) ->
    case parser_length(Rst) of
        {ok, Len, Rst1} ->
            case parser_args(Rst1, Len) of
                {ok, Args, Rst2} ->
                    do_parser(Rst2, [Args | Acc]);
                _ ->
                    {ok, lists:reverse(Acc), Bin}
            end;
        _ ->
            {ok, lists:reverse(Acc), Bin}
    end;
do_parser(Bin, _) ->
    % support telnet
    {ok, [binary:split(Req, <<" ">>, [global]) ||
            Req <- binary:split(Bin, <<"\r\n">>, [global]), Req =/= <<>>], <<>>}.

parser_args(Bin, Argc) ->
    parser_args(Bin, Argc, []).

parser_args(Rst, 0, Acc) ->
    {ok, lists:reverse(Acc), Rst};
parser_args(<<$$, Rst/binary>>, Argc, Acc) ->
    case parser_length(Rst) of
        {ok, Len, Rst1} ->
            case Rst1 of
                <<Arg:Len/binary, "\r\n", Rst2/binary>> ->
                    parser_args(Rst2, Argc - 1, [Arg | Acc]);
                _ ->
                    error
            end;
        _ ->
            error
    end.

parser_length(Rst) ->
    case binary:split(Rst, <<"\r\n">>) of
        [LenBin, Rst1] ->
            {ok, binary_to_integer(LenBin), Rst1};
        _ ->
            error
    end.
