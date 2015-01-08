-module(cache_util).
-compile(export_all).

% 数据存储时，优先尝试使用int 节省内存
try_to_int(Bin) when is_binary(Bin) ->
    case string:to_integer(binary_to_list(Bin)) of
        {Int, []} -> Int;
        _ -> Bin
    end;
try_to_int(Str) ->
    Str.

% unix timestamp
now() ->
    {T1, T2, _} = os:timestamp(),
    T1*1000000 + T2.

upper(Bin) ->
      upper(Bin, <<>>).

upper(<<>>, Acc) ->
    Acc;
upper(<<C, Rest/binary>>, Acc) when $a =< C, C =< $z ->
    upper(Rest, <<Acc/binary, (C-32)>>);
upper(<<195, C, Rest/binary>>, Acc) when 160 =< C, C =< 182 -> %% A-0 with tildes plus enye
    upper(Rest, <<Acc/binary, 195, (C-32)>>);
upper(<<195, C, Rest/binary>>, Acc) when 184 =< C, C =< 190 -> %% U and Y with tilde plus greeks
    upper(Rest, <<Acc/binary, 195, (C-32)>>);
upper(<<C, Rest/binary>>, Acc) ->
    upper(Rest, <<Acc/binary, C>>).

lower(Bin) ->
    lower(Bin, <<>>).

lower(<<>>, Acc) ->
    Acc;
lower(<<C, Rest/binary>>, Acc) when $A =< C, C =< $Z ->
    lower(Rest, <<Acc/binary, (C+32)>>);
lower(<<195, C, Rest/binary>>, Acc) when 128 =< C, C =< 150 -> %% A-0 with tildes plus enye
    lower(Rest, <<Acc/binary, 195, (C+32)>>);
lower(<<195, C, Rest/binary>>, Acc) when 152 =< C, C =< 158 -> %% U and Y with tilde plus greeks
    lower(Rest, <<Acc/binary, 195, (C+32)>>);
lower(<<C, Rest/binary>>, Acc) ->
    lower(Rest, <<Acc/binary, C>>).
