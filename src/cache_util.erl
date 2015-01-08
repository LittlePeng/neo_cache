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

