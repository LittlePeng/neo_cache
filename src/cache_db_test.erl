-module(cache_db_test).
-compile(export_all).

append(N) ->
    lists:foreach(fun(Uid) ->
                Key = neo_util:to_binary(Uid),
                Field = neo_util:to_binary(Uid),
                Value0 = <<"111111111111111122222222222222222333333333">>,
                lists:foreach(fun(Item) ->
                            Value = <<Value0/binary, (neo_util:to_binary(Item))/binary>>,
                            cache_db:append(Key, Field, Value, Item, Item -1)
                    end, lists:seq(Uid, Uid+20))
        end, lists:seq(100000000, 100000000 + N)).


