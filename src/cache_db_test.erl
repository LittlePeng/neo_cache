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

run(Concurrent, Total) ->
    neo_counter:reset(),
    Avg = Total div Concurrent,
    lists:foreach(fun(Index) ->
                BeginUid = 1000000000 + (Index-1) * Avg + 1,
                EndUid = 1000000000 + Index * Avg,
                spawn(fun() ->
                            {ok, Pid} = eredis:start_link("10.77.128.93", 8000),
                            eredis:q(Pid, ["flushall"]),
                            loop_run(Pid, BeginUid, EndUid)
                    end)
        end, lists:seq(1, Concurrent)).

loop_run(_Client, EndUid, EndUid) ->
    ok;
loop_run(Client, BeginUid, EndUid) ->
    case BeginUid rem 10 == 0 of
        true -> neo_counter:inc(neo_cache, req, 10);
        _ -> ok
    end,
    Version = 3798569767177308 + BeginUid,
    Data = <<"1111111111222222222233333333334444444444", (neo_util:to_binary(BeginUid))/binary>>,
    QP = [[<<"APPEND">>, BeginUid, BeginUid + 1000000, Data, Version+Field, Version+Field-1] ||
            Field <- lists:seq(1, 1)],
    eredis:qp(Client, QP),
    loop_run(Client, BeginUid+1, EndUid).
