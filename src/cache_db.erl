-module(cache_db).
-include("cache.hrl").
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([terminate/2, code_change/3]).
-export([database/1, append/5, del/1, fetch/3, info/0, llen/1, ttl/1, dump/1, info/1]).
-export([random_key/0, flush_all/0]).

-define(DBID(Key), database(erlang:phash2(Key) rem ?DB_COUNT)).

-define(ONE_MB, 1024 * 1024).
-define(EXPIRE_CHECK_INTERVAL, 5000).
-define(MAX_ALIGN_TIMOUT, 10*60). % 对齐超时时间
-define(MAX_FIELD_LEN, 20). % 一个field 最大长度
-define(MAX_FIELD_COUNT, 50). % 最大field数量
-define(MAX_EXPIRE_COUNT, 100).
-define(KEY_EXPIRE_TIME, 7 * 24 * 3600).

-define(KEY_LRU, '__lru__').
-define(KEY_INDEX, '__index__').

-record(list, {len=0, atime=cache_time:now(), index=0,
               align_dict=dict:new(), align_max=0, align_min=0, unaligns=[]}). 
-record(state, {id=0, append=0, del=0, fetch=0}).
-record(msg, {value, version, pre_version, time}).

%% 
%%  内存控制方式:
%% 1. 定时器
%%    内存限额、Key 过期时，通过LRU 整个Key删除
%% 2. fetch
%%    会遍历消息列表，顺便删除过期消息、顺延过期时间
%% 3. append
%%    只做最大会话数，最大消息数判断
%%

database(DBId) ->
    list_to_atom(atom_to_list(?MODULE) ++ "_" ++ integer_to_list(DBId)).

start_link(DBId) ->
    gen_server:start_link({local, database(DBId)}, ?MODULE, [DBId], []).

append(Key0, Field0, Value, Version, PreVersion) ->
    Key = cache_util:try_to_int(Key0),
    Field = cache_util:try_to_int(Field0),
    Msg = #msg{value=Value, version=Version, pre_version=PreVersion, time=cache_time:now()},
    gen_server:call(?DBID(Key), {append, Key, Field, Msg}).

fetch(Key0, Vmin, Vmax) ->
    Key = cache_util:try_to_int(Key0),
    gen_server:call(?DBID(Key), {fetch, Key, Vmin, Vmax}).

del(Key0) ->
    Key = cache_util:try_to_int(Key0),
    gen_server:call(?DBID(Key), {del, Key}).

llen(Key0) ->
    Key = cache_util:try_to_int(Key0),
    gen_server:call(?DBID(Key), {llen, Key}).

ttl(Key0) ->
    Key = cache_util:try_to_int(Key0),
    gen_server:call(?DBID(Key), {ttl, Key}).

dump(Key0) ->
    Key = cache_util:try_to_int(Key0),
    gen_server:call(?DBID(Key), {dump, Key}).

random_key() ->
    gen_server:call(?DBID(os:timestamp()), random_key).

flush(DBId) ->
    gen_server:call(database(DBId), flush).

flush_all() ->
    [flush(Id) || Id <- lists:seq(0, ?DB_COUNT -1)].

info() ->
    [info(Id) || Id <- lists:seq(0, ?DB_COUNT -1)].

info(DBId) ->
    gen_server:call(database(DBId), info).

init([DBId]) ->
    dict_init(),
    InitTimer = (?EXPIRE_CHECK_INTERVAL div ?DB_COUNT) * DBId,
    erlang:send_after(InitTimer, self(), check_expire),
    {ok, #state{id=DBId}}.

handle_call({append, Key, Field, Msg}, _From, State0) ->
    {R, State} = do_append(Key, Field, Msg, State0),
    {reply, R, State#state{append=State#state.append+1}};
handle_call({del, Key}, _From, State) ->
    {R, State} = do_del(Key, State),
    {reply, R, State#state{del=State#state.del+1}};
handle_call({llen, Key}, _From, State) ->
    R = case get(Key) of
        undefined -> 0;
        List -> list_len(List) 
    end,
    {reply, R, State};
handle_call({ttl, Key}, _From, State) ->
    Ttl = case get(Key) of
        undefined -> -1;
        #list{atime=ATime} -> ?KEY_EXPIRE_TIME - (cache_time:now() - ATime)
    end,
    {reply, Ttl, State};
handle_call({fetch, Key, VMax, VMin}, _From, State0) ->
    {R, State} = do_fetch(Key, VMax, VMin, State0),
    {reply, R, State#state{fetch=State#state.fetch+1}};
handle_call({dump, Key}, _From, State) ->
    {reply, get(Key), State};
handle_call(random_key, _From, State) ->
    {reply, lru_min(), State};
handle_call(flush, _From, State) ->
    erase(),
    dict_init(),
    {reply, ok, State, hibernate};
handle_call(info, _From, #state{id=Id, fetch=Fetch, append=Append, del=Del}=State) ->
    KeyCount = lru_key_space(),
    R = [{id, Id}, {keys, KeyCount},
         {fetch, Fetch}, {append, Append}, {del, Del}],
    {reply, R, State};
handle_call(_Req, _From, State) ->
    {reply, not_support, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(check_expire, State) ->
    % 1. 清理过期数据
    % 2. 内存压力阈值时，LRU清理替换
    % 3. *不能hibernate 强制GC (内存较大时, Major GC消耗大量CPU)
    erlang:send_after(?EXPIRE_CHECK_INTERVAL, self(), check_expire),
    {noreply, do_lru_expire(State)};
handle_info(_Req, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_del(Key, State) ->
    case get(Key) of
        undefined ->
            {0, State};
        List ->
            delete_key(Key, List),
            {1, State}
    end.

% 匹配的时候一定要同时匹配下界和上界
% 下界分为两种:
% 1. 完整的下界，全部对其
% 2. 因单个会话量大被踢，不保证对其
% 中间部分 Fields 过长可能不连续
do_fetch(Key, VMin, VMax, State) ->
    case get(Key) of
        undefined -> {[], State};
        List -> 
            case try_align(List) of
                {ok, List1} ->
                    R = get_inc(List1, VMin, VMax),
                    % 顺延ttl & 清理过期msgs
                    List2 = try_trim(List1),
                    List3 = lru_update(Key, List2),
                    put(Key, List3),
                    {R, State};
                _ ->
                    delete_key(Key, List),
                    {[], State}
            end
    end.

get_inc(#list{align_dict=Dict, align_max=AlignMax, align_min=AlignMin},
        VMin, VMax) when AlignMax >= VMax orelse VMax =:= infinite ->
    dict:fold(fun(Field, ValueList, DAcc) ->
                R = lists:foldl(fun(#msg{value=Value, version=Version, pre_version=PreVersion}, Acc) ->
                                case Version >= VMin andalso Version =< VMax of
                                    true -> [[Version, PreVersion, Value] | Acc];
                                    false -> Acc
                                end
                        end, [], ValueList),
                case length(R) > 0 of
                    true ->
                        %TODO: IsFull 此处不精确
                        IsFull = case AlignMin >= VMin of
                            true -> 1;
                            false -> 0
                        end,
                        [[Field, IsFull, R] | DAcc];
                    false ->
                        DAcc
                end
        end, [], Dict);
get_inc(_List, _VMin, _VMax) ->
    [].

do_append(Key, Field, #msg{pre_version=PreVersion}=Msg, State) ->
    List0 = case get(Key) of
        undefined ->
            NewList=lru_new_list(Key),
            NewList#list{align_max=PreVersion, align_min=PreVersion};
        V -> V
    end,
    case try_append(List0, Field, Msg) of
         {ok, List} ->
            put(Key, List),
            {list_len(List), State};
         _Err ->
            delete_key(Key, List0),
            {0, State} 
    end.

delete_key(Key, List) ->
    lager:debug("delete key:~p", [Key]),
    lru_delete(Key, List),
    erase(Key).

try_append(#list{align_max=MaxVersion}=List0, Field, #msg{pre_version=MaxVersion}=Msg) ->
    append_msg(Field, Msg, List0);
try_append(#list{unaligns=UAS}=List0, Field, Msg) ->
    neo_counter:inc(cache_db, append_align_failed),
    List = List0#list{unaligns=[{Field, Msg} | UAS]},
    case length(UAS) > 1 of
        true -> try_align(List);
        false -> {ok, List}
    end.

% 对齐后添加
% 1. 直接添加
% 2. 不在现有Field, 且Fields 过多 -> error
% 3. 所在Filed Value过多 -> 踢出一个替换，修改align_min
append_msg(Field, Msg, #list{align_dict=Dict0}=List0) ->
    R = case dict:find(Field, Dict0) of
        error ->
            case dict:size(Dict0) >= ?MAX_FIELD_COUNT of
                true -> error;
                false -> {[Msg], none}
            end;
        {ok, Msgs} ->
            {Lefts, [RmMsg0]} =
            case length(Msgs) == ?MAX_FIELD_LEN of
                true -> lists:split(?MAX_FIELD_LEN-1, Msgs);
                false -> {Msgs, [none]}
            end,
            {[Msg | Lefts], RmMsg0}
    end,
    case R of
        {NewMsgs, RmMsg} ->
            {Len, NewAlignMin} = case RmMsg of
                none ->
                    {List0#list.len+1, List0#list.align_min};
                #msg{version=RmVersion} ->
                    neo_counter:inc(cache_db, max_filed_len),
                    {List0#list.len, max(List0#list.align_min, RmVersion)}
            end,
            Dict = dict:store(Field, NewMsgs, Dict0),
            {ok, List0#list{len=Len, align_max=Msg#msg.version,
                           align_min=NewAlignMin, align_dict=Dict}};
        Error ->
            neo_counter:inc(cache_db, max_filed_count),
            Error
    end.

% 尝试对未对其列表
% 1. 先尝试对其可对齐部分
% 2. 不可对其部分time 超过一定时间，认为丢失不可恢复 -> error
%
% 触发场景
% 1. 每次新增一个unalign时
% 2. fetch操作读取之前
try_align(#list{unaligns=UAS} = List) when length(UAS) == 0 ->
    {ok, List};
try_align(#list{unaligns=UAS, align_max=MaxVersion}=List) ->
    case [Msg0 || {_, #msg{pre_version=Version}}=Msg0 <- UAS, Version == MaxVersion] of
        [{Field, Value}=Msg] ->
            case append_msg(Field, Value, List#list{unaligns=UAS--[Msg]}) of
                {ok, NewList} ->
                    neo_counter:inc(cache_db, try_aligin_ok),
                    try_align(NewList);
                Err -> Err
            end;
        _ -> 
            case lists:any(fun({_, #msg{time=Time}}) ->
                           cache_time:now() - Time > ?MAX_ALIGN_TIMOUT
                    end, UAS) of
                true -> {ok, List};
                false ->
                    neo_counter:inc(cache_db, try_aligin_timeout),
                    error
            end
    end.

try_trim(#list{len=Len, align_dict=Dict}=List) ->
    Now = cache_util:now(),
    {Dict2, Dels2} = dict:fold(fun(Field, ValueList, {Dict0, Del0}) ->
                    ValueList2 = lists:filter(fun(#msg{time=CTime}) ->
                                    Now - CTime < ?KEY_EXPIRE_TIME
                            end, ValueList),
                    Dict1=
                    case length(ValueList2) > 0 of
                        true -> dict:store(Field, ValueList2, Dict0);
                        false -> dict:erase(Field, Dict0)
                    end,
                    Dels1 = (length(ValueList) - length(ValueList2)),
                    {Dict1, Del0 + Dels1}
            end, {Dict, 0}, Dict),
    List#list{len=Len-Dels2, align_dict=Dict2}.

do_lru_expire(State) ->
    Lru = get(?KEY_LRU),
    MaxMem = application:get_env(neo_cache, max_memory, 100) * ?ONE_MB,
    Force = erlang:memory(total) > MaxMem,
    {Lru2, Dels} = collect_expire(Lru, Force),
    case Dels > 0 of
        true ->
            case Force of
                true -> neo_counter:inc(cache_db, evicted, Dels);
                false -> neo_counter:inc(cache_db, expired, Dels)
            end,
            lager:debug("expire delete:~pitem", [Dels]);
        false -> ok
    end,
    put(?KEY_LRU, Lru2),
    State.

collect_expire(Lru, Force) ->
    collect_expire(Lru, Force, 0).

collect_expire(Lru, _Force, ?MAX_EXPIRE_COUNT) ->
    {Lru, ?MAX_EXPIRE_COUNT};
collect_expire(Lru, Force, AccCount) ->
    case gb_trees:is_empty(Lru) of
        true -> {Lru, AccCount};
        false ->
            {_Index, Key, LruRst} = gb_trees:take_smallest(Lru),
            List = #list{atime=Atime} = get(Key),
            CanRm = case Force of
                true -> true;
                false -> Atime - cache_time:now() > ?KEY_EXPIRE_TIME
            end,
            case CanRm of
                true ->
                    delete_key(Key, List),
                    collect_expire(LruRst, Force, AccCount+1);
                false ->
                    {Lru, AccCount}
            end
    end.

list_len(#list{len=Len, unaligns=UAS}) -> 
    Len + length(UAS).

dict_init() ->
    put(?KEY_INDEX, 0),
    put(?KEY_LRU, gb_trees:empty()).

%%%%% LRU %%%%%%%%
lru_next_index() ->
    Index = get(?KEY_INDEX) + 1,
    put(?KEY_INDEX, Index),
    Index.

lru_new_list(Key) ->
    Index = lru_next_index(),
    put(?KEY_LRU, gb_trees:insert(Index, Key, get(?KEY_LRU))),
    #list{index=Index}.

lru_update(Key, #list{index=Index}=List) ->
    Lru = gb_trees:delete(Index, get(?KEY_LRU)),
    NewIndex = lru_next_index(),
    put(?KEY_LRU, gb_trees:insert(NewIndex, Key, Lru)),
    List#list{atime=cache_time:now(), index=NewIndex}.

lru_delete(_Key, #list{index=Index}) ->
    put(?KEY_LRU, gb_trees:delete(Index, get(?KEY_LRU))).

lru_min() ->
    Lru = get(?KEY_LRU),
    case gb_trees:is_empty(Lru) of
        true -> undefined;
        false ->
            {_Index, Key, _LruRst} = gb_trees:take_smallest(Lru),
            neo_util:to_binary(Key)
    end.

lru_key_space() ->
    gb_trees:size(get(?KEY_LRU)).
