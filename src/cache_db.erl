-module(cache_db).
-include("cache.hrl").
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([terminate/2, code_change/3]).
-export([database/1, append/5, del/1, fetch/3, info/0, info/1]).

-define(DBID(Key), database(erlang:phash2(Key) rem ?DB_COUNT)).
-define(MAX_ALIGN_TIMOUT, 10*60). % 对齐超时时间
-define(MAX_FIELD_LEN, 20). % 一个field 最大长度
-define(MAX_FIELD_COUNT, 50). % 最大field数量

-record(state, {len=0, append=0, del=0, fetch=0}).
-record(list, {len=0, atime=cache_time:now(), index=0,
               align_dict=dict:new(), align_max=0, align_min=0, unaligns=[]}). 
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

database(Index) ->
    list_to_atom(atom_to_list(?MODULE) ++ "_" ++ integer_to_list(Index)).

start_link(Index) ->
    gen_server:start_link({local, database(Index)}, ?MODULE, [], []).

append(Key0, Field0, Value, Version, PreVersion) ->
    Key = cache_util:try_to_int(Key0),
    Field = cache_util:try_to_int(Field0),
    Msg = #msg{value=Value, version=Version, pre_version=PreVersion, time=cache_time:now()},
    gen_server:call(?DBID(Key), {append, Key, Field, Msg}).

del(Key0) ->
    Key = cache_util:try_to_int(Key0),
    gen_server:call(?DBID(Key), {del, Key}).

fetch(Key0, Vmin, Vmax) ->
    Key = cache_util:try_to_int(Key0),
    gen_server:call(?DBID(Key), {fetch, Key, Vmin, Vmax}).

info() ->
    [info(Id) || Id <- lists:seq(0, ?DB_COUNT -1)].

info(DB_Id) ->
    gen_server:call(database(DB_Id), info).

init([]) ->
    lru_init(),
    {ok, #state{}}.

handle_call({append, Key, Field, Msg}, _From, State) ->
    R = do_append(Key, Field, Msg),
    {reply, R, State#state{append=State#state.append+1}};
handle_call({del, Key}, _From, State) ->
    {reply, do_del(Key), State#state{del=State#state.del+1}};
handle_call({fetch, Key, VMax, VMin}, _From, State) ->
    {reply, do_fetch(Key, VMax, VMin), State#state{fetch=State#state.fetch+1}};
handle_call(info, _From, State) ->
    {reply, State, State};
handle_call(_Req, _From, State) ->
    {reply, not_support, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Req, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_del(Key) ->
    case erase(Key) of
        undefined -> 0;
        _ -> 1
    end.

% 匹配的时候一定要同时匹配下界和上界
% 下界分为两种:
% 1. 完整的下界，全部对其
% 2. 因单个会话量大被踢，不保证对其
% 中间部分 Fields 过长可能不连续
do_fetch(Key, VMin, VMax) ->
    case get(Key) of
        undefined -> [];
        #list{align_dict=Dict}=List -> 
            % 顺延ttl & 清理过期msgs
            List1 = try_trim(List),
            List2 = lru_update(Key, List1),
            put(Key, List2),
            lists:map(fun({Field, ValueList}) ->
                        {Field, 1, [{Value, Version, PreVersion} ||
                                #msg{value=Value, version=Version,
                                     pre_version=PreVersion} <- ValueList]}
                end, dict:to_list(Dict))
    end.

do_append(Key, Field, Msg) ->
    List0 = case get(Key) of
        undefined ->
            NewList=lru_new_list(Key),
            NewList#list{align_max=Msg#msg.pre_version,
                         align_min=Msg#msg.pre_version};
        V -> V
    end,
    case try_append(List0, Field, Msg) of
         {ok, List} ->
            put(Key, List),
            List#list.len;
         _Err ->
            lru_delete(Key, List0),
            erase(Key),
            0
    end.

try_append(#list{align_max=MaxVersion}=List0, Field, #msg{pre_version=MaxVersion}=Msg) ->
    append_msg(Field, Msg, List0);
try_append(#list{unaligns=UAS}=List0, Field, Msg) ->
    neo_counter:inc(db, append_align_failed),
    List = List0#list{unaligns=[{Field, Msg} | UAS]},
    case length(UAS) > 1 of
        true -> try_align(List);
        false -> List
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

try_trim(List) ->
    List.

%%%%% LRU %%%%%%%%
lru_init() ->
    put(index, 0),
    put(lru, gb_trees:empty()).

lru_next_index() ->
    Index = get(index) + 1,
    put(index, Index),
    Index.

lru_new_list(Key) ->
    Index = lru_next_index(),
    put(lru, gb_trees:insert(Index, Key, get(lru))),
    #list{index=Index}.

lru_update(Key, #list{index=Index}=List) ->
    Lru = gb_trees:delete(Index, get(lru)),
    NewIndex = lru_next_index(),
    put(lru, gb_trees:insert(NewIndex, Key, Lru)),
    List#list{atime=cache_time:now(), index=NewIndex}.

lru_delete(Key, #list{index=Index}) ->
    put(lru, gb_trees:delete(Index, get(lru))).

lru_clean() ->
    todo.
