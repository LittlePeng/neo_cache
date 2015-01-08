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
               align_dict=dict:new(), max_align_version=0, unaligns=[]}). 
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

% 匹配的时候一定要同时匹配下界和上界, 下界需要遍历查找
% 中间部分 Fields 过长可能不连续
do_fetch(Key, VMax, VMin) ->
    case get(Key) of
        undefined -> none;
        #list{align_dict=Dict}=List -> 
            put(Key, lru_update(Key, List)),
            Dict
    end.

do_append(Key, Field, Msg) ->
    List0 = case get(Key) of
        undefined ->
            NewList=lru_new_list(Key),
            NewList#list{max_align_version=Msg#msg.pre_version};
        V -> V
    end,
    case try_append(List0, Field, Msg) of
         {ok, List} -> put(Key, List);
         _ -> erase(Key)
    end.

try_append(#list{max_align_version=MaxVersion}=List0, Field, #msg{pre_version=MaxVersion}=Msg) ->
    append_msg(Field, Msg, List0);
try_append(#list{unaligns=UAS}=List0, Field, Msg) ->
    neo_counter:inc(db, append_align_failed),
    try_align(List0#list{unaligns=[{Field, Msg} | UAS]}).

append_msg(Field, Msg, #list{len=Len, align_dict=Dict0}=List0) ->
        NewMsgs = case dict:find(Field, Dict0) of
        error ->
            case dict:size(Dict0) >= ?MAX_FIELD_COUNT of
                true -> error;
                false -> [Msg]
            end;
        {ok, Msgs} ->
            case length(Msgs) >= ?MAX_FIELD_LEN of
                true ->
                    {Lefts, _} = lists:split(?MAX_FIELD_LEN-1, Msgs),
                    [Msg | Lefts];
                false ->
                    [Msg | Msgs]
            end
    end,
    case NewMsgs =/= error of
        true ->
            Dict = dict:store(Field, NewMsgs, Dict0),
            {ok, List0#list{len=Len+1, max_align_version=Msg#msg.version, align_dict=Dict}};
        false -> error
    end.

% 尝试对未对其列表
% 1. 先尝试对其可对齐部分
% 2. 不可对其部分time 超过一定时间，认为丢失，整体删除
%
% 触发场景
% 1. 每次新增一个unalign时
% 2. fetch操作读取之前
try_align(#list{unaligns=UAS} = List) when length(UAS) == 0 ->
    {ok, List};
try_align(#list{unaligns=UAS, max_align_version=MaxVersion}=List) ->
    case [Msg0 || {_, #msg{pre_version=Version}}=Msg0 <- UAS, Version == MaxVersion] of
        [{Field, Value}=Msg] ->
            case append_msg(Field, Value, List#list{unaligns=UAS--[Msg]}) of
                {ok, NewList} -> try_align(NewList);
                Err -> Err
            end;
        _ -> 
            case lists:any(fun({_, #msg{time=Time}}) ->
                           cache_time:now() - Time > ?MAX_ALIGN_TIMOUT
                    end, UAS) of
                true -> {ok, List};
                false -> error
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
    put(lru, Index),
    Index.

lru_new_list(Key) ->
    Index = lru_next_index(),
    put(lru, gb_trees:insert(Index, Key, get(lru))),
    #list{index=Index}.

lru_update(Key, #list{index=Index}=List) ->
    Lru = gb_trees:delete(Index, get(lru)),
    NewIndex = lru_next_index(),
    put(lru, gb_tree:insert(NewIndex, Key, Lru)),
    List#list{atime=cahce_time:now(), index=NewIndex}.

lru_clean() ->
    todo.
