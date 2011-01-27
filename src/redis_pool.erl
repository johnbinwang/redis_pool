%% Copyright (c) 2010 Jacob Vorreuter <jacob.vorreuter@gmail.com>
%% 
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%% 
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
-module(redis_pool).
-behaviour(gen_server).

%% gen_server callbacks
-export([start_link/2, start_link/3, init/1, handle_call/3,
	 handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([add/2, add/3, remove/1, register/2, pid/1, unlock/2, info/1, info/2]).

-record(state, {opts, available, locked}).

-define(TIMEOUT, 8000).

%% API functions
start_link(Opts, NumWorkers) ->
    gen_server:start_link(?MODULE, [Opts, NumWorkers], []).

start_link(Name, Opts, NumWorkers) ->
    gen_server:start_link({local, Name}, ?MODULE, [Opts, NumWorkers], []).

add(Opts, NumWorkers) when is_list(Opts), is_integer(NumWorkers) ->
    redis_pool_sup:start_child(Opts, NumWorkers).

add(Name, Opts, NumWorkers) when is_atom(Name), is_list(Opts), is_integer(NumWorkers) ->
    redis_pool_sup:start_child(Name, Opts, NumWorkers).

remove(NameOrPid) ->
    gen_server:call(NameOrPid, stop).

register(NameOrPid, WorkerPid) ->
    gen_server:cast(NameOrPid, {register, WorkerPid}).

pid(NameOrPid) ->
    gen_server:call(NameOrPid, pid, ?TIMEOUT).

unlock(NameOrPid, WorkerPid) ->
    gen_server:cast(NameOrPid, {unlock, WorkerPid}).

info(NameOrPid) ->
    gen_server:call(NameOrPid, info).

info(NameOrPid, opts) ->
    R = info(NameOrPid),
    R#state.opts.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%% @hidden
%%--------------------------------------------------------------------
init([Opts, NumWorkers]) ->
    Workers = start_workers(NumWorkers, Opts),
    Queue = queue:from_list(Workers),
    {ok, #state{opts=Opts, available=Queue, locked=gb_trees:empty()}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%% @hidden
%%--------------------------------------------------------------------
handle_call(pid, {From, _Mref}, #state{available=Queue, locked=Tree}=State) ->
    case queue:out(Queue) of
        {{value, Pid}, Queue1} ->
            Tree1 = gb_trees:enter(Pid, From, Tree),
            {reply, Pid, State#state{available=Queue1, locked=Tree1}};
        {empty, _} ->
            {reply, empty, State}
    end;

handle_call(info, _From, State) ->
    {reply, State, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Msg, _From, State) ->
    {reply, {error, invalid_call}, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%% @hidden
%%--------------------------------------------------------------------
handle_cast({unlock, WorkerPid}, #state{available=Queue, locked=Tree}=State) ->
    io:format("unlock ~p~n", [WorkerPid]),
    Tree1 = gb_trees:delete_any(WorkerPid, Tree),
    {noreply, State#state{available=queue:in(WorkerPid, Queue), locked=Tree1}};

handle_cast({register, WorkerPid}, #state{available=Queue}=State) ->
    erlang:monitor(process, WorkerPid),
    {noreply, State#state{available=queue:in(WorkerPid, Queue)}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%% @hidden
%%--------------------------------------------------------------------
handle_info({'DOWN', _MonitorRef, process, Pid, _Info}, #state{available=Queue, locked=Tree}=State) ->
    io:format("worker down: ~p~n", [Pid]),
    case gb_trees:is_defined(Pid, Tree) of
        true ->
            io:format("remove from locked tree~n"),
            Tree1 = gb_trees:delete(Pid, Tree),
            {noreply, State#state{locked=Tree1}};
        false ->
            io:format("remove from available queue~n"),
            Queue1 = queue:filter(fun(Item) -> Item =/= Pid end, Queue),
            {noreply, State#state{available=Queue1}}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @hidden
%%--------------------------------------------------------------------
terminate(_Reason, #state{available=Queue, locked=Tree}) ->
    [gen_server:cast(Pid, die) || Pid <- gb_trees:keys(Tree)],
    [gen_server:cast(Pid, die) || Pid <- queue:to_list(Queue)],
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%% @hidden
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
start_workers(Num, Opts) ->
    start_workers(Num, Opts, []).

start_workers(0, _Opts, Acc) ->
    Acc;

start_workers(Num, Opts, Acc) ->
    {ok, Pid} = redis_pid_sup:start_child(self(), Opts),
    erlang:monitor(process, Pid),
    start_workers(Num-1, Opts, [Pid|Acc]).
