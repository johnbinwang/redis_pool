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
-module(redis).
-behaviour(gen_server).

%% gen_server callbacks
-export([start_link/2, init/1, handle_call/3, handle_cast/2, 
         handle_info/2, terminate/2, code_change/3]).

-export([q/2, q/3, stop/1]).

-define(NL, <<"\r\n">>).

-record(state, {ip = "127.0.0.1", port = 6379, db = 0, pass, socket, key, callback, buffer}).

-define(TIMEOUT, 5000).

%% API functions
start_link(Pool, Opts) ->
    gen_server:start_link(?MODULE, [Pool, Opts], []).

q(Pool, Parts) ->
    q(Pool, Parts, ?TIMEOUT).

q(Pool, Parts, Timeout) ->
    case redis_pool:pid(Pool) of
        Pid when is_pid(Pid) ->
            case catch gen_server:call(Pid, {q, Parts, Timeout}, Timeout) of
                {'EXIT', Error} ->
                    io:format("caught exception ~p~n", [Error]),
                    Error;
                {error, Error} ->
                    io:format("caught error ~p~n", [Error]),
                    redis_pool:unlock(Pool, Pid),
                    {error, Error};
                Result ->
                    redis_pool:unlock(Pool, Pid),
                    Result
            end;
        Error ->
            Error
    end.

stop(Pid) ->
    gen_server:call(Pid, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%    {ok, State, Timeout} |
%%    ignore                             |
%%    {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Pool, Opts]) ->
    io:format("init redis worker: ~p~n", [self()]),
    State = parse_options(Opts, #state{}),
    case connect(State#state.ip, State#state.port, State#state.pass) of
        {ok, Socket} ->
            Pool =/= undefined andalso redis_pool:register(Pool, self()),
            {ok, State#state{socket=Socket}};
        Error ->
            {stop, Error}
    end.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%    {reply, Reply, State, Timeout} |
%%    {noreply, State} |
%%    {noreply, State, Timeout} |
%%    {stop, Reason, Reply, State} |
%%    {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({q, Parts, Timeout}, _From, State) ->
    case do_q(Parts, Timeout, State#state.socket) of
        {redis_error, Error} ->
            {reply, {error, Error}, State};
        {error, timeout} ->
            {reply, {error, timeout}, State};
        {error, Reason} ->
            {stop, Reason, {error, Reason}, State};
        Result ->
            {reply, Result, State}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Msg, _From, State) ->
    {reply, unknown_message, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%    {noreply, State, Timeout} |
%%    {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%    {noreply, State, Timeout} |
%%    {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
    disconnect(State#state.socket),
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
disconnect(Socket) ->
    catch gen_tcp:close(Socket).

do_q(Parts, Timeout, Socket) when is_list(Parts) ->
    send_recv(Socket, Timeout, redis_proto:build(Parts));

do_q(Packet, Timeout, Socket) when is_binary(Packet) ->
    send_recv(Socket, Timeout, Packet).
    
parse_options([], State) ->
    State;
parse_options([{ip, Ip} | Rest], State) ->
    parse_options(Rest, State#state{ip = Ip});
parse_options([{port, Port} | Rest], State) ->
    parse_options(Rest, State#state{port = Port});
parse_options([{db, Db} | Rest], State) ->
    parse_options(Rest, State#state{db = Db});
parse_options([{pass, Pass} | Rest], State) ->
    parse_options(Rest, State#state{pass = Pass}).

connect(Ip, Port, Pass) ->
    case gen_tcp:connect(Ip, Port, [binary, {active, false}, {keepalive, true}]) of
        {ok, Sock} when Pass == undefined ->
            {ok, Sock};
        {ok, Sock} ->
            case do_auth(Sock, Pass) of
                {ok, <<"OK">>} -> {ok, Sock};
                Err -> Err
            end;
        Err ->
            exit(Err)
    end.

do_auth(Socket, Pass) when is_binary(Pass), size(Pass) > 0 ->
    send_recv(Socket, ?TIMEOUT, [<<"AUTH ">>, Pass, ?NL]);

do_auth(_Socket, _Pass) ->
    {ok, "not authenticated"}.

send_recv(Socket, Timeout, Packet) when is_port(Socket) ->
    case gen_tcp:send(Socket, Packet) of
        ok ->
            io:format("read_resp ~p~n", [read_resp(Socket, Timeout)]),
            io:format("other ~p~n", [gen_tcp:recv(Socket, 0)]);
        Error ->
            disconnect(Socket),
            exit(Error)
    end.

read_resp(Socket, Timeout) ->
    inet:setopts(Socket, [{packet, line}]),
    Resp = gen_tcp:recv(Socket, 0, Timeout),
    io:format("read_resp ~p~n", [Resp]),
    case Resp of
        {ok, <<"*", Rest/binary>>} ->
            Count = list_to_integer(binary_to_list(strip_nl(Rest))),
            io:format("read multi bulk ~p~n", [Count]),
            read_multi_bulk(Socket, Timeout, Count, []);
        {ok, <<"+", Rest/binary>>} ->
            io:format("read single line~n"),
            {ok, strip_nl(Rest)};
        {ok, <<"-", Rest/binary>>} ->
            {redis_error, strip_nl(Rest)};
        {ok, <<":", Size/binary>>} ->
            io:format("read integer~n"),
            {ok, list_to_integer(binary_to_list(strip_nl(Size)))};
        {ok, <<"$", Size/binary>>} ->
            Size1 = list_to_integer(binary_to_list(strip_nl(Size))),
            io:format("read bulk reply ~p~n", [Size1]),
            read_body(Socket, Size1);
        {ok, <<"\r\n">>} ->
            io:format("read nl~n"),
            read_resp(Socket, Timeout);
        {error, Err} ->
            disconnect(Socket),
            exit({error, Err})
    end.

strip_nl(B) when is_binary(B) ->
    S = size(B) - size(?NL),
    <<B1:S/binary, _/binary>> = B,
    B1.
    
read_body(_Socket, -1) ->
    {ok, undefined};
read_body(_Socket, 0) ->
    {ok, <<>>};
read_body(Socket, Size) ->
    inet:setopts(Socket, [{packet, raw}]),
    case gen_tcp:recv(Socket, Size) of
        {error, Error} ->
            disconnect(Socket),
            exit({error, Error});
        Recv ->
            Recv
    end.

read_multi_bulk(_Data, _Timeout, 0, Acc) ->
    lists:reverse(Acc);
read_multi_bulk(Socket, Timeout, Count, Acc) ->
    Acc1 = [read_resp(Socket, Timeout) | Acc],
    read_multi_bulk(Socket, Timeout, Count-1, Acc1).

