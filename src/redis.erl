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
-export([start_link/1, start_link/2, init/1, handle_call/3,
          handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-compile(export_all).

-define(NL, <<"\r\n">>).

-record(state, {ip = "127.0.0.1", port = 6379, db = 0, pass, socket}).

-define(TIMEOUT, 5000).

%% API functions
start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

start_link(Pool, Opts) ->
    case gen_server:start_link(?MODULE, [Opts], []) of
        {ok, Pid} ->
            redis_pool:register(Pool, Pid),
            {ok, Pid};
        Err ->
            Err
    end.

q(Pid, Parts) ->
    q(Pid, Parts, ?TIMEOUT).

q(Pid, Parts, Timeout) ->
    case catch gen_server:call(Pid, {q, Parts}, Timeout) of
        {'EXIT', {timeout, {gen_server, call, _}}} ->
            gen_server:cast(Pid, {stop, timeout}),
            {error, timeout};
        {'EXIT', {Error,   {gen_server, call, _}}} ->
            {error, Error};
        Reply ->
            Reply
    end.

stop(Pid) ->
    gen_server:cast(Pid, {stop, normal}).

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
init([Opts]) ->
    io:format("init redis worker: ~p~n", [self()]),
    State = parse_options(Opts, #state{}),
    case connect(State#state.ip, State#state.port, State#state.pass) of
        {ok, Socket} ->
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
handle_call({q, Parts}, _From, #state{socket=Socket, ip=Ip, port=Port, pass=Pass}=State) ->
    Packet = redis_proto:build(Parts),
    case send_recv(Socket, Ip, Port, Pass, Packet, 1) of
        {error, Error} ->
            {stop, Error, State};
        {Reply, NewSocket} ->
            {reply, Reply, State#state{socket=NewSocket}}
    end;

handle_call(_Msg, _From, State) ->
    {reply, unknown_message, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%    {noreply, State, Timeout} |
%%    {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({stop, Reason}, State) ->
    {stop, Reason, State};

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
    case gen_tcp:connect(Ip, Port, [binary, {active, false}, {keepalive, true}, {nodelay, true}]) of
        {ok, Sock} when Pass == undefined; Pass == <<>>; Pass == "" ->
            {ok, Sock};
        {ok, Sock} ->
            case redis_proto:send_auth(Sock, Pass) of
                true -> {ok, Sock};
                Err -> Err
            end;
        Err ->
            Err
    end.

send_recv(_Socket, _Ip, _Port, _Pass, _Packet, 0) ->
    {error, closed};

send_recv(Socket, Ip, Port, Pass, Packet, Retries) when is_port(Socket) ->
    case gen_tcp:send(Socket, Packet) of
        ok ->
            case read_resp(Socket) of
                {error, Err} ->
                    disconnect(Socket),
                    {error, Err};
                {redis_error, Err} ->
                    {{error, Err}, Socket};
                Reply ->
                    {Reply, Socket}
            end;
        {error, closed} ->
            disconnect(Socket),
            case connect(Ip, Port, Pass) of
                {ok, Socket1} ->
                    send_recv(Socket1, Ip, Port, Pass, Packet, Retries);
                _ ->
                    send_recv(Socket, Ip, Port, Pass, Packet, Retries-1)
            end;
        Error ->
            disconnect(Socket),
            Error
    end.

read_resp(Socket) ->
    inet:setopts(Socket, [{packet, line}]),
    case gen_tcp:recv(Socket, 0) of
        {ok, <<"+", Rest/binary>>} ->
            strip_nl(Rest);
        {ok, <<"-", Rest/binary>>} ->
            {redis_error, strip_nl(Rest)};
        {ok, <<":", Rest/binary>>} ->
            Int = strip_nl(Rest),
            list_to_integer(binary_to_list(Int));
        {ok, <<"$", Size/binary>>} ->
            Size1 = list_to_integer(binary_to_list(strip_nl(Size))),
            read_body(Socket, Size1);
        {ok, <<"*", Rest/binary>>} ->
            Count = list_to_integer(binary_to_list(strip_nl(Rest))),
            read_multi_bulk(Socket, Count, []);
        {error, Err} ->
            disconnect(Socket),
            {error, Err}
    end.

strip_nl(B) when is_binary(B) ->
    S = size(B) - size(?NL),
    <<B1:S/binary, _/binary>> = B,
    B1.
    
read_body(_Socket, -1) ->
    undefined;

read_body(_Socket, 0) ->
    <<>>;

read_body(Socket, Size) ->
    inet:setopts(Socket, [{packet, raw}]),
    case gen_tcp:recv(Socket, Size + size(?NL)) of
        {error, Error} ->
            disconnect(Socket),
            {error, Error};
        {ok, <<Body:Size/binary, _/binary>>} ->
            Body
    end.

read_multi_bulk(_Socket, 0, Acc) ->
    lists:reverse(Acc);

read_multi_bulk(Socket, Count, Acc) ->
    Resp =
        case read_resp(Socket) of
            {redis_error, Err} -> {error, Err};
            Other -> Other
        end,
    read_multi_bulk(Socket, Count-1, [Resp|Acc]).

