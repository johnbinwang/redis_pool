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
-module(redis_subscribe).
-export([connect/3,unsubscribe/2]).

connect(Key, Opts, {M,F,A}=Callback) when is_binary(Key), is_list(Opts), is_atom(M), is_atom(F), is_list(A) ->
    connect1(Key, Opts, Callback);
connect(Key, Opts, Callback) when is_binary(Key), is_list(Opts), is_function(Callback) ->
    connect1(Key, Opts, Callback);
connect(Key, Opts, {Fun, Args}=Callback) when is_binary(Key), is_list(Opts), is_function(Fun), is_list(Args) ->
    connect1(Key, Opts, Callback).

connect1(Key, Opts, Callback) ->
	case redis:start_link(undefinded, Opts) of
		{ok,Pid} -> 
			redis:subscribe(Pid, Key, Callback),
			{ok,Pid};	
		_ -> err
	end.
unsubscribe(Pid,Key) ->
	ok = redis:unsubscribe(Pid,Key),
	redis:stop(Pid),
	{ok,Pid}.  