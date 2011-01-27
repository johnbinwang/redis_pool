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
-module(redis_tests).
-include_lib("eunit/include/eunit.hrl").

redis_test() ->
    application:start(sasl),
    application:start(redis),
    {ok, Pid} = redis:start_link([]),

    %% STRINGS
    ?assertEqual(<<"OK">>, redis:q(Pid, ["FLUSHALL"])),
    ?assertEqual(undefined, redis:q(Pid, ["GET", "foo"])),
    ?assertEqual(<<"OK">>, redis:q(Pid, ["SET", "foo", "bar"])),
    ?assertEqual(<<"bar">>, redis:q(Pid, ["GET", "foo"])),
    ?assertEqual(1, redis:q(Pid, ["DEL", "foo"])),

    %% SETS
    ?assertEqual([], redis:q(Pid, ["SMEMBERS", "foo"])),
    ?assertEqual(1, redis:q(Pid, ["SADD", "foo", "bar"])),
    ?assertEqual(1, redis:q(Pid, ["SADD", "foo", "baz"])),
    ?assertEqual([<<"baz">>, <<"bar">>], redis:q(Pid, ["SMEMBERS", "foo"])),

    ok.

redis_pool_test() ->
    application:start(sasl),
    application:start(redis),
    {ok, Pool} = redis_pool:add([], 2),
    
    ?assertEqual(<<"OK">>, redis_pool:q(Pool, ["FLUSHALL"])),
    ?assertEqual(<<"OK">>, redis_pool:q(Pool, ["SET", "foo", "bar"])),
    ?assertEqual(<<"bar">>, redis_pool:q(Pool, ["GET", "foo"])),

    ok.
