-module(rabbit_riak_queue_app).
-author("J. Brisbin <jon@jbrisbin.com>").

-export([
	start/0, 
	stop/0, 
	start/2, 
	stop/1
]).

start() ->
	ok.

stop() ->
	ok.

start(normal, []) ->
	{ok, self()}.

stop(_State) ->
	ok.