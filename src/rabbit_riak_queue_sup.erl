%%% -------------------------------------------------------------------
%%% Author  : J. Brisbin <jon@jbrisbin.com>
%%% Description : Description of plugin
%%%
%%% Created : Date created
%%% -------------------------------------------------------------------
-module(rabbit_riak_queue_sup).
-author("J. Brisbin <jon@jbrisbin.com>").

-behaviour(supervisor).

-include("rabbit_riak_queue.hrl").

-export([start_link/0, init/1]).

%% --------------------------------------------------------------------
%% Macros
%% --------------------------------------------------------------------
-define(SERVER, ?MODULE).

%% --------------------------------------------------------------------
%% Records
%% --------------------------------------------------------------------

%% ====================================================================
%% External functions
%% ====================================================================
start_link() -> 
	io:format("Configuring rabbit_riak_queue...", []),
	Pid = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
	io:format("done~n", []),
	Pid.

%% ====================================================================
%% Server functions
%% ====================================================================
%% --------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok,  {SupFlags,  [ChildSpec]}} |
%%          ignore                          |
%%          {error, Reason}
%% --------------------------------------------------------------------
init([]) ->
	Config = case application:get_env(riak_msg_store) of
		{ok, C} -> [C];
		_ -> []
	end,
	{ok, {{one_for_one, 3, 10}, [{
		riak_msg_store, {riak_msg_store, start_link, Config},
		permanent,
		10000,
		worker,
		[ riak_msg_store ]
	}]}}.
