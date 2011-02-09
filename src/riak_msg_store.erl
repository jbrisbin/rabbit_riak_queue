-module(riak_msg_store).
-author("J. Brisbin <jon@jbrisbin.com>").

-behaviour(gen_server2).

-include("rabbit_riak_queue.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

% gen_server2 callbacks
-export([
	start_link/2,
	init/1, 
	handle_call/3, 
	handle_cast/2, 
	handle_info/2,
	terminate/2, 
	code_change/3, 
	prioritise_call/3, 
	prioritise_cast/2
]).
% API
-export([
	push/5,
	pop/3,
	len/2,
	purge/2
]).

-record(state, { server, config, riak }).

start_link(Server, Config) -> 
  gen_server2:start_link({local, Server}, ?MODULE, [Server, Config], []).
  
init([Server, Config]) ->
	Host = proplists:get_value(riak_pb_host, Config, "localhost"),
	Port = proplists:get_value(riak_pb_port, Config, 8087),
	{ok, Riak} = riakc_pb_socket:start_link(Host, Port),
	{ok, #state{ server=Server, config=Config, riak=Riak }}.
	
handle_call({purge, _QueueName}, _From, State=#state{ server=_Server, config=Config, riak=Riak }) ->
	Bucket = get_bucket(Config),
	{ok, Keys} = riakc_pb_socket:list_keys(Riak, Bucket),
	lists:foldl(fun(Key, _Acc) ->
		riakc_pb_socket:delete(Riak, Bucket, Key)
	end, [], Keys),
	{reply, ok, State};

handle_call({len, QueueName}, _From, State=#state{ server=_Server, config=Config, riak=Riak }) ->
	Bucket = get_bucket(Config),
	{ok, [{_, [Len]}]} = riakc_pb_socket:mapred_bucket(Riak, Bucket, [
		{map, {jsanon, list_to_binary(io_lib:format("function(v){ ejsLog('/tmp/mapred.log', JSON.stringify(v)); if(v.values[0].metadata['X-Riak-Meta']['X-Riak-Meta-Queue'] == '~s'){ return [1]; }else{ return [0]; }}", [binary_to_list(QueueName#resource.name)]))}, undefined, false},
		{reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}
	]),
	%rabbit_log:info(" ~p length: ~p", [QueueName#resource.name, Len]),
	{reply, Len, State};
	
handle_call({pop, QueueName, AckRequired}, _From, State=#state{ server=_Server, config=Config, riak=Riak }) ->
	Bucket = get_bucket(Config),
	{ok, [{_, Keys}]} = riakc_pb_socket:mapred_bucket(Riak, Bucket, [
		{map, {jsanon, list_to_binary(io_lib:format("function(v){ ejsLog('/tmp/mapred.log', JSON.stringify(v)); if(v.values[0].metadata['X-Riak-Meta']['X-Riak-Meta-Queue'] == '~s'){ return [parseInt(v.key)]; }else{ return []; }}", [binary_to_list(QueueName#resource.name)]))}, undefined, false},
		{reduce, {jsanon, <<"function(v){ return [v.sort()[0]+'']; }">>}, undefined, true}
	]),

	Reply = case length(Keys) of
		0 -> empty;
		_ -> 
			Key = lists:nth(1, Keys),
			case riakc_pb_socket:get(Riak, Bucket, Key) of
				{error, notfound} -> empty;
				{ok, Robj} ->
					V = riakc_obj:get_value(Robj),
					CT = riakc_obj:get_content_type(Robj),
					MsgProps = riakc_obj:get_metadata(Robj),
					{ClassId, Guid, ExchangeName, RoutingKey, CMeta, SeqId} = try 
						CustomMeta = dict:from_list(dict:fetch(?META, MsgProps)),
						{ClassIdInt, _} = string:to_integer(dict:fetch(binary_to_list(?CLASS_ID), CustomMeta)),
						CustomMeta2 = dict:erase(?CLASS_ID, CustomMeta),
						
						GuidStr = base64:decode(dict:fetch(binary_to_list(?GUID), CustomMeta2)),
						CustomMeta3 = dict:erase(?GUID, CustomMeta2),
						
						{Seq, _} = string:to_integer(dict:fetch(binary_to_list(?SEQ_ID), CustomMeta3)),
						CustomMeta4 = dict:erase(binary_to_list(?SEQ_ID), CustomMeta3),

						Vhost = dict:fetch(binary_to_list(?VHOST), CustomMeta4),
						CustomMeta5 = dict:erase(binary_to_list(?VHOST), CustomMeta4),
						
						Exchange = #resource{ virtual_host=list_to_binary(Vhost), kind=exchange, name=list_to_binary(dict:fetch(binary_to_list(?EXCHANGE), CustomMeta5)) },
						CustomMeta6 = dict:erase(binary_to_list(?EXCHANGE), CustomMeta5),
						
						RoutingKeyStr = list_to_binary(dict:fetch(binary_to_list(?ROUTING_KEY), CustomMeta6)),
						CustomMeta7 = dict:erase(binary_to_list(?ROUTING_KEY), CustomMeta6),
						
						{ClassIdInt, GuidStr, Exchange, RoutingKeyStr, CustomMeta7, Seq}
					catch _ ->
						"application/octet-stream"
					end,
					
					case AckRequired of
						true -> riakc_pb_socket:delete(Riak, Bucket, Key)
					end,
					
					% Send the message back
					Props = #'P_basic'{ content_type=CT, headers=CMeta },
					Content = #content{ class_id=ClassId, properties=Props, payload_fragments_rev=[V] },
					{#basic_message{ exchange_name=ExchangeName, routing_key=RoutingKey, content=Content, guid=Guid }, SeqId}
			end
	end,
	
	{reply, Reply, State};
	
handle_call({push, [QueueName, SeqId, _Msg=#basic_message{
			exchange_name=ExchangeName,
			routing_key=RoutingKey,
			content=#content{
				class_id=ClassId,
				properties=#'P_basic'{
					content_type=ContentType, 
					headers=Headers
				},
				payload_fragments_rev=Payload
			},
			guid=Guid
		}, _MsgProps]}, _From, State=#state{ server=_Server, config=Config, riak=Riak }) ->
			
	Key = lists:flatten(io_lib:format("~p", [get_timestamp()])),
	
	% Get bucket to store msgs in
	Bucket = get_bucket(Config),
	
	% Get the body of the message
	[Body] = lists:flatten(Payload),

	Props = [
		{?META, lists:foldl(fun(I, Acc) ->
				Acc ++ [case I of
					{K, _T, V} when is_binary(V) -> {K, binary_to_list(V)};
					{K, _T, V} when is_integer(V) -> {K, io_lib:format("~p", [V])};
					{K, _T, V} -> {K, V}
				end]
			end, [
				{?GUID, base64:encode(Guid)},
				{?CLASS_ID, io_lib:format("~p", [ClassId])},
				{?QUEUE, QueueName#resource.name},
				{?VHOST, ExchangeName#resource.virtual_host},
				{?EXCHANGE, ExchangeName#resource.name},
				{?ROUTING_KEY, RoutingKey},
				{?SEQ_ID, io_lib:format("~p", [SeqId])}
			], Headers)},
		{<<"content-type">>, binary_to_list(ContentType)}
	],	
	Robj = riakc_obj:update_metadata(riakc_obj:new(Bucket, Key, Body), dict:from_list(Props)),
	
	% PUT object
	ok = riakc_pb_socket:put(Riak, Robj),
	
	{reply, ok, State};

handle_call(Msg, _From, State=#state{ config=_Config }) ->
	rabbit_log:warning(" Unkown call: ~p~n State: ~p~n", [Msg, State]),
	{noreply, State}.

handle_cast(Msg, State=#state{ config=_Config }) ->
	rabbit_log:warning(" Unkown cast: ~p~n State: ~p~n", [Msg, State]),
	{noreply, State}.

handle_info(Msg, State) ->
	rabbit_log:warning(" Unkown message: ~p~n State: ~p~n", [Msg, State]),
	{noreply, State}.

terminate(_, #state{ config=_Config }) -> 
	%rabbit_log:info("Terminating ~p~n", [self()]),
	ok.

code_change(_OldVsn, State, _Extra) -> 
	{ok, State}.

prioritise_call(_Msg, _From, _State) ->
	%rabbit_log:info("pcall ~p, ~p~n", [Msg, State]),
	0.
	
prioritise_cast(_Msg, _State) ->
	%rabbit_log:info("pcall ~p, ~p~n", [Msg, State]),
	0.

push(Server, QueueName, SeqId, Msg, MsgProps) ->
	gen_server2:call(Server, {push, [QueueName, SeqId, Msg, MsgProps]}).
	
pop(Server, QueueName, AckRequired) ->
	gen_server2:call(Server, {pop, QueueName, AckRequired}).
	
len(Server, QueueName) ->
	gen_server2:call(Server, {len, QueueName}).

purge(Server, QueueName) ->
	gen_server2:call(Server, {purge, QueueName}).
	
%md5sum(What) ->
%	lists:flatten([io_lib:format("~2.16.0b", [I]) || <<I>> <= crypto:md5(What)]).
	
get_bucket(Config) ->
	case proplists:get_value(riak_msg_bucket, Config, <<"riak_msg_store">>) of
		B when is_binary(B) -> B;
		B when is_list(B) -> list_to_binary(B)
	end.
	
get_timestamp() ->
	{Mega, Sec, Micro} = erlang:now(),
	(((Mega * 1000000 + Sec) * 1000000) + Micro).