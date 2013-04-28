-module(kvserv).

-behaviour(gen_server).

-include("partition.hrl").

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2, code_change/3, start_link/0]).
-export([store/2, delete/1, get/1, exists/1, save/0]).

%% Client Interface Functions

store(K, V) ->
	gen_server:cast(?MODULE, {store, K, V}).
delete(K) ->
	gen_server:cast(?MODULE, {delete, K}).
get(K) ->
	gen_server:call(?MODULE, {get, K}).
exists(K) ->
	gen_server:call(?MODULE, {exists, K}).
save() ->
	gen_server:cast(?MODULE, save).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Internal Functions

init(_Args) ->
	mnesia:start(),
	mnesia:create_table(partition, [{attributes, record_info(fields, partition)}]),
	create_partition(0),
	{ok, true}.

handle_cast(C = {_, K}, State) ->
	P = consistent_hashing(K),
	gen_server:cast(P, C),
	{noreply, State};

handle_cast(C = {_, K, _}, State) ->
	P = consistent_hashing(K),
	gen_server:cast(P, C),
	{noreply, State};
handle_cast(save, State) ->
	save_all_partitions(),
	{noreply, State}.

handle_call(C = {_, K}, _F, State) ->
	P = consistent_hashing(K),
	Reply = gen_server:call(P, C),
	{reply, Reply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

code_change(_, State, _) ->
	{ok, State}.

terminate(Reason, State) ->
	save_all_partitions(),
	mnesia:stop(),
	io:format("Error with kvserv: ~p~n", [Reason]),
	State.

create_partition(Start) ->
	case supervisor:start_child(partitions_sup, [Start]) of
		{ok, Pid} ->
			Pid;
		Reason ->
			error(Reason)
	end.

%% combine match_partition and string_hash
consistent_hashing(Key) ->
	match_partition(erlang:phash2(Key)).


%% find partition by hashcode
match_partition(Hash) ->
	Exec = fun() ->
		Head = #partition{pid='$1', start_hash='$2'},
		Guards = [{'<', '$2', Hash}],
		case mnesia:select(partition, [{Head, Guards, ['$1']}]) of
			[P | _] ->
				P;
			[] ->
				create_partition(Hash)
		end
	end,
	{atomic, Pid} = mnesia:transaction(Exec),
	Pid.

%% Save all the partitions
save_all_partitions() ->
	Iterator = fun(Rec,_)->
					gen_server:cast(Rec#partition.pid, save),
					[]
				end,
	case mnesia:is_transaction() of
		true -> mnesia:foldl(Iterator,[],partition);
		false -> 
			Exec = fun({Fun,Tab}) -> mnesia:foldl(Fun, [],Tab) end,
			mnesia:activity(transaction,Exec,[{Iterator,partition}],mnesia_frag)
	end.