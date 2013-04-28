-module(partition_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() ->
	supervisor:start_link({local, partitions_sup}, ?MODULE, []).

init(_Args) ->
	{ok, {{simple_one_for_one, 5, 60},
	[{
		partition_id,
		{partition, start_link, []},
		permanent,
		2000,
		worker,
		[partition]
	}]}}.