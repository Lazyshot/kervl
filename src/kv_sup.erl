-module(kv_sup).
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link(?MODULE, []).

init(_Args) ->
    {ok, {{one_for_one, 5, 60},
    [{
        partitions_sup,
        {partition_sup, start_link, []},
        permanent,
        2000,
        supervisor,
        [partition_sup]
    },
    {
        kvserv,
        {kvserv, start_link, []},
        permanent,
        2000,
        worker,
        [kvserv]
    }]}}.