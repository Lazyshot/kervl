-module(balancer).

-behaviour(gen_server).

start_link() ->
	gen_server:start_link(?module, []).

init(Args) ->
	{ok, true}.

handle_call({split, P}, From, _) ->

	
split(P) ->
	
	{}.