-module(partition).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2, start_link/1]).
-behaviour(gen_server).

-include("partition.hrl").

-record(state, {start, data}).

start_link(StartHash) ->
	gen_server:start_link(?MODULE, [StartHash], []).

init([Start]) ->
	P = #partition{pid=self(),start_hash=Start},
	case mnesia:is_transaction() of
		true ->
			mnesia:write(P);
		false ->
			Exec = fun() -> mnesia:write(P) end,
			mnesia:transaction(Exec)
	end,
	{ok, #state{
		start=Start, 
		data=load(Start)
	}}.

handle_call({exists, K}, _From, State) ->
	Response = case gb_trees:is_defined(K, State#state.data) of
		true ->
			{found, K};
		false ->
			{not_found, K}
	end,
	{reply, Response, State};
handle_call({get, K}, _From, State) ->
	Response = case gb_trees:lookup(K, State#state.data) of
		none ->
			{not_found, K};
		{value, V} ->
			{found, K, V};
		_ ->
			{error, unknown_return}
	end,
	{reply, Response, State};
handle_call({split}, _From, State) ->
	{NS, H2} = split(State),
	{reply, {ok, H2}, NS};
handle_call(Msg, _From, State) ->
	{reply, {unkown_command, Msg}, State}.


handle_cast({delete, K}, State) ->
	NState = State#state{data = gb_trees:delete_any(K, State#state.data)},
	{noreply, NState};
handle_cast({store, K, V}, State) ->
	NState = State#state{data = gb_trees:enter(K, V, State#state.data)},
	{noreply, NState};
handle_cast(save, State) ->
	save(State#state.start, gb_trees:to_list(State#state.data)),
	{noreply, State};
handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, State) ->
	save(State#state.start, gb_trees:to_list(State#state.data)).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%  Split the partition in half straight down the middle
%%  This only splits the file then reloads the data.
%%  Note: partition must be locked to split

split(State) ->
	LD = gb_trees:to_list(D),
	Size = gb_trees:size(D),
	N = math:floor(Size / 2),
	{D1, D2} = lists:split(N, LD),
	{State#state{data = D1}, D2}.

%%  File operations to save/load data

seek_to_key(Fd, Key) ->
	case load_kv(Fd) of
		{K, V} when strings:equal(K, Key) ->
			{K, V};
		{_, _} ->
			seek_to_key(Fd, Key);
		eof ->
			error(key_not_found_in_file)
	end.

filename(Start) ->
	filename:absname(filename:join("data", integer_to_list(Start))).

load(Start) ->
	FileName = filename(Start),
	case filelib:is_regular(FileName) of
		true ->
			case file:open(FileName, [binary]) of
				{ok, FD} ->
					gb_trees:from_orddict(load(FD, []));
				{error, Reason} ->
					error(Reason)
			end;
		false ->
			io:format("Did you expect a file here? ~p~n", [FileName]),
			gb_trees:empty()
	end.

load(Fd, Acc) ->
	case load_kv(Fd) of
		{K, V} ->
			load(Fd, [{K, V} | Acc]);
		eof ->
			file:close(Fd),
			Acc
	end.


load_kv(Fd) ->
	case read_kvlens(Fd) of
		{KL, VL} ->
			read_kv(Fd, {KL, VL});
		eof ->
			eof
	end.

read_kv(Fd, {KL, VL}) ->
	case file:read(Fd, KL + VL) of
		{ok, D} ->
			K = binary_to_list(binary:part(D, {0, KL})),
			V = binary_to_list(binary:part(D, {KL, VL})),
			{K, V};
		eof ->
			eof;
		{error, Reason} ->
			error(Reason)
	end.

read_kvlens(Fd) ->
	case file:read(Fd, 12) of
		{ok, D} ->
			KL = binary:decode_unsigned(binary:part(D, {0, 4})),
			VL = binary:decode_unsigned(binary:part(D, {4, 8})),
			{KL, VL};
		eof ->
			eof;
		{error, Reason} ->
			error(Reason)
	end.


%% Save file

save(Start, Data) ->
	FileName = filename(Start),
	case file:open(FileName, [write]) of
		{ok, FD} ->
			save_to_fd(FD, Data);
		{error, Reason} ->
			error(Reason)
	end.
save_to_fd(Fd, []) ->
	file:close(Fd);
save_to_fd(Fd, [Head | Tail]) ->
	case file:write(Fd, serializeKV(Head)) of
		ok ->
			save_to_fd(Fd, Tail);
		{error, Reason} ->
			error(Reason)
	end.

serializeKV({K, V}) ->
	Key = list_to_binary(K),
	KL = size(Key),
	Val = list_to_binary(V),
	VL = size(Val),
	<<KL:32, VL:64, Key/binary, Val/binary>>.