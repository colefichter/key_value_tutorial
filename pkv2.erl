-module(pkv2).

-behaviour(gen_server).

% Client API
-export([start_link/0, store/1, lookup/1, delete/1, hash/1]).

% Server Implementation
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2, code_change/3]).

-define(DISK_FILE, "keys_and_values.dat").

%---------------------------------------------------------------------------------------------
% Client API
%---------------------------------------------------------------------------------------------
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% Add a new value to the the KV-store. The return value is the new KEY related to the value V.
store(V) -> gen_server:call(?MODULE, {store, V}).

% Lookup a value in the KV-store. If the key is not found, return the atom not_found.
lookup(K) -> gen_server:call(?MODULE, {lookup, K}).

% Delete an existing key K. Always returns {deleted, ok}, even if the key does not exist.
delete(K) -> gen_server:call(?MODULE, {delete, K}).

%---------------------------------------------------------------------------------------------
% Server Implementation
%---------------------------------------------------------------------------------------------
init([]) ->
    crypto:start(),
    Dict = read_from_disk(),
    {ok, Dict}.

handle_cast(_any, State) -> {noreply, State}.

handle_call({store, V}, _From, Dict) ->
    Hash = hash(V),
    Dict2 = dict:store(Hash, V, Dict),
    {reply, {hash, Hash}, Dict2};
handle_call({lookup, K}, _From, Dict) ->
    Reply = case dict:find(K, Dict) of
        {ok, Value} -> {value, Value};
        error       -> {value, not_found}
    end,
    {reply, Reply, Dict};
handle_call({delete, K}, _From, Dict) ->
    Dict2 = case dict:is_key(K, Dict) of
        true -> dict:erase(K, Dict);
        false -> Dict
    end,
    {reply, {deleted, ok}, Dict2}.

handle_info(Message, State) ->
    io:format("Unexpected message ~p~n", [Message]),
    {noreply, State}.

terminate(shutdown, Dict) ->
    write_to_disk(Dict),
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%---------------------------------------------------------------------------------------------
% Utilities
%---------------------------------------------------------------------------------------------
write_to_disk(Dict) ->
    L = dict:to_list(Dict),
    unconsult(?DISK_FILE, L).

read_from_disk() ->
    case  file:consult(?DISK_FILE) of
        {ok, List} -> dict:from_list(List);
        {error, _} -> dict:new()
    end.

hash(Data) when is_atom(Data) -> hash(atom_to_list(Data));
hash(Data) ->
    Binary160 = crypto:hash(sha, Data),
    hexstring(Binary160).

% Convert a 160-bit binary hash to a hex string.
hexstring(<<X:160/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~40.16.0b", [X])).

unconsult(File, L) ->
    {ok, S} = file:open(File, write),
    lists:foreach(fun(X) -> io:format(S, "~p.~n", [X]) end, L),
    file:close(S).