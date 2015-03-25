-module(pkv).

-export([start/0, stop/0, store/1, lookup/1, delete/1, hash/1]).

-export([server_loop/1]).

-define(DISK_FILE, "keys_and_values.dat").

%---------------------------------------------------------------------------------------------
% Client API
%---------------------------------------------------------------------------------------------

% Start the KV-store.
start() ->
    crypto:start(),
    Dict = read_from_disk(),
    Pid = spawn(?MODULE, server_loop, [Dict]),
    register(?MODULE, Pid),
    ok.

stop() -> ?MODULE ! {stop}.

% Add a new value to the the KV-store. The return value is the new KEY related to the value V.
store(V) -> rpc({store, V, self()}).

% Lookup a value in the KV-store. If the key is not found, return the atom not_found.
lookup(K) -> rpc({lookup, K, self()}).

% Delete an existing key K. Returns ok or not_found.
delete(K) -> rpc({delete, K, self()}).

%---------------------------------------------------------------------------------------------
% Server Implementation
%---------------------------------------------------------------------------------------------
server_loop(Dict) ->
    receive
        {store, V, Client} ->
            Hash = hash(V),
            Dict2 = dict:store(Hash, V, Dict),
            Client ! {hash, Hash},
            server_loop(Dict2);
        {lookup, K, Client} ->
            case dict:find(K, Dict) of
                {ok, Value} -> Client ! {value, Value};
                error       -> Client ! {value, not_found}
            end,
            server_loop(Dict);
        {delete, K, Client} ->
            Dict2 = case dict:is_key(K, Dict) of
                true -> dict:erase(K, Dict);
                false -> Dict
            end,
            Client ! {deleted, ok},
            server_loop(Dict2);
        {stop} -> 
            write_to_disk(Dict),
            exit(shutdown)
    end.

write_to_disk(Dict) ->
    L = dict:to_list(Dict),
    unconsult(?DISK_FILE, L).

read_from_disk() ->
    case  file:consult(?DISK_FILE) of
        {ok, List} -> dict:from_list(List);
        {error, _} -> dict:new()
    end.

%---------------------------------------------------------------------------------------------
% Utilities
%---------------------------------------------------------------------------------------------
rpc(Message) ->    
    ?MODULE ! Message,
    receive
        {hash, Hash} -> Hash;
        {value, Value} -> Value;
        {deleted, Outcome} -> Outcome
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

%---------------------------------------------------------------------------------------------
% Unit Tests
%---------------------------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

store_test() ->
    start(),
    Key = store(my_value),
    ?assert(erlang:is_list(Key)),
    ?assertEqual("b58b5a8ced9db48b30e008b148004c1065ce53b1", store("One")),
    stop().

delete_test() ->
    start(),
    Key = store(my_value),
    ok = delete(Key),
    ?assertEqual(not_found, lookup(Key)),
    stop().

delete_missing_key_test() ->
    start(),        
    ?assertEqual(ok, delete(not_a_key)),
    stop().

lookup_test() ->
    start(),
    Key = store(my_value),
    ?assertEqual(my_value, lookup(Key)),
    stop().

lookup_missing_key_test() ->
    start(),
    ?assertEqual(not_found, lookup(not_a_key)),
    stop().

start_test() ->
    ?assertEqual(undefined, whereis(?MODULE)),
    start(),
    Pid = whereis(?MODULE),
    ?assert(is_pid(Pid)),
    stop().

stop_test() ->
    start(),
    Pid = whereis(?MODULE),
    ?assert(is_pid(Pid)),
    stop(),
    timer:sleep(50), % Wait for message passing to complete.
    ?assertEqual(undefined, whereis(?MODULE)).

hash_returns_string_test() ->
    crypto:start(),
    ?assert(is_list(hash("TESTING TESTING 123!"))).

hash_test() ->
    crypto:start(),
    ?assertEqual("b58b5a8ced9db48b30e008b148004c1065ce53b1", hash("One")),
    ?assertEqual("fe05bcdcdc4928012781a5f1a2a77cbb5398e106", hash(one)).

read_from_disk_no_file_test() ->
    file:delete(?DISK_FILE),
    Dict = read_from_disk(),
    ?assertEqual(0, length(dict:to_list(Dict))).

read_from_disk_with_file_test() ->
    L = [{"54321","Joe"}, {"12345","Cole"}],
    unconsult(?DISK_FILE, L),
    Dict = read_from_disk(),
    ?assertEqual(2, length(dict:to_list(Dict))).

write_to_disk_test() ->
    D1 = dict:store("12345", "Cole", dict:new()),
    D2 = dict:store("54321", "Joe", D1),
    L = dict:to_list(D2),
    write_to_disk(D2),
    {ok, L2} = file:consult(?DISK_FILE),
    ?assertEqual(L, L2).