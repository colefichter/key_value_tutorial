Key/Value Server Tutorial In Erlang
===================================

The source code in this repo is for the tutorial at [http://erlang-tutorials.colefichter.ca/kv1.html](http://erlang-tutorials.colefichter.ca/kv1.html).

The file [kv.erl](kv.erl) implements an in-memory version of the key/value server. It always starts with an empty set of key/value pairs and does not retain its state at shutdown.

The file [pkv.erl](pvk.erl) contains a persistent version of the key/value server. At shutdown it writes the key/value pairs to disk. When it starts up again it will load the key/value pairs from the disk file (if available) to restore the previous state.
