#!/bin/sh
erlc ./*.erl && erl -s kv test -s pkv test -s init stop