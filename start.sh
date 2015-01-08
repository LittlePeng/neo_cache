#!/bin/sh
CMD="erl -pa ebin ../deps/*/ebin\
    +K true -smp +P 2000000 \
    -name neo_cache$$@127.0.0.1 \
    -s crypto \
    -s ssl \
    -s lhttpc \
    -s lager \
    -s neo_cache_app \
    -config neo_cache"

exec $CMD
