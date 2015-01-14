compile:
	rebar compile skip_deps=true

all:
	rebar get-deps compile
	dialyzer -r ebin

shell:
	erl -pa ./ebin  ../deps/*/ebin

update:
	erl -pa ./ebin -pa ../deps/*/ebin -setcookie neo -noshell -hidden -name ds_update@127.0.0.1 -s neo_dbg remote_reload neo_cache@10.77.128.93 $(mod) -s init stop

update2:
	erl -pa ./ebin -pa ../deps/*/ebin -setcookie neo -noshell -hidden -name ds_update@127.0.0.1 -s neo_dbg remote_reload neo_cache@10.77.128.94 $(mod) -s init stop
