-module(distreg_sup).
-behavior(supervisor).
-export([start_link/0, init/1,start/2,stop/1]).
-include("distreg.hrl").

start(_Type, _Args) ->
	start_link().
stop(_State) ->
	[exit(Pid,kill) || {Pid,_} <- ets:tab2list(?PIDT)],
	ok.

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
	{ok, {{one_for_one, 500, 1},
		 [
		{distreg_tracker,
		 	{distreg_tracker, start, []},
			 permanent,
			 100,
			 worker,
			[distreg_tracker]}
		]}}.
