-module(distreg).
-define(CALL(Msg),gen_server:call(distreg_tracker,Msg,infinity)).
-define(CAST(Msg),gen_server:cast(distreg_tracker,Msg)).
-export([reg/1,reg/2,unreg/1,track/1, whereis/1,
				 call/2,cast/2,inform/2,start/2,
				 procinfo/1,node_for_name/1,processes/0,
				 % Used internally do not call from client.
				 node_for_hash/2]).
-include("distreg.hrl").
%  -define(NOTEST, 1).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).




procinfo(Pid) ->
	case ets:lookup(?PIDT,Pid) of
		[{Pid,L}] ->
			L;
		_ ->
			undefined
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 							Distributed worker API functions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Name: term
% StartFunction: {Module,Func,Args} | {Module,Func} | {Fun,Param} | Fun/0
% 	StartFunctionReturn: {ok,Pid} | Pid | anything else for error
start(Name,StartFunction) ->
	case get_g_pid(Name) of
		undefined ->
			Node = node_for_name(Name),
			case Node == node() of
				true ->
					case callstart(StartFunction) of
						{ok,Pid} when is_pid(Pid) ->
							reg_global(Pid,Name,StartFunction);
						Pid when is_pid(Pid) ->
							reg_global(Pid,Name,StartFunction);
						Err ->
							Err
					end;
				false ->
					rpc:call(Node,?MODULE,start,[Name,StartFunction])
			end;
		Pid ->
			{ok,Pid}
	end.


call(Name,Msg) ->
	call(Name,Msg,infinity).
call(Name,Msg,Timeout) ->
	case get_g_pid(Name) of
		undefined ->
			Node = node_for_name(Name),
			case Node == node() of
				true ->
					{error,worker_not_found};
				false ->
					rpc:call(Node,distreg,call,[Name,Msg,Timeout],Timeout)
			end;
		Pid ->
			gen_server:call(Pid,Msg,Timeout)
	end.

cast(Name,Msg) ->
	case get_g_pid(Name) of
		undefined ->
			Node = node_for_name(Name),
			case Node == node() of
				true ->
					{error,worker_not_found};
				false ->
					rpc:cast(Node,distreg,cast,[Name,Msg])
			end;
		Pid ->
			gen_server:cast(Pid,Msg)
	end.

inform(Name,Msg) ->
	case get_g_pid(Name) of
		undefined ->
			Node = node_for_name(Name),
			case Node == node() of
				true ->
					{error,worker_not_found};
				false ->
					rpc:cast(Node,distreg,inform,[Name,Msg])
			end;
		Pid ->
			Pid ! Msg
	end.


get_g_pid(Name) ->
	case ets:lookup(?NAMET_GLOBAL,Name) of
		[{Name,Pid}] ->
			Pid;
		_ ->
			undefined
	end.

node_for_name(Name) ->
	case ets:lookup(?PIDT,nodes) of
		[{nodes,Nodes}] ->
			node_for_hash(erlang:phash2(Name),Nodes);
		_ ->
			undefined
	end.
node_for_hash(HName,Nodes) ->
	Range = ?MAX_HASH div tuple_size(Nodes),
	NodePos = HName div Range + 1,
	element(min(tuple_size(Nodes),NodePos),Nodes).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 			Functions for processes that run only on local node.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Name = term
% Result: ok | already_named | name_exists
reg(Name) ->
	reg(self(),Name).
reg(Pid,Name) ->
	(catch ?CALL({register,Pid,Name})).

unreg(PidOrName) ->
	(catch ?CALL({unregister,PidOrName})).

whereis(Name) ->
	case ets:lookup(?NAMET,Name) of
		[{Name,Pid}] ->
			Pid;
		_ ->
			undefined
	end.

% Saves pid to table, but does not set any name.
track(Pid) ->
	?CALL({track,Pid}).

processes() ->
	ets:tab2list(?NAMET).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%						 			Unexported utility functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
reg_global(Pid,Name,StartFunction) ->
	case ?CALL({register_global,Pid,Name}) of
		ok ->
			{ok,Pid};
		% Name already registered, kill this pid. Calling start again will return existing PID under this name.
		name_exists ->
			exit(Pid,kill),
			start(Name,StartFunction)
	end.
callstart({Mod,Fun,Param}) ->
	apply(Mod,Fun,Param);
callstart({Fun,Param}) when is_function(Fun) ->
	apply(Fun,Param);
callstart({Mod,Fun}) ->
	apply(Mod,Fun,[]);
callstart(Fun) when is_function(Fun) ->
	Fun().








-ifdef(TEST).

-define(SLAVE1,'slave1@127.0.0.1').
-define(SLAVE2,'slave2@127.0.0.1').

startapp() ->
	application:start(distreg).
startapp(Node) ->
	{_,_,Path} = code:get_object_code(?MODULE),
	rpc:call(Node,code,add_path,[filename:dirname(Path)]),
	rpc:call(Node,application,start,[distreg]),
	timer:sleep(500),
	ok.
stopapp(_) ->
	application:stop(distreg).

startglobal() ->
	startapp(),
	case node() == 'nonode@nohost' of
		true ->
			net_kernel:start(['master@127.0.0.1',longnames]);
		_ ->
			ok
	end,
	Cookie = erlang:get_cookie(),
	ExNodes = nodes(),
	slave:start_link('127.0.0.1',slave1,"-setcookie "++atom_to_list(Cookie)),
	slave:start_link('127.0.0.1',slave2,"-setcookie "++atom_to_list(Cookie)),
	slave:start_link('127.0.0.1',slavedummy,"-setcookie "++atom_to_list(Cookie)),
	timer:sleep(1000),
	['slavedummy@127.0.0.1'|ExNodes].
stopglobal(_) ->
	stopapp(ok).

local_test_() ->
	{setup,
		fun startapp/0,
		fun stopapp/1,
		fun localtests/1}.

global_test_() ->
	{setup,
		fun startglobal/0,
		fun stopglobal/1,
		fun globaltests/1}.



localtests(_) ->
	[?_assertEqual(ok,reg(asdf)),
	 ?_assertEqual(self(),?MODULE:whereis(asdf)),
	 ?_assertEqual([{asdf,self()}],ets:tab2list(?NAMET)),
	 ?_assertEqual(ok,unreg(asdf)),
	 ?_assertEqual([],ets:tab2list(?NAMET)),
	 ?_assertEqual({self(),[]},lists:keyfind(self(),1,ets:tab2list(?PIDT)))].

globaltests(IgnoredNodes) ->
	% First compare ignored nodes, master should be the only one running distreg
	[?_assertEqual(lists:sort([?SLAVE1,?SLAVE2|IgnoredNodes]),lists:sort(?CALL(ignored_nodes))),
	 ?_assertEqual(node(),node_for_name(1)),
	 % Start distreg on first slave
	 ?_assertEqual(ok,startapp(?SLAVE1)),
	 % Slave1 no longer in ignored
	 ?_assertEqual(lists:sort([?SLAVE2|IgnoredNodes]),lists:sort(?CALL(ignored_nodes))),
	 ?_assertEqual(node(),node_for_name(1)),
	 ?_assertEqual(node(),node_for_name(2)),
	 ?_assertEqual(?SLAVE1,node_for_name(3)),
	 % Start 100 processes
	 % Check they are all spread across 2 nodes
	 % Add a node, causing processes to restart themselves
	 % Check they are spread across 3 nodes.
	 ?_assertEqual(ok,test_distproc()),
	 ?_assertEqual(ok,startapp(?SLAVE2)),
	 ?_assertEqual(lists:sort(IgnoredNodes),lists:sort(?CALL(ignored_nodes))),
	 fun test_check_rebalance/0
	].

test_distproc() ->
	L = [start_test_proc(N) || N <- lists:seq(1,100)],
	[?assertEqual(node(Pid),Node) || {_N,Pid,Node} <- L],
	[{_,AL},{_,BL}] = test_group(L,[]),
	io:format(user,"Distribution ~p~n", [{length(AL),length(BL)}]),
	?assert(length(AL) >= 30 andalso 70 >= length(AL)),
	?assert(length(BL) >= 30 andalso 70 >= length(BL)),
	ok.
% Workers now running over three nodes
test_check_rebalance() ->
	timer:sleep(2000),
	[?assertEqual(node_for_name(N),node(test_get_pid(N))) || N <- lists:seq(1,100)],
	L = [{N,test_get_pid(N),node_for_name(N)} || N <- lists:seq(1,100)],
	[{_,AL},{_,BL},{_,CL}] = test_group(L,[]),
	io:format(user,"Distribution ~p~n", [{length(AL),length(BL),length(CL)}]),
	?assert(length(AL) >= 25 andalso 45 >= length(AL)),
	?assert(length(BL) >= 25 andalso 45 >= length(BL)),
	?assert(length(CL) >= 25 andalso 45 >= length(CL)),
	ok.

test_get_pid(Name) ->
	case get_g_pid(Name) of
		undefined ->
			Node = node_for_name(Name),
			rpc:call(Node,?MODULE,get_g_pid,[Name]);
		Pid ->
			Pid
	end.

test_group([{N,Pid,Node}|T],L) ->
	case lists:keyfind(Node,1,L) of
		false ->
			test_group(T,[{Node,[{N,Pid}]}|L]);
		{Node,KL} ->
			test_group(T,[{Node,[{N,Pid}|KL]}|lists:keydelete(Node,1,L)])
	end;
test_group([],L) ->
	L.

start_test_proc(N) ->
	{ok,Pid} = start(N,fun() -> spawn(?MODULE,test_proc,[self(),N]) end),
	{N,Pid,node_for_name(N)}.
test_proc(Home,N) ->
	receive
		{distreg,shouldrestart} ->
			Home ! {self(),should},
			spawn(fun() -> timer:sleep(100), start_test_proc(N) end);
		{distreg,dienow} ->
			Home ! {self(),dienow}
	end.

-endif.
