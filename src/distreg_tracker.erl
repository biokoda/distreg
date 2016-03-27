-module(distreg_tracker).
-behaviour(gen_server).
-export([start/0, stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([print_info/0,reload/0,deser_prop/1]).
-include("distreg.hrl").

% When distreg_tracker starts up, check which connected nodes are running distreg. Calc consistent hashing boundaries.
% Every time a node comes online, check if it is running distreg_tracker.
% 	If not add it to ignored nodes, but still check again in 10s.
%   If yes, recalc consistent hashing boundaries and scann the table of processes.
% Every time a node goes offline, recalc and scan the table of processes.

regpid(Pid,Name) ->
	regpid(Pid,Name,?NAMET).
regpid(Pid,Name,Nametable) ->
	case ets:lookup(Nametable,Name) of
		[] ->
			case ets:lookup(?PIDT,Pid) of
				[{_,L}] ->
					ok;
				_ ->
					L = []
			end,
			case proplists:get_value(name,L) of
				undefined ->
					ets:insert(?PIDT,{Pid,lists:keystore(name,1,lists:keystore(nametable,1,L,{nametable,Nametable}),{name,Name})}),
					ets:insert(Nametable,{Name,Pid}),
					ok;
				Name ->
					ok;
				_Nm ->
					already_named
			end;
		[{Name,Pid}] ->
			ok;
		_ ->
			name_exists
	end.

unregpid(Pid) when is_pid(Pid) ->
	case ets:lookup(?PIDT,Pid) of
		[{_,L}] ->
			case proplists:get_value(name,L) of
				undefined ->
					ok;
				Name ->
					ets:delete(?NAMET,Name)
			end,
			ets:insert(?PIDT,{Pid,lists:keydelete(name,1,lists:keydelete(nametable,1,L))}),
			ok;
		_X ->
			ok
	end;
unregpid(Name) ->
	case ets:lookup(?NAMET,Name) of
		[{_,Pid}] ->
			unregpid(Pid);
		_ ->
			ok
	end.

trackpid(Pid) ->
	case ets:lookup(?PIDT,Pid) of
		[{_,_}] ->
			known;
		_ ->
			ets:insert(?PIDT,{Pid,[]}),
			ok
	end.

regglobal(L) ->
	regglobal(L,[]).
regglobal([{Name,Pid}|T],L) ->
	case regpid(Pid,Name,?NAMET_GLOBAL) of
		ok ->
			erlang:monitor(process,Pid),
			regglobal(T,L);
		already_named ->
			regglobal(T,L);
		name_exists ->
			regglobal(T,[{Name,Pid}|L])
	end;
regglobal([],L) ->
	L.


-record(dp,{problem_nodes = [], ignored_nodes = []}).
-define(R2P(Record), distreg_util:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), distreg_util:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).

handle_call({register,Pid,Name},_From,P) ->
	case regpid(Pid,Name) of
		ok ->
			erlang:monitor(process,Pid),
			{reply,ok,P};
		X ->
			{reply,X,P}
	end;
handle_call({unregister,Pid},_From,P) ->
	{reply,unregpid(Pid),P};
handle_call({getreg,Name},_From,P) ->
	{reply,distreg:whereis(Name),P};
handle_call({track,Pid},_From,P) ->
	{reply,trackpid(Pid),P};
% Called from other nodes. Sends us processes that should be running on this node.
% Return: {ok,L}
%  L - list of workers already registered locally, they need to be killed on the remote node.
% ProcInfo: [{WorkerName,WorkerPid},..]
handle_call({remote_pids,ProcInfo},_From,P) ->
	{reply,{ok,regglobal(ProcInfo)},P};
handle_call({register_global,Pid,Name},_From,P) ->
	case regpid(Pid,Name,?NAMET_GLOBAL) of
		ok ->
			erlang:monitor(process,Pid),
			{reply,ok,P};
		X ->
			{reply,X,P}
	end;
handle_call(ignored_nodes,_,P) ->
	{reply,P#dp.ignored_nodes,P};
handle_call({reload}, _, P) ->
	code:purge(?MODULE),
	code:load_file(?MODULE),
	{reply, ok, ?MODULE:deser_prop(?R2P(P))};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

deser_prop(P) ->
	?P2R(P).

handle_cast({ignore_node,Node},P) ->
	case lists:member(Node,P#dp.ignored_nodes) of
		true ->
			{noreply,P};
		false ->
			% Check again in 10s.
			erlang:send_after(10000,self(),{nodeup,Node}),
			{noreply,P#dp{ignored_nodes = [Node|P#dp.ignored_nodes]}}
	end;
handle_cast({nolonger_ignored,Node},P) ->
	{noreply,P#dp{ignored_nodes = lists:delete(Node,P#dp.ignored_nodes)}};
handle_cast(save_nodeinfo,P) ->
	save_nodeinfo(P#dp.ignored_nodes),
	{noreply,P};
handle_cast({nodeup,N},P) ->
	handle_info({nodeup,N},P);
handle_cast({nodeup,Callback,N},P) ->
	handle_info({nodeup,Callback,N},P);
handle_cast({print_info},P) ->
	io:format("~p~n", [?R2P(P)]),
	{noreply,P};
handle_cast(_, P) ->
	{noreply, P}.

handle_info({'DOWN', _Monitor, _, Pid, _Reason},P) ->
	case distreg:procinfo(Pid) of
		undefined ->
			ok;
		L ->
			case proplists:get_value(name,L) of
				undefined ->
					ok;
				Name ->
					ets:delete(proplists:get_value(nametable,L),Name)
			end,
			ets:delete(?PIDT,Pid)
	end,
	{noreply,P};
handle_info({nodeup,Node},P) ->
	handle_info({nodeup,true,Node},P);
handle_info({nodeup,Callback,Node},P) ->
	% Check if this gen_server is running on that node.
	spawn(fun() ->
		case is_node_participating(Node) of
			true ->
				case Callback of
					true ->
						% This is a safety measure to prevent inconsistencies where
						% 	a node thinking some other node is not participating
						rpc:cast(Node,gen_server,cast,[?MODULE,{nodeup,false,Node}]);
					false ->
						ok
				end,
				case lists:member(Node,P#dp.ignored_nodes) of
					true ->
						gen_server:cast(?MODULE,{nolonger_ignored,Node}),
						NL = lists:delete(Node,P#dp.ignored_nodes);
					false ->
						NL = P#dp.ignored_nodes
				end,
				save_nodeinfo_op(NL);
			false ->
				gen_server:cast(?MODULE,{ignore_node,Node})
		end
	end),
	{noreply,P};
handle_info({nodedown,_},P) ->
	save_nodeinfo(P#dp.ignored_nodes),
	{noreply,P};
handle_info({stop},P) ->
	handle_info({stop,noreason},P);
handle_info({stop,Reason},P) ->
	{stop, Reason, P};
handle_info(_, P) ->
	{noreply, P}.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.

init([]) ->
	net_kernel:monitor_nodes(true),
	case ets:info(?PIDT) of
		undefined ->
			ets:new(?PIDT, [named_table,public,{heir,whereis(distreg_sup),<<>>}]);
		_ ->
			[erlang:monitor(process,Pid) || {Pid,_Info} <- ets:tab2list(?PIDT), is_pid(Pid)]
	end,
	case ets:info(?NAMET) of
		undefined ->
			ets:new(?NAMET, [named_table,public,{heir,whereis(distreg_sup),<<>>}]);
		_ ->
			ok
	end,
	case ets:info(?NAMET_GLOBAL) of
		undefined ->
			ets:new(?NAMET_GLOBAL, [named_table,public,{heir,whereis(distreg_sup),<<>>}]);
		_ ->
			ok
	end,
	spawn(fun() ->
					Ignored = [Nd || Nd <- nodes(), is_node_participating(Nd) == false],
					[gen_server:cast(?MODULE,{ignore_node,Nd}) || Nd <- Ignored],
					save_nodeinfo_op(Ignored)
				 end),
	% Nodeup is specifically sent in the case that distreg was started after nodes have already been connected.
	% Because other nodes will have checked if distreg is running on local node and have determined that it is not.
	% Well written systems should not connect to nodes before all applications have been run however....
	gen_server:abcast(nodes(),?MODULE,{nodeup,node()}),
	{ok,#dp{}}.


is_node_participating(Node) ->
	Res = rpc:call(Node,erlang,whereis,[?MODULE]),
	is_pid(Res).


save_nodeinfo(Ignored) ->
	spawn(fun() -> save_nodeinfo_op(Ignored) end).
% HAS TO BE CALLED IN SEPERATE PROCESS
save_nodeinfo_op(IgnoredNodes) ->
	case catch register(distreg_checknodes,self()) of
		true ->
			case ets:lookup(?PIDT,nodes) of
				[{nodes,OldNodes}] ->
					ok;
				_ ->
					OldNodes = {}
			end,
			Nodes = list_to_tuple(lists:sort(lists:subtract([node()|nodes()],IgnoredNodes))),
			case OldNodes == Nodes of
				true ->
					ok;
				false ->
					Pos = find_myself(1,Nodes),
					Range = ?MAX_HASH div tuple_size(Nodes),
					MyRangeFrom = Pos*Range-Range,
					MyRangeTo1 = Pos * Range,
					case MyRangeTo1 + tuple_size(Nodes) >= ?MAX_HASH of
						true ->
							MyRangeTo = ?MAX_HASH;
						false ->
							MyRangeTo = MyRangeTo1
					end,
					ets:insert(?PIDT,[{node_range,Pos,MyRangeFrom, MyRangeTo},{nodes,Nodes}]),
					ToMove1 = ets:foldl(fun({Name,Pid},WorkersToMove) ->
											HN = erlang:phash2(Name),
											case HN >= MyRangeFrom andalso HN =< MyRangeTo of
												true ->
													WorkersToMove;
												false ->
													Node = distreg:node_for_hash(HN,Nodes),
													case node(Pid) /= Node of
														true ->
															Pid ! {distreg,shouldrestart},
															[{Node,Name,Pid}|WorkersToMove];
														false ->
															WorkersToMove
													end
											end
										end,[],?NAMET_GLOBAL),
					ToMove = group(ToMove1,[]),
					[begin
						case rpc:call(Node,gen_server,call,[?MODULE,{remote_pids,ProcInfo},20000]) of
							{ok,L} ->
								[begin
									case node(Pid) /= Node of
										true ->
											pid_conflicted(Pid),
											ets:delete(?PIDT,Pid),
											ets:delete(?NAMET_GLOBAL,Name);
										false ->
											ok
									end
								 end || {Pid,Name} <- L];
							_X ->
								gen_server:cast(?MODULE,{ignore_node,Node}),
								gen_server:cast(?MODULE,save_nodeinfo)
						end
					 end || {Node,ProcInfo} <- ToMove]
			end;
		_ ->
			Monitor = erlang:monitor(process,distreg_checknodes),
			receive
				{'DOWN', Monitor, _, _Pid, _Reason} ->
					save_nodeinfo_op(IgnoredNodes)
			end
	end.

pid_conflicted(Pid) ->
	Pid ! {distreg,dienow},
	spawn(fun() -> timer:sleep(1000),exit(Pid,kill) end).

group([{GK,V,K}|T],L) ->
	case lists:keyfind(GK,1,L) of
		false ->
			group(T,[{GK,[{V,K}]}|L]);
		{GK,KL} ->
			group(T,[{GK,[{V,K}|KL]}|lists:keydelete(GK,1,L)])
	end;
group([],L) ->
	L.

find_myself(N,Nodes) when element(N,Nodes) == node() ->
	N;
find_myself(N,Nodes) ->
	find_myself(N+1,Nodes).



start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).
stop() ->
	gen_server:call(?MODULE, stop).
print_info() ->
	gen_server:cast(?MODULE,{print_info}).
reload() ->
	gen_server:call(?MODULE, {reload}).
