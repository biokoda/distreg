% Public ETS tables:

% Every registered process has a value in PIDT ETS: 
% 	{Pid,[{name,Name},{nametable,[?NAMET|?NAMET_GLOBAL]},{cleanup,Function},..]}
% Info about local node and tuple of all nodes (sorted by name) is stored in this ETS as well.
% 	{nodes,{node1,node2,node3,...}}
% 	{node_range,LocalNodePos,LocalNodeFrom,LocalNodeTo} -> workers are spread across nodes using consistent hashing with phash2
-define(PIDT,distreg_pids).
% {Name,Pid}
-define(NAMET,distreg_names).
% {Name,Pid}
-define(NAMET_GLOBAL,distreg_names_global).
% pow(2,27)
-define(MAX_HASH,134217728).
-define(LOCK,distreg_lock).