-module(distreg_util).
-compile(export_all).

% record to proplist
rec2prop(Rec, RecordFields) ->
	loop_rec(RecordFields, 1, Rec, []).

loop_rec([H|T], N, Rec, L) ->
	loop_rec(T, N+1, Rec, [{H, element(N+1, Rec)}|L]);
loop_rec([], _, _, L) ->
	L.
 
% % convert prop list to record
prop2rec(Prop, RecName, DefRec, RecordFields) ->
	loop_fields(erlang:make_tuple(tuple_size(DefRec), RecName), RecordFields, DefRec, Prop, 2).

loop_fields(Tuple, [Field|T], DefRec, Props, N) ->
	case lists:keysearch(Field, 1, Props) of
		{value, {_, Val}} ->
			loop_fields(setelement(N, Tuple, Val), T, DefRec, Props, N+1);
		false ->
			loop_fields(setelement(N, Tuple, element(N, DefRec)), T, DefRec, Props, N+1)
	end;
loop_fields(Tuple, [], _, _, _) ->
	Tuple.
