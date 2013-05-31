{application, distreg, [{description, "Distributed process registry."},
				{vsn, "0.1"},
				{modules, [distreg_sup,distreg_tracker]},
				{registered, [distreg_sup,distreg_tracker]},
				{applications, [kernel, stdlib]},
				{mod, {distreg_sup, []}},
				{start_phases, []}
				]}.
