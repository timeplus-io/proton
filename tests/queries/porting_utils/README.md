Example:
python3 create_mv_from_search.py ./remote.search remote.sh -f '/Users/,.sql,.sh,.reference,.py,.python,.expect,.queries' -s '/Users/,.sql,.sh,.reference,.py,.python,.expect,.queries' -r 'mv /Users/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/remote/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/remote/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/remote/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/remote/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/remote/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/remote/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/remote/'


python3 create_mv_from_search.py ./remote.search remote.sh -f '/Users/,.sql,.sh,.reference,.py,.python,.expect,.queries' -s '/Users/,.sql,.sh,.reference,.py,.python,.expect,.queries' -r 'mv /Users/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/1_stateful/remote/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/1_stateful/remote/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/1_stateful/remote/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/1_stateful/remote/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/1_stateful/remote/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/1_stateful/remote/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/1_stateful/remote/'


python3 create_mv_from_search.py ./remote.search remote.sh -f '/Users/,.sql,.sh' -s '/Users/,.sql,.sh' -r 'mv /Users/,.* /Users/jameshao/projects/proton/tests/queries_ported/bugs/remote/,.* /Users/jameshao/projects/proton/tests/queries_ported/bugs/remote/'



python3 create_mv_from_search.py ./remote.search remote.sh -f '/Users/,.sql,.sh,.reference,.py,.python,.expect,.queries' -s '/Users/,.sql,.sh,.reference,.py,.python,.expect,.queries' -r 'rm /Users/,.*,.*,.* ,.*,.*,.* ,.*'
