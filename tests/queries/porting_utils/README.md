Example:
python3 create_mv_from_search.py ./currentDatabase.search currentDatabase.sh -f '/Users/,.sql,.sh,.reference,.py,.python,.expect,.queries' -s '/Users/,.sql,.sh,.reference,.py,.python,.expect,.queries' -r 'mv /Users/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/currentDatabase/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/currentDatabase/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/currentDatabase/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/currentDatabase/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/currentDatabase/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/currentDatabase/,.* /Users/jameshao/projects/proton/tests/queries_not_supported/0_stateless/currentDatabase/'


python3 create_mv_from_search.py ./currentDatabase.search currentDatabase.sh -f '/Users/,.sql,.sh' -s '/Users/,.sql,.sh' -r 'mv /Users/,.* /Users/jameshao/projects/proton/tests/queries_ported/bugs/currentDatabase/,.* /Users/jameshao/projects/proton/tests/queries_ported/bugs/currentDatabase/'



python3 create_mv_from_search.py ./currentDatabase.search currentDatabase.sh -f '/Users/,.sql,.sh,.reference,.py,.python,.expect,.queries' -s '/Users/,.sql,.sh,.reference,.py,.python,.expect,.queries' -r 'rm /Users/,.*,.*,.* ,.*,.*,.* ,.*'
