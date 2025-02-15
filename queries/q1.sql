select
    --this is a comment
    'pippo' as pippo,
    "zippo" as zippo,
    32.1E4+5 as pi,
    123.456e-67 as b,
    .1E4 as c,
    +58. as d,
    4e2 as e,
    +37 as f,
    -52 as g
from `project.dataset.table`
/* this is multiline comment
aaaa
bbbb
*/
where 1=1
order by c desc