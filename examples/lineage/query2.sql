with tmp as (
    select * except (c)
    from proj.dataset.table_1
),

tmp2 as (
    select 
        a,
        b,
        3 as c
    from proj.dataset.table_2
)

select
    *
from tmp inner join tmp2 using (a,b)