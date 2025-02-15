-- with kappa as (
--     select z
--     from D
-- )
-- , tmp as (
--     select
--         t.z as s,
--         (select t.z) as s,
--         z+2*z as zdouble,
--         3,
--         (with k as (select 1 as x) select x from k) as ZX
--     from kappa t
-- )
-- select
--     zdouble
-- from tmp
-- with tmp as (
--   select
--     z as x,
--     z as y,
--     z
--   from D
-- )
-- , tmp2 as (
--   select
--   x
--   from tmp N inner join (select z as x from D) O using (x)
-- )
-- select x from tmp2
--
--
with
    f1 as (
        select
            1 as x,
            2 as y,
            5 as g
    ),
    tmp as (
        select
            1 as x,
            2 as y,
            5 as g
    ),
    tmp2 as (
        select
            2 as y,
            3 as z,
            4 as g
    ),
    final as (
        select * from (
            select
                f1.g+d.g as y
            from
                f1
                inner join project.dataset.table as d using (x, y)
                inner join tmp2 using (y)
        )
    )
select
    y
from
    final
--
-- select
--     x + (select y from C),
--     tmp.z
-- from B cross join tmp
--
--
-- with tmp as (select 1 as x)
-- select
--   1+(with tmp as (select 2 as y) select y from tmp)+tmp.x
-- from tmp
