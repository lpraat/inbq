-- select
--     struct(t.f0 as x).x as K
-- from proj.dataset.in_table t;


-- select struct(f0 as x).x as K
-- from proj.dataset.in_table t;


-- select 
--     struct(f0 as z, f0 as z2) as k,
--     struct(struct(f0 as x) as s).s.x as z
-- from proj.dataset.in_table t;

-- select 
--     -- struct(1 as x, [t.f0,f0] as y).y[0],
--     struct(struct([struct(t.f0 as x)] as sin, struct(t.f1 as y)) as s).s.sin[0].x as k
-- from proj.dataset.in_table t;


-- select 
--     f1.a as k
-- from proj.dataset.in_table t;



-- select
--     struct(t.f0 as x).x + f1.a as K
-- from proj.dataset.in_table as t;



-- select struct(
--   struct(3 as y) as s1,
--   struct(f0 as z1, 5 as z2) as s2
-- ).s2.z1 as K
-- from proj.dataset.in_table t;

-- struct(f1 as x).*

-- select 
--     struct(f1 as x).*
--     -- f2.a.y[0],
--     -- struct(struct(f0 as x) as z).z.x
-- from proj.dataset.in_table t;

-- with tmp as (
--   select struct(1 as x) as tmp, 3 as z
-- )

-- select 
-- f3.a[0].x

-- from proj.dataset.in_table t
-- 
INSERT INTO proj.dataset.output_table
WITH struct_to_array AS (
  SELECT 
    id,
    [struct1, struct2] AS combined_structs
  FROM proj.dataset.struct_table
)
SELECT
  s.id,
  item.field1,
  item.field2
FROM 
  struct_to_array s,
  s.combined_structs AS item
-- TODO: test same wit h <struct>
-- 

-- select 
--    struct<z struct<x int64>, y int64>(struct(f0), f1.a) as S
--    from proj.dataset.in_table
   

-- select 
--     struct(struct(f0 as x) as z, f1.a as y) as S
--     from proj.dataset.in_table