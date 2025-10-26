declare default_val float64 default (select min(val) from project.dataset.out);

insert into `project.dataset.out`
select
    id,
    if(x is null or y is null, default_val, x+y)
from `project.dataset.t1` inner join `project.dataset.t2` using (id)
