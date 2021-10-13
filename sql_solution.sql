create temporary table mydupes as select count(1), creation_date, id
from source_starlink
group by creation_date, id
having count(1) > 1;

drop table if exists source_starlink_dupes; 

create table source_starlink_dupes as 
select ss.* from source_starlink ss 
inner join mydupes m
on ss.id = m.id
and ss.creation_date = m.creation_date;

delete from source_starlink using mydupes m
where source_starlink.id = m.id
and source_starlink.creation_date = m.creation_date;

drop table mydupes;

drop function if exists public.getlocation(character varying, timestamp with time zone);

drop table if exists timeseries_starlink;

create table timeseries_starlink as
select 
to_timestamp(src.creation_date , 'YYYY-MM-DDTHH:MI:SS') as start_time, 
min(to_timestamp(dest.creation_date , 'YYYY-MM-DDTHH:MI:SS') - 1 * interval '1 millisecond' ) as end_time , 
src.id, src.longitude, src.latitude
from source_starlink src
inner join source_starlink dest 
on src.id = dest.id
and src.creation_date < dest.creation_date
group by src.creation_date, src.id, src.longitude, src.latitude;


insert into timeseries_starlink
select distinct to_timestamp(src.creation_date , 'YYYY-MM-DDTHH:MI:SS') as start_time, 
to_timestamp('9999-12-31T23:59:59', 'YYYY-MM-DDTHH:MI:SS') as end_time,
src.id, src.longitude, src.latitude
from source_starlink src
where not exists(select * from timeseries_starlink l where l.id = src.id and l.start_time = to_timestamp(src.creation_date , 'YYYY-MM-DDTHH:MI:SS'));

CREATE OR REPLACE FUNCTION public.getlocation(character varying, timestamp with time zone)
 RETURNS SETOF timeseries_starlink
 LANGUAGE sql
AS $function$
    SELECT * FROM timeseries_starlink WHERE id = $1 and $2 between start_time and end_time
$function$
;

