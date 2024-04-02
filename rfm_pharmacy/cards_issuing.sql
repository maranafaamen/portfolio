-- Находим количество продаж за неделю
with sales_by_week as (
	select
		to_char(dr_dat, 'WW') as week
		, count(distinct dr_nchk) as sales
	from
		sales
	group by to_char(dr_dat, 'WW')
)
-- Находим количество регистраций бонусных карт за неделю
, registrations_by_week as (
	select
		registration_week
		,count (bonus_card) as registrations
	from
		(
		select
			distinct dr_bcdisc as bonus_card
			, first_value (to_char(dr_dat, 'WW')) over (partition by dr_bcdisc) as registration_week
		from
			sales
		group by to_char(dr_dat, 'WW'), dr_bcdisc
	) t
group by registration_week
)
-- Объединяем таблицы
select
	sbw.week
	, sales
	, registrations
from
	sales_by_week sbw
join
	registrations_by_week rbw
on rbw.registration_week = sbw.week