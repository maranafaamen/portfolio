with count_sales as (
	select
		dr_bcdisc as bonus_card
		, count(distinct dr_nchk) as sales_cnt
		, make_date(2022,06,09) - max(dr_dat) as days_from_last_purchase
	from 
		sales
	where dr_bcdisc != 'NULL'
	group by 
		dr_bcdisc 
)
select 
	count(*)
	, 'Всего держателей карт' as name
from 
	count_sales
union
select 
	count(*)
	, 'Из них совершили лишь одну покупку'
from
	count_sales
where 
	sales_cnt = 1
union
select 
	count(*)
	, 'Более трех недель назад'
from
	count_sales
where 
	sales_cnt = 1
	and days_from_last_purchase > 21
order by 
	1 desc
