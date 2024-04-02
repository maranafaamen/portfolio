-- Находим количество дней с момента последней покупки для каждого пользователя
with last_purchase as (
	select
		dr_bcdisc as bonus_card
		, make_date(2022,06,09) - max(dr_dat) as last_purchase_days
	from
		sales
	where dr_bcdisc != 'NULL'
	group by dr_bcdisc
)
, recency as (
	select
		bonus_card
		, case
			when last_purchase_days <= 10 then '1'
			when last_purchase_days <= 25 then '2'
			else '3'
		end as category_by_recency
	from
		last_purchase
)
-- Находим количество среднее количество покупок за 7 дней для каждого пользователя
, avg_purchases_per_7_days as (
	select
		dr_bcdisc as bonus_card
		, count(distinct dr_nchk) * 1.0 /
		case
			when ceil((max(dr_dat) - min(dr_dat))*1.0 / 7) = 0 then 1
			else ceil((max(dr_dat) - min(dr_dat))*1.0 / 7)
		end as avg_purchases_cnt
	from
		sales
	where dr_bcdisc != 'NULL'
	group by dr_bcdisc
)
, frequency as (
	select
		bonus_card
		, case
			when avg_purchases_cnt > median  then '1'
			when avg_purchases_cnt = median  then '2'
			else '3'
		end as category_by_frequency
	from
		avg_purchases_per_7_days
		, (
		select
			percentile_cont (0.5) within group (order by avg_purchases_cnt) as median
		from avg_purchases_per_7_days
	) as median_frequency
)
-- Находим количество потраченных средств для каждого пользователя
, purchases_summ as (
	select
		dr_bcdisc as bonus_card
		, round(sum(dr_kol * dr_croz)) as summ
	from
		sales
	where dr_bcdisc != 'NULL'
	group by dr_bcdisc
)
, monetary as (
	select
		bonus_card
		, case
			when summ >= 1800 then '1'
			when summ >= 800 then '2'
			else '3'
		end as category_by_monetary
	from
		purchases_summ
)
-- Формируем RFM-группы
, rfm_total as (
	select
		r.bonus_card
		, category_by_recency || category_by_frequency || category_by_monetary as rfm
	from
		recency r
	join frequency f
		on f.bonus_card = r.bonus_card
	join monetary m
		on m.bonus_card = r.bonus_card
) 
, rfm_groups as (
	select
		rfm
		, count(*)
			, case
			when rfm like '1%' then 'Недавние'
			when rfm like '2%' then 'Спящие'
			when rfm like '3%' then 'Давние'
		end
		||
		case
			when rfm like '_1_' then ' частые'
			when rfm like '_2_' then ' редкие'
			when rfm like '_3_' then ' разовые'
		end
		||
		case
			when rfm like '__1' then ' с большим чеком'
			when rfm like '__2' then ' со средним чеком'
			when rfm like '__3' then ' экономные'
		end as category
	from
		rfm_total
	group by 
		rfm
	order by 
		2 desc
)
-- Формируем группы для SMS-рассылки
select
	coalesce(
		case
			when rfm like '22_' then 'Середнячки'
			when rfm in ('112', '121', '122', '131', '211', '212') then 'Перспективные клиенты'
			when rfm like '111' then 'VIP-клиенты'
			when rfm like '13_' then 'Новички'
			when rfm like '31_' then 'Бывшие лояльные'
			when rfm like '32_' then 'Под угрозой оттока'
			when rfm like '33_' then 'Потерянные клиенты'
			when rfm in ('123', '113') then 'Лояльные экономные'
		end
	, 'Остальные') as category
	, count(*)
from
	rfm_total
group by 
	1