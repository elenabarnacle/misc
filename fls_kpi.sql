ы--with players_seg as (
--	select sg.*
--	from agg.players_fls_gp as sg
--	join (
--	    select event_user,
--	    	max(day) as day
--	    from agg.players_fls_gp
--	    where day <= unix_timestamp('2023-05-31')
--	    group by event_user
--	) as idx
--	on sg.event_user = idx.event_user and sg.day = idx.day
--	)


-- DAU
select (timestamp 'epoch' + players.day * interval '1 second')::date as period,
	count(players.event_user) as dau
from agg.players_fls_gp players
where players.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
group by (timestamp 'epoch' + players.day * interval '1 second')::date
order by period


-- MAU total
select count(distinct players.event_user) as mau
from agg.players_fls_gp players
where players.last_active/1000 between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')


-- MAU rolling (last 30 days)

	
-- ARPU total
with active_users as (
	select count(distinct players.event_user) as mau
	from agg.players_fls_gp players
	where players.last_active/1000 between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	)
	, revenue as (
	select sum(payments.offer_price) as revenue
	from agg.valid_iap_fls_gp payments
	where payments.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	)
select round((revenue.revenue / active_users.mau)::double precision), 2) as arpu
from revenue, active_users


-- ARPU daily
with dau as (
	select (timestamp 'epoch' + players.day * interval '1 second')::date as period,
		count(players.event_user) as dau
	from agg.players_fls_gp players
	where players.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	group by (timestamp 'epoch' + players.day * interval '1 second')::date
	order by period
	)
	, revenue as (
	select (timestamp 'epoch' + payments.day * interval '1 second')::date as period,
		sum(payments.offer_price) as revenue
	from agg.valid_iap_fls_gp payments
	where payments.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	group by (timestamp 'epoch' + payments.day * interval '1 second')::date
	)
select dau.period,
	round(revenue.revenue / dau.dau, 2) as arpu
from dau
left join revenue on dau.period = revenue.period
order by dau.period


-- ARPPU total
select round((sum(payments.offer_price) / count(distinct payments.event_user))::double precision, 2) as arppu
from agg.valid_iap_fls_gp payments
where payments.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')


-- ARPPU daily
select (timestamp 'epoch' + payments.day * interval '1 second')::date as period,
	round((sum(payments.offer_price) / count(distinct payments.event_user))::double precision, 2) as arppu
from agg.valid_iap_fls_gp payments
where payments.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
group by (timestamp 'epoch' + payments.day * interval '1 second')::date
order by period


-- скорость игроков
-- количество пройденных уровней за единицу времени (день) на игрока
with levels_passed as (
	select attempts.event_user,
		(timestamp 'epoch' + attempts.event_time/1000 * interval '1 second')::date as period,
		count(attempts.level) as levels_passed
	from agg.attempts_fls_gp attempts
	where attempts.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
		and attempts.name = 'Level.LevelComplete'
	group by attempts.event_user,
		(timestamp 'epoch' + attempts.event_time/1000 * interval '1 second')::date
	)
select levels_passed.period,
	round(avg(levels_passed.levels_passed::double precision), 2) as avg_speed
from levels_passed
group by levels_passed.period
order by levels_passed.period


-- количество попыток за единицу времени (день) на игрока
with attempts as (
	select attempts.event_user,
		(timestamp 'epoch' + attempts.event_time/1000 * interval '1 second')::date as period,
		count(attempts.level) as attempts
	from agg.attempts_fls_gp attempts
	where attempts.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	group by attempts.event_user,
		(timestamp 'epoch' + attempts.event_time/1000 * interval '1 second')::date
	)
select attempts.period,
	round(avg(attempts.attempts::double precision),2) as attempts_avg
from attempts
group by attempts.period
order by attempts.period


-- boosts given
select (timestamp 'epoch' + events.event_time/1000 * interval '1 second')::date as period,
	sum(
		case
			when lower(events.item) like '%unlimited%' then 1
			else events.quantity
		end
		) as boosts_received
from agg.currency_stream_fls_gp events
where events.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	and events.currency = 'BoostIn'
group by (timestamp 'epoch' + events.event_time/1000 * interval '1 second')::date
order by period
	

-- boosts spent
select (timestamp 'epoch' + events.event_time/1000 * interval '1 second')::date as period,
	sum(
		case
			when lower(events.item) like '%unlimited%' then 1
			else events.quantity
		end
		) as boosts_used
from agg.currency_stream_fls_gp events
where events.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	and events.currency = 'BoostOut'
group by (timestamp 'epoch' + events.event_time/1000 * interval '1 second')::date
order by period


-- coins given (Impala)
select from_unixtime(cast(events.event_time/1000 as bigint), 'yyyy-MM-dd') as period,
	sum(cast(regexp_extract(events.parameters, '"cost":([0-9]*)', 1) as integer)) as coins_earned
from main_day.all_events_fls_gp events
where events.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	and events.event_type = 'purchase'
	and lower(events.parameters) like '%currency_given%'
group by from_unixtime(cast(events.event_time/1000 as bigint), 'yyyy-MM-dd')
order by from_unixtime(cast(events.event_time/1000 as bigint), 'yyyy-MM-dd')
	

-- coins spent (Impala)
select from_unixtime(cast(events.event_time/1000 as bigint), 'yyyy-MM-dd') as period,
	sum(cast(regexp_extract(events.parameters, '"cost":([0-9]*)', 1) as integer)) as coins_spent
from main_day.all_events_fls_gp events
where events.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	and events.event_type = 'purchase'
	and regexp_extract(events.parameters, '"currency":"([^/"]*?)"', 1) in ('Coins', 'RealCoins')
group by from_unixtime(cast(events.event_time/1000 as bigint), 'yyyy-MM-dd')
order by from_unixtime(cast(events.event_time/1000 as bigint), 'yyyy-MM-dd')

