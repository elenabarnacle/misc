-- DAU
select from_unixtime(players.hour-60*60*24, 'yyyy-MM-dd') as period,
	count(players.event_user) as dau
from main_day.seg_players_3426_pq players
where players.hour between unix_timestamp('2023-05-01')+60*60*24 and unix_timestamp('2023-05-31')+60*60*24
	and players.last_active/1000 between players.hour-60*60*24 and players.hour
group by from_unixtime(players.hour-60*60*24, 'yyyy-MM-dd')
order by from_unixtime(players.hour-60*60*24, 'yyyy-MM-dd')


-- MAU total
select count(players.event_user) as mau
from main_day.seg_players_3426_pq players
where players.hour = unix_timestamp('2023-05-31')+60*60*24
	and players.last_active/1000 between unix_timestamp('2023-05-01') and players.hour

	
-- MAU rolling (last 30 days)
select from_unixtime(players.hour-60*60*24, 'yyyy-MM-dd') as period,
	count(players.event_user) as mau
from main_day.seg_players_3426_pq players
where players.hour between unix_timestamp('2023-05-01')+60*60*24 and unix_timestamp('2023-05-31')+60*60*24
	and players.last_active/1000 between players.hour-60*60*24*30 and players.hour
group by from_unixtime(players.hour-60*60*24, 'yyyy-MM-dd')
order by from_unixtime(players.hour-60*60*24, 'yyyy-MM-dd')
	
	
-- ARPU total
with active_users as (
	select count(players.event_user) as mau
	from main_day.seg_players_3426_pq players
	where players.hour = unix_timestamp('2023-05-31')+60*60*24
		and players.last_active/1000 between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	)
	, revenue as (
	select sum(payments.offer_price) as revenue
	from main_day.valid_iap_3426_pq payments
	where payments.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	)
select round(revenue.revenue / active_users.mau, 2) as arpu
from revenue, active_users


-- ARPU daily
with dau as (
	select from_unixtime(players.hour-60*60*24, 'yyyy-MM-dd') as period,
		count(players.event_user) as dau
	from main_day.seg_players_3426_pq players
	where players.hour between unix_timestamp('2023-05-01')+60*60*24 and unix_timestamp('2023-05-31')+60*60*24
		and players.last_active/1000 between players.hour-60*60*24 and players.hour
	group by from_unixtime(players.hour-60*60*24, 'yyyy-MM-dd')
	)
	, revenue as (
	select from_unixtime(payments.day, 'yyyy-MM-dd') as period,
		sum(payments.offer_price) as revenue
	from main_day.valid_iap_3426_pq payments
	where payments.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	group by from_unixtime(payments.day, 'yyyy-MM-dd')
	)
select dau.period,
	round(revenue.revenue / dau.dau, 2) as arpu
from dau
left join revenue on dau.period = revenue.period
order by dau.period


-- ARPPU total
select round(sum(payments.offer_price) / count(distinct payments.event_user), 2) as arppu
from main_day.valid_iap_3426_pq payments
where payments.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')


-- ARPPU daily
select from_unixtime(payments.day, 'yyyy-MM-dd') as period,
	round(sum(payments.offer_price) / count(distinct payments.event_user), 2) as arppu
from main_day.valid_iap_3426_pq payments
where payments.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
group by from_unixtime(payments.day, 'yyyy-MM-dd')
order by from_unixtime(payments.day, 'yyyy-MM-dd')


-- скорость игроков
-- количество пройденных уровней за единицу времени (день) на игрока
with levels_passed as (
	select attempts.event_user,
		from_unixtime(cast(attempts.event_time/1000 as bigint), 'yyyy-MM-dd') as period,
		count(attempts.level) as levels_passed
	from main_day.attempts_3426_pq attempts
	where attempts.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
		and attempts.name in ('Level.Complete', 'Chains.Level.Complete')
	group by attempts.event_user,
		from_unixtime(cast(attempts.event_time/1000 as bigint), 'yyyy-MM-dd')
	)
select levels_passed.period,
	round(avg(levels_passed.levels_passed), 2) as avg_speed
from levels_passed
group by levels_passed.period
order by levels_passed.period


-- количество попыток за единицу времени (день) на игрока
with attempts as (
	select attempts.event_user,
		from_unixtime(cast(attempts.event_time/1000 as bigint), 'yyyy-MM-dd') as period,
		count(attempts.level) as attempts	
	from main_day.attempts_3426_pq attempts
	where attempts.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	group by attempts.event_user,
		from_unixtime(cast(attempts.event_time/1000 as bigint), 'yyyy-MM-dd')
	)
select attempts.period,
	round(avg(attempts.attempts), 2) as attempts_avg
from attempts
group by attempts.period
order by attempts.period


-- boosts given
select from_unixtime(cast(events.event_time/1000 as bigint), 'yyyy-MM-dd') as period,
	sum(
		case
			when events.currency = 'BoostIn.Unlimited' then 1
			else events.quantity
		end
		) as boosts_received
from main_day.currency_stream_3426_pq events
where events.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	and events.currency in ('BoostIn', 'BoostIn.Unlimited')
group by from_unixtime(cast(events.event_time/1000 as bigint), 'yyyy-MM-dd')
order by from_unixtime(cast(events.event_time/1000 as bigint), 'yyyy-MM-dd')

		
-- boosts spent
select from_unixtime(cast(events.event_time/1000 as bigint), 'yyyy-MM-dd') as period,
	sum(
		case
			when events.currency = 'BoostOut.Unlimited' then 1
			else events.quantity
		end
		) as boosts_used
from main_day.currency_stream_3426_pq events
where events.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
	and events.currency in ('BoostOut', 'BoostOut.Unlimited')
group by from_unixtime(cast(events.event_time/1000 as bigint), 'yyyy-MM-dd')
order by from_unixtime(cast(events.event_time/1000 as bigint), 'yyyy-MM-dd')


-- cash given
select from_unixtime(cast(cash_earned.event_time/1000 as bigint), 'yyyy-MM-dd') as period,
	sum(cash_earned.amount) as cash_earned
from main_day.cash_earned_3426_pq cash_earned
where cash_earned.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
group by from_unixtime(cast(cash_earned.event_time/1000 as bigint), 'yyyy-MM-dd')
order by from_unixtime(cast(cash_earned.event_time/1000 as bigint), 'yyyy-MM-dd')


-- cash spent
select from_unixtime(cast(cash_spent.event_time/1000 as bigint), 'yyyy-MM-dd') as period,
	sum(cash_spent.free_cash) as cash_spent
from main_day.cash_spent_3426_pq cash_spent
where cash_spent.day between unix_timestamp('2023-05-01') and unix_timestamp('2023-05-31')
group by from_unixtime(cast(cash_spent.event_time/1000 as bigint), 'yyyy-MM-dd')
order by from_unixtime(cast(cash_spent.event_time/1000 as bigint), 'yyyy-MM-dd')