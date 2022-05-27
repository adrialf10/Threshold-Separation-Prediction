-- Aereopuerto e intervalo de fechas
declare @ICAO varchar(4) = 'LEMD'
declare @FROM int = 20190101
declare @TO int = 20200101

-- Limites del los grupos (percentiles 25 y 75)
declare @ARR_BAJA int = 5
declare @ARR_ALTA int = 18
declare @DEP_BAJA int = 4
declare @DEP_ALTA int = 18
declare @TOT_BAJA int = 11
declare @TOT_ALTA int = 36

-- Constantes internas
declare @END_TIME time = '00:00:00.0000000'
declare @END_TIMEKEY int = 9240000


;with timeRef as(
	select t.timeKey
		 , lead(t.timeKey, 1, @END_TIMEKEY) over (order by t.timeKey) as next_timeKey 
		 , t.time
		 , lead(t.time, 1, @END_TIME) over (order by t.time) as next_time
	from dwh.dbo.CalendarTime t
	where TimeInSecond % 1800 = 0
)
, arrivals as(
	select d.date
		 , case when t.next_timeKey = @END_TIMEKEY  then dateadd(day, 1, d.date) else d.date end as next_date
		 , t.time
		 , t.next_time
		 , a.flightKey
	from timeRef t
	inner join dwh.dbo.CalendarDate d 
		on d.dateKey >= @FROM and d.dateKey< @TO
	left join dwh.dbo.flowsLocalArrivals_Fact a
		on a.ALDTTimeKey >= t.timeKey and a.ALDTTimeKey < t.next_timeKey
		and a.ALDTDateKey = d.dateKey 
	    and a.ades = @ICAO 
)
, arr_vol as(
	select date
		 , next_date
		 , time
		 , next_time
		 , count(flightKey) as volume
	from arrivals
	group by date, time, next_date, next_time
)
, departures as(
	select d.date
		 , case when t.next_timeKey = @END_TIMEKEY  then dateadd(day, 1, d.date) else d.date end as next_date
		 , t.time
		 , t.next_time
		 , dep.flightKey
	from timeRef t
	inner join dwh.dbo.CalendarDate d 
		on d.dateKey >= @FROM and d.dateKey< @TO
	left join dwh.dbo.flowsLocalDepartures_Fact dep
		on dep.ATOTTimeKey>= t.timeKey and dep.ATOTTimeKey < t.next_timeKey
		and dep.ATOTDateKey = d.dateKey 
		and dep.adep = @ICAO
)
, dep_vol as(
	select date
		 , next_date
		 , time
		 , next_time
		 , count(flightKey) as volume
	from departures
	group by date, time, next_date, next_time
)
, vol as(
	select arr.date as date
		 , arr.next_date
		 , arr.time
		 , arr.next_time
		 , arr.volume as arrivals_volume
		 , dep.volume as departures_volume
		 , (arr.volume + dep.volume) as total_volume
	from arr_vol arr
	inner join dep_vol dep
	on arr.date = dep.date
		and arr.time = dep.time
		and arr.next_time = dep.next_time
)
, result as(
	select cast(vol.date as datetime) + cast(vol.time as datetime) as dateTime
		 , cast(vol.next_date as datetime) + cast(vol.next_time as datetime) as next_dateTime
		 , vol.arrivals_volume
		 , (case when arrivals_volume < @ARR_BAJA then 'BAJA' when arrivals_volume >= @ARR_BAJA and arrivals_volume < @ARR_ALTA then 'MEDIA' when arrivals_volume >= @ARR_ALTA then 'ALTA' end) as arr_volume_group
		 , vol.departures_volume
		 , (case when departures_volume < @DEP_BAJA then 'BAJA' when departures_volume >= @DEP_BAJA and departures_volume < @DEP_ALTA then 'MEDIA' when departures_volume >= @DEP_ALTA then 'ALTA' end) as dep_volume_group
		 , vol.total_volume
		 , (case when total_volume < @TOT_BAJA then 'BAJA' when total_volume >= @TOT_BAJA and total_volume < @TOT_ALTA then 'MEDIA' when total_volume >= @TOT_ALTA then 'ALTA' end) as volume_group
	from vol vol
)

select * 
into staging.dbo.trafficVolumeMadrid
from result
