declare @START_DATE int = 20190101
declare @END_DATE int = 20200101

declare @ICAO varchar(4) = 'LEMD'
declare @APPROACH varchar(7) = 'LANDING'
declare @TRESHOLD varchar(6) = 'SEPEND'
declare @DIAGONAL varchar(7) = 'SEPDIAG'
declare @MILE_4 varchar(4) = 'FSLP'

; with threshold_data as(
	select sep.referenceTimeDt as date
		 , sep.referenceTimeDateKey as dateKey
		 , sep.dstSeparationNM as threshold_separation
		 , sep.flightKey
		 , sep.referenceFlightKey
	from flowsarrivalsseparation_standalone_facts sep
	where sep.referenceApproachType = @APPROACH
	  and sep.referenceType = @TRESHOLD
	  and sep.referenceTimeDateKey >= @START_DATE and sep.referenceTimeDateKey < @END_DATE
--	  and sep.flightKey = 4849813
)
-- Vuelo a estudiar y vuelo previo
, flights as (
	select sep.*
		 , actual_fl.flightKey as fl_key
		 , actual_fl.wake as flight_wake
		 , prev_fl.flightKey as prev_fl_key
		 , prev_fl.wake as prev_flight_wake
	from threshold_data sep
	inner join dwh.dbo.dimFlowsFlights actual_fl
		on actual_fl.flightKey = sep.flightKey
		and actual_fl.ades = @ICAO
	inner join dwh.dbo.dimFlowsFlights prev_fl
		on prev_fl.flightKey = sep.referenceFlightKey
)
-- Separacion diagonal (solo en Madrid)
, diag_data as (
	select fl.*
		 , diag.dstSeparationNM as diag_distance
	from flights fl
	left join dwh.dbo.flowsarrivalsseparation_standalone_facts diag 
		on diag.flightKey = fl.fl_key
		and diag.referenceApproachType = @APPROACH 
		and diag.referenceType = @DIAGONAL
		and diag.referenceTimeDateKey >= @START_DATE and diag.referenceTimeDateKey < @END_DATE
)
-- Datos controlados en milla 4
, mile_4_data as (
	select dd.*
		 , track.vel_mod as m4_speed
		 , track.modo_c as m4_altitude
	from diag_data dd
	inner join dwh.dbo.flowsarrivalsseparation_standalone_facts m4 
		on m4.flightKey = dd.fl_key
		and m4.referenceApproachType = @APPROACH 
		and m4.referenceType = @MILE_4
		and m4.referenceTimeDateKey >= @START_DATE and m4.referenceTimeDateKey < @END_DATE
	inner join dwh.dbo.flowsTracksFacts track 
		on track.flightKey = m4.flightKey
	inner join dwh.dbo.CalendarDate dt
		on dt.dateKey = track.date 
	inner join dwh.dbo.CalendarTime tm
		on tm.timeKey = track.time
		and concat(dt.date, ' ', convert(char(8), tm.time)) >= cast(convert(char(19), m4.referenceTimeDt) as datetime) 
		and concat(dt.date, ' ', convert(char(8), tm.time)) < dateadd(s, 5, cast(convert(char(19), m4.referenceTimeDt) as datetime))
)
-- Aeropuerto y pista destino
, arrival as (
	select m4.*
		 , la.ICAO
		 , la.Runway
	from mile_4_data m4
	inner join dwh.dbo.flowsLocalArrivals_Fact arr 
		on arr.flightKey = m4.fl_key
	inner join dwh.dbo.dimLocalAerodromes la 
		on la.aerodromesKey = arr.runwayKey
)
-- Intervalo y grupo de trafico asociado
, thresholdIntervals as(
	select arr.date
		 , v.dateTime as dateTimeStart
		 , v.next_dateTime as dateTimeEnd
		 , v.arr_volume_group
		 , arr.ICAO
		 , arr.Runway as runway 
		 , arr.fl_key as flight_key
		 , arr.threshold_separation
		 , arr.flight_wake
		 , arr.prev_flight_wake
		 , arr.diag_distance
		 , arr.m4_speed
		 , arr.m4_altitude 
	from arrival arr
	left join staging.dbo.trafficVolumeMadrid v
		on v.dateTime <= arr.date and v.next_dateTime > arr.date
)

select *
into staging.dbo.thresholdSeparationMeteoMadrid
from thresholdIntervals
