declare @ICAO varchar(4) = 'LEMD'

declare @TREND table (Id int)
insert into @TREND values (1), (2)

select tvm.dateTime
	 , tvm.next_dateTime
	 , tvm.arr_volume_group
	 , tvm.dep_volume_group
	 , tvm.volume_group
	 , mt.metarKey
into staging.dbo.metar2019
from staging.dbo.trafficVolumeMadrid tvm
inner join dwh.dbo.dimMeteoMETARreport mt 
	on mt.initialValidInstant >= tvm.dateTime and mt.initialValidInstant < tvm.next_dateTime
	and mt.ICAO = @ICAO
	and mt.reportType != 'SPECI'

-- Miscellaneous
select mt.*
	 , mwth.visibility
	 , mwth.verVisibility
	 , mwth.minVisibility
	 , mwth.dirMinVisibility
	 , mwth.CAVOK
	 , mwth.temperature
	 , mwth.dewPoint
	 , mwth.qnh
	 , mwth.unitPressure
	 , wind.variable
	 , wind.direction
	 , wind.knots
	 , wind.gustyWind
	 , wind.maxKnots
	 , wind.oscillationFrom
	 , wind.oscillationTo
into staging.dbo.metar2019miscellaneous
from staging.dbo.metar2019 mt
inner join dwh.dbo.meteoMetarMiscellaneousWeather_Facts mwth
	on mwth.metarReportKey = mt.metarKey
	and mwth.meteoTrendKey in (select Id from @TREND)
left join dwh.dbo.dimMeteoWind wind
	on wind.meteoWindKey = mwth.meteoWindKey

-- Runway
select mt.*
	 , runway.runwayName
	 , runway.tendencyRVR
	 , runway.RVR
	 , runway.shear
	 , runway.coverage
	 , runway.dimCoverage
	 , runway.heightCoverage
	 , runway.frictionCoefficient
into staging.dbo.metar2019runway
from staging.dbo.metar2019 mt
left join dwh.dbo.meteoMetarRunwayInformation_Facts ri
	on ri.metarReportKey = mt.metarKey
	and ri.meteoTrendKey in (select Id from @TREND)
left join dwh.dbo.dimMeteoRunwayInformation runway
	on runway.meteoRunwayKey = ri.meteoRunwayKey

-- Clouds
select mt.*
	 , cloud.noSignificantClouds
	 , cloud.noCloudDetected
	 , cloud.amount
	 , cloud.height
	 , cloud.type
into staging.dbo.metar2019clouds
from staging.dbo.metar2019 mt
left join dwh.dbo.meteoMetarCloud_Facts clf
	on clf.metarReportKey = mt.metarkey
	and clf.meteoTrendKey in (select Id from @TREND)
left join dwh.dbo.dimMeteoCloud cloud
	on cloud.meteoCloudKey = clf.meteoCloudKey

-- Fenomenos
select mt.*
	 , wtphen.noPhenDetected
	 , wtphen.recent
	 , wtphen.intensity
	 , wtphen.characteristic
	 , wtphen.phenomenon1
	 , wtphen.phenomenon2
	 , wtphen.phenomenon3
into staging.dbo.metar2019phen
from staging.dbo.metar2019 mt
left join dwh.dbo.meteoMetarWeatherPhenomena_Facts wp
	on wp.metarReportKey = mt.metarKey
	and wp.meteoTrendKey in (select Id from @TREND)
left join dwh.dbo.dimMeteoWeatherPhenomena wtphen
	on wtphen.meteoWeatherPhenKey = wp.meteoWeatherPhenKey
