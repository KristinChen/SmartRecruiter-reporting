WITH AnalysisTable1 AS
(select *, 
      YEAR(eventDate) AS year, 
      MONTH(eventDate) AS month, 
      DATEPART(WEEK, eventDate) as week,
      DATEPART(WEEKDAY, eventDate) as weekday,
      concat(candidateId, '&', jobId, '&', joinId) as uniqueId
from CleanedValidEvents),

JoinData AS (
select distinct candidateId, jobId, joinId, uniqueId, jobCapability, jobLevel, jobLocation, year, week from AnalysisTable1 
where (funnel = 'join' or funnel = 'rejoin')
),

inFunnelData AS (
select distinct * from
(
select candidateId, jobId, joinId, uniqueId, jobCapability, jobLevel, jobLocation, startYear year, startWeek week, sum(distinct outFlag) totalOut from 
(
select e.candidateId, e.jobId, e.joinId, e.uniqueId, e.jobCapability, e.jobLevel, e.jobLocation, 
       e.year currentYear, 
       a2.year startYear,
       a2.week startWeek,
       e.week currentWeek, 
       e.funnel,
       case when e.funnel = 'OUT' then 1 else 0 end outFlag
       from JoinData a2
left join AnalysisTable1 e
ON a2.candidateId = e.candidateId 
and a2.jobId = e.jobId 
and a2.joinId = e.joinId
and a2.uniqueId = e.uniqueId 
and a2.jobCapability = e.jobCapability 
and a2.jobLevel = e.jobLevel 
and a2.jobLocation = e.jobLocation
) allRecords
group by candidateId, jobId, joinId, uniqueId, jobCapability, jobLevel, jobLocation, startYear, startWeek
) o
where totalOut = 0),

outData AS (
select distinct * from
(
select candidateId, jobId, joinId, uniqueId, jobCapability, jobLevel, jobLocation, startYear year, startWeek week, sum(distinct outFlag) totalOut from 
(
select e.candidateId, e.jobId, e.joinId, e.uniqueId, e.jobCapability, e.jobLevel, e.jobLocation, 
       e.year currentYear, 
       a2.year startYear,
       a2.week startWeek,
       e.week currentWeek, 
       e.funnel,
       case when e.funnel = 'OUT' then 1 else 0 end outFlag
       from JoinData a2
left join AnalysisTable1 e
ON a2.candidateId = e.candidateId 
and a2.jobId = e.jobId 
and a2.joinId = e.joinId
and a2.uniqueId = e.uniqueId 
and a2.jobCapability = e.jobCapability 
and a2.jobLevel = e.jobLevel 
and a2.jobLocation = e.jobLocation
) allRecords
group by candidateId, jobId, joinId, uniqueId, jobCapability, jobLevel, jobLocation, startYear, startWeek
) o
where totalOut > 0), 

ActiveInFunnelData AS (
SELECT distinct uniqueId, candidateId, jobId, joinId, jobCapability, jobLevel, jobLocation, year, week FROM
(
select e.uniqueId, e.joinId, e.candidateId, e.jobId, e.jobCapability, e.jobLevel, e.jobLocation, a3.year, a3.week, sum(case when funnel is NULL then 1 else 0 end) inFlag from
(select * from inFunnelData) a3
left join AnalysisTable1 e
ON a3.candidateId = e.candidateId 
and a3.jobId = e.jobId 
and a3.uniqueId = e.uniqueId 
and a3.joinId = e.joinId
and a3.jobCapability = e.jobCapability 
and a3.jobLevel = e.jobLevel 
and a3.jobLocation = e.jobLocation
group by e.uniqueId, e.joinId, e.candidateId, e.jobId, e.jobCapability, e.jobLevel, e.jobLocation, a3.year, a3.week --startWeek
) a4 
where a4.inFlag > 0),

InactiveInFunnelData AS (
SELECT distinct uniqueId, joinId, candidateId, jobId, jobCapability, jobLevel, jobLocation, year, week FROM
(
select e.uniqueId, e.joinId, e.candidateId, e.jobId, e.jobCapability, e.jobLevel, e.jobLocation, a3.year, a3.week, sum(case when funnel is NULL then 1 else 0 end) inFlag from
(select * from inFunnelData) a3
left join AnalysisTable1 e
ON a3.candidateId = e.candidateId 
and a3.jobId = e.jobId 
and a3.uniqueId = e.uniqueId 
and a3.joinId = e.joinId
and a3.jobCapability = e.jobCapability 
and a3.jobLevel = e.jobLevel 
and a3.jobLocation = e.jobLocation
group by e.uniqueId, e.joinId, e.candidateId, e.jobId, e.jobCapability, e.jobLevel, e.jobLocation, a3.year, a3.week --startWeek
) a4 
where a4.inFlag = 0)

-- IF EXISTS(SELECT * FROM  dbo.JoinApplicants) DROP TABLE dbo.JoinApplicants;
-- SELECT * INTO dbo.JoinApplicants FROM JoinData where jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering'
-- order by jobCapability, jobLevel, jobLocation, year, week

-- IF EXISTS(SELECT * FROM  dbo.INFunnelApplicants) DROP TABLE dbo.INFunnelApplicants;
-- SELECT * INTO dbo.INFunnelApplicants FROM inFunnelData where jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering'
-- order by jobCapability, jobLevel, jobLocation, year, week

-- IF EXISTS(SELECT * FROM  dbo.ActiveInFunnelApplicants) DROP TABLE dbo.ActiveInFunnelApplicants;
-- SELECT * INTO dbo.ActiveInFunnelApplicants FROM activeInFunnelData where jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering'
-- order by jobCapability, jobLevel, jobLocation, year, week

-- IF EXISTS(SELECT * FROM  dbo.InactiveInFunnelApplicants) DROP TABLE dbo.InactiveInFunnelApplicants;
-- SELECT * INTO dbo.InactiveInFunnelApplicants FROM InactiveInFunnelData where jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering'
-- order by jobCapability, jobLevel, jobLocation, year, week

-- IF EXISTS(SELECT * FROM  dbo.OutApplicants) DROP TABLE dbo.OutApplicants
-- SELECT * INTO dbo.OutApplicants FROM outData where jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering'
-- order by jobCapability, jobLevel, jobLocation, year, week

-- example 1: join (161) = (out) 106 + (infunnel) 55; (infunnel) 55 = (active) 46 + (inactive) 9
-- select * from JoinApplicants where jobCapability like '%bus%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14';
-- select * from INFunnelApplicants where jobCapability like '%bus%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14'
-- select * from outApplicants where jobCapability like '%bus%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14'
-- select * from ActiveInFunnelApplicants where jobCapability like '%bus%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14'
-- select * from InactiveInFunnelApplicants where jobCapability like '%bus%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14'

-- example 2: join (14) = (out) 8 + (infunnel) 6; (infunnel) 6 = (active) 0 + (inactive) 6
-- select * from JoinApplicants where jobCapability like '%science%' AND joblevel like '%all%' and jobLocation like '%md' and week = '12';
-- select * from INFunnelApplicants where jobCapability like '%science%' AND joblevel like '%all%' and jobLocation like '%md' and week = '12';
-- select * from outApplicants where jobCapability like '%science%' AND joblevel like '%all%' and jobLocation like '%md' and week = '12';
-- select * from ActiveInFunnelApplicants where jobCapability like '%science%' AND joblevel like '%all%' and jobLocation like '%md' and week = '12';
-- select * from InactiveInFunnelApplicants where jobCapability like '%science%' AND joblevel like '%all%' and jobLocation like '%md' and week = '12';