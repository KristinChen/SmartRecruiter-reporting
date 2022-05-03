WITH AnalysisTable1 AS
(
SELECT *, MAX(a.step) over (partition by a.candidateId, a.jobId, a.joinId) maxStep, MIN(a.step) over (partition by a.candidateId, a.jobId, a.joinId) minStep FROM 
(
select *, 
      ROW_NUMBER() over (partition by candidateId, jobid, joinId order by eventDate) step, 
      YEAR(eventDate) AS year, 
      MONTH(eventDate) AS month, 
      DATEPART(WEEK, eventDate) as week,
      DATEPART(WEEKDAY, eventDate) as weekday,
      concat(candidateId, '&', jobId, '&', joinId) as uniqueId
from CleanedValidEvents where jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering'
) a
),

-- select count(distinct uniqueId) from AnalysisTable1 --7215

outData AS 
(
select b.*, a.year outYear, a.week outWeek from
(select uniqueId, year, week from AnalysisTable1 where funnel like '%out%' or (applicationStatus like '%hired%' or applicationStatus like '%transferred%' or applicationStatus like '%rejected%' or applicationStatus like '%withdrawn%')) a
left join AnalysisTable1 b
on a.uniqueId = b.uniqueId
),


InFunnelData AS (
select b.* from
(select distinct uniqueId from AnalysisTable1 where uniqueId not in (select uniqueId from outData)) a
left join AnalysisTable1 b
on a.uniqueId = b.uniqueId), 

ActiveInFunnelData AS (
select b.* from
(select uniqueId from InFunnelData where funnel is NULL) a
left join AnalysisTable1 b
on a.uniqueId = b.uniqueId)

-- IF EXISTS(SELECT * FROM  dbo.InFunnelData) DROP TABLE dbo.InFunnelData;
-- SELECT * INTO dbo.InFunnelData FROM InFunnelData; 

-- IF EXISTS(SELECT * FROM  dbo.ActiveInFunnelData) DROP TABLE dbo.ActiveInFunnelData;
-- SELECT * INTO dbo.ActiveInFunnelData FROM ActiveInFunnelData 

-- IF EXISTS(SELECT * FROM  dbo.outData) DROP TABLE dbo.outData;
-- SELECT * INTO dbo.outData FROM outData 


-- example 1: join (161) = (out) 106 + (infunnel) 55; (infunnel) 55 = (active) 46 + (inactive) 9
-- select * from JoinApplicants where jobCapability like '%bus%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14';
-- select * from InFunnelApplicants where jobCapability like '%bus%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14'
-- select * from outApplicants where jobCapability like '%bus%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14'
-- select * from ActiveInFunnelApplicants where jobCapability like '%bus%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14'
-- select * from InactiveInFunnelApplicants where jobCapability like '%bus%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14'

-- example 2: join (14) = (out) 8 + (infunnel) 6; (infunnel) 6 = (active) 0 + (inactive) 6
-- select * from JoinApplicants where jobCapability like '%science%' AND joblevel like '%all%' and jobLocation like '%md' and week = '12';
-- select * from InFunnelApplicants where jobCapability like '%science%' AND joblevel like '%all%' and jobLocation like '%md' and week = '12';
-- select * from outApplicants where jobCapability like '%science%' AND joblevel like '%all%' and jobLocation like '%md' and week = '12';
-- select * from ActiveInFunnelApplicants where jobCapability like '%science%' AND joblevel like '%all%' and jobLocation like '%md' and week = '12';
-- select * from InactiveInFunnelApplicants where jobCapability like '%science%' AND joblevel like '%all%' and jobLocation like '%md' and week = '12';