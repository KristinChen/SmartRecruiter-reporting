WITH AnalysisTable1 AS
(
SELECT *, MAX(a.step) over (partition by a.candidateId, a.jobId, a.joinId) maxStep, MIN(a.step) over (partition by a.candidateId, a.jobId, a.joinId) minStep FROM 
(
select *, 
      ROW_NUMBER() over (partition by candidateId, jobid, joinId order by eventDate) step, 
      concat(candidateId, '&', jobId, '&', joinId) as uniqueId
from CleanedValidEvents where jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering'
) a
),

OutData AS 
(
select distinct b.* from
(select distinct uniqueId from AnalysisTable1 where funnel like '%out%' or (applicationStatus like '%hired%' or applicationStatus like '%transferred%' or applicationStatus like '%rejected%' or applicationStatus like '%withdrawn%')) a
left join AnalysisTable1 b
on a.uniqueId = b.uniqueId
),

InFunnelData AS (
select distinct b.* from
(select distinct uniqueId from AnalysisTable1 where uniqueId not in (select uniqueId from OutData)) a
left join AnalysisTable1 b
on a.uniqueId = b.uniqueId),

ActiveInFunnelData AS (
select distinct b.* from
(select distinct uniqueId from InFunnelData where applicationStatus like '%new%' and step != maxStep and step = minStep 
) a
left join AnalysisTable1 b
on a.uniqueId = b.uniqueId)



-- IF EXISTS(SELECT * FROM  dbo.InFunnelData) DROP TABLE dbo.InFunnelData;
-- SELECT * INTO dbo.InFunnelData FROM InFunnelData; 

-- IF EXISTS(SELECT * FROM  dbo.ActiveInFunnelData) DROP TABLE dbo.ActiveInFunnelData;
-- SELECT * INTO dbo.ActiveInFunnelData FROM ActiveInFunnelData 

-- IF EXISTS(SELECT * FROM  dbo.outData) DROP TABLE dbo.outData;
-- SELECT * INTO dbo.outData FROM outData 
