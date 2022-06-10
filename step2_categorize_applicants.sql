WITH AnalysisTable1 AS
(
SELECT *, 
      MAX(a.step) over (partition by a.candidateId, a.jobId, a.joinId) maxStep, 
      MIN(a.step) over (partition by a.candidateId, a.jobId, a.joinId) minStep FROM 
(
select *, 
      ROW_NUMBER() over (partition by candidateId, jobid, joinId order by eventDate) step
from CleanedValidEvents 
) a
),

OutData AS 
(
select distinct b.* from
(select distinct uniqueId from AnalysisTable1 where analyticalStatus like '%withdrawn%' or analyticalStatus like '%transferred%' or analyticalStatus like '%hired%' or analyticalStatus like '%rejected%' or analyticalStatus like '%rejected%') a
left join AnalysisTable1 b
on a.uniqueId = b.uniqueId
),



InFunnelData AS (
select distinct b.* from
(select distinct uniqueId from AnalysisTable1 where uniqueId not in (select uniqueId from OutData)) a
left join AnalysisTable1 b
on a.uniqueId = b.uniqueId), 

-- select count(distinct uniqueId) from outData; --4484
-- select analyticalStatus, count(distinct uniqueId) from InFunnelData where step = maxStep group by analyticalStatus; 
-- select count(distinct uniqueId) from InFunnelData; --2239
-- select count(distinct uniqueId) from AnalysisTable1; --6723

ActiveInFunnelApplicants AS (
select distinct uniqueId from InFunnelData where uniqueId not in 
(
select distinct uniqueId from InFunnelData where step = maxStep and (analyticalStatus like '%join%' or analyticalStatus like '%rejoin%') 
) 
),


ActiveInFunnelData AS (
select distinct * from AnalysisTable1 where uniqueId in (select uniqueId from ActiveInFunnelApplicants) 
)

-- select * from ActiveInFunnelApplicants; --440
-- select analyticalStatus, count(distinct uniqueId) from ActiveInFunnelData where step = maxStep group by analyticalStatus; 

-- Hiring Team Screen	118
-- Offer Pending	7
-- On-Site Interview	27
-- Recruiter Screen	100
-- Skills Test	188

-- IF EXISTS(SELECT * FROM  dbo.InFunnelData) DROP TABLE dbo.InFunnelData;
-- SELECT * INTO dbo.InFunnelData FROM InFunnelData; 

-- IF EXISTS(SELECT * FROM  dbo.ActiveInFunnelData) DROP TABLE dbo.ActiveInFunnelData;
-- SELECT * INTO dbo.ActiveInFunnelData FROM ActiveInFunnelData 

-- IF EXISTS(SELECT * FROM  dbo.outData) DROP TABLE dbo.outData;
-- SELECT * INTO dbo.OutData FROM OutData 

