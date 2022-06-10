------ reporting 1 -------------------------
WITH AnalysisTable1 AS
(
SELECT *, 
      MAX(a.step) over (partition by a.candidateId, a.jobId, a.joinId) maxStep, 
      MIN(a.step) over (partition by a.candidateId, a.jobId, a.joinId) minStep FROM 
(
select *, 
      ROW_NUMBER() over (partition by candidateId, jobid, joinId order by eventDate) step,
       case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
       when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag
from CleanedValidEvents 
) a
)

select a.jobCapability, a.experiencedFlag, a.jobLocation, b.applicationSource, count(distinct a.uniqueId) numApplicants from AnalysisTable1 a 
left join ApplicationEvents_Merged b 
on a.candidateId = b.candidateId and a.jobid = b.jobId
group by a.jobCapability, a.experiencedFlag, a.jobLocation, b.applicationSource
having experiencedFlag is not null order by a.jobCapability, a.experiencedFlag, a.jobLocation, numApplicants desc

------ reporting 2 -------------------------

WITH AnalysisTable1 AS
(
SELECT *, 
      MAX(a.step) over (partition by a.candidateId, a.jobId, a.joinId) maxStep, 
      MIN(a.step) over (partition by a.candidateId, a.jobId, a.joinId) minStep FROM 
(
select *, 
      ROW_NUMBER() over (partition by candidateId, jobid, joinId order by eventDate) step,
       case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
       when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag
from CleanedValidEvents 
) a
)

select a.jobCapability, a.experiencedFlag, b.applicationSource, count(distinct a.uniqueId) numApplicants from AnalysisTable1 a 
left join ApplicationEvents_Merged b 
on a.candidateId = b.candidateId and a.jobid = b.jobId
group by a.jobCapability, a.experiencedFlag, a.jobLocation, b.applicationSource
having experiencedFlag is not null order by a.jobCapability, a.experiencedFlag, numApplicants desc

------ reporting 3 -------------------------

WITH AnalysisTable1 AS
(
SELECT *, 
      MAX(a.step) over (partition by a.candidateId, a.jobId, a.joinId) maxStep, 
      MIN(a.step) over (partition by a.candidateId, a.jobId, a.joinId) minStep FROM 
(
select *, 
      ROW_NUMBER() over (partition by candidateId, jobid, joinId order by eventDate) step,
       case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
       when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag
from CleanedValidEvents 
) a
)

select a.jobCapability, a.jobLocation, b.applicationSource, count(distinct a.uniqueId) numApplicants from AnalysisTable1 a 
left join ApplicationEvents_Merged b 
on a.candidateId = b.candidateId and a.jobid = b.jobId
group by a.jobCapability, a.experiencedFlag, a.jobLocation, b.applicationSource
having experiencedFlag is not null order by a.jobCapability, a.jobLocation, numApplicants desc
