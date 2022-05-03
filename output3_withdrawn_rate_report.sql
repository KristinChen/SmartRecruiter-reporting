-- 176
-- select count(distinct concat(candidateId, jobId, joinId)) from CleanedValidEvents where applicationStatus like '%WITHDRAWN%' and (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%');
-- select count(distinct concat(candidateId, jobId, joinId)) from outData where applicationStatus like '%WITHDRAWN%' and (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%');

With WithdrawnApplicants AS 
(
select *,
       ROW_NUMBER() over (partition by candidateId, jobId, joinId order by eventDate) step,
       case when applicationStatus = 'WITHDRAWN' then 1 else 0 end withdrawnFlag
from CleanedValidEvents where (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%') 
),

WithdrawnRateTable AS (
select d.jobCapability, d.funnel, d.applicationAggStatus, d.applicationAggSubStatus, count(distinct concat(candidateId, jobId, joinId)) as numApplicants from
(
select  distinct c.*, 
m.minWithdrawnFlag,
case when funnel like '%join%' or funnel like '%rejoin%' then 'NEW' 
    when funnel IS NULL then 'IN_REVIEW' 
    else applicationStatus
    end applicationAggStatus,

case when (jobCapability like '%engineer%' or jobCapability like '%business%') and (joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Recruiter Screen' 
    when (jobCapability like '%engineer%' or jobCapability like '%business%') and (joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
    when (jobCapability like '%engineer%' or jobCapability like '%business%') and (joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending'
    
    when (jobCapability like '%science%') and (joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
    when (jobCapability like '%science%') and (joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
    when (jobCapability like '%science%') and (joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 

    -- experienced
    when joblevel not like 'all-star' and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
    when joblevel not like 'all-star' and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
    when joblevel not like 'all-star' and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
    when joblevel not like 'all-star' and (applicationSubStatus like '%warm%' or applicationSubStatus like '%test%' or applicationSubStatus like '%interview%') then 'On-Site Interview' 
    else applicationSubStatus
    end applicationAggSubStatus 
FROM
(
    select candidateid, jobid, joinid, min(step) as minWithdrawnFlag from withdrawnApplicants where withdrawnFlag = 1 group by candidateId, jobId, joinid
    ) m --get the minimum withdrawn step
left join withdrawnApplicants c
on c.candidateId = m.candidateId and c.jobId = m.jobId and c.joinId = m.joinid and c.step = m.minWithdrawnFlag - 1 --prior step before withdrawn
) d
group by d.jobCapability, d.funnel, d.applicationAggStatus, d.applicationAggSubStatus  
)
-- select sum(numApplicants) from WithdrawnRateTable; --176

-- IF EXISTS(SELECT * FROM dbo.WithdrawnRateTable) DROP TABLE dbo.WithdrawnRateTable
-- SELECT * INTO dbo.WithdrawnRateTable FROM WithdrawnRateTable;
