-- 169
-- select count(distinct concat(candidateId, jobId, joinId)) from CleanedValidEvents where applicationStatus like '%WITHDRAWN%' and (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%');

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

-- IF EXISTS(SELECT * FROM dbo.WithdrawnRateTable) DROP TABLE dbo.WithdrawnRateTable
-- SELECT * INTO dbo.WithdrawnRateTable FROM WithdrawnRateTable;

----------------------------------------REJECTION----------------------------------------------
-- 3954
-- select count(distinct concat(candidateId, jobId, joinId)) from CleanedValidEvents where applicationStatus like '%REJECTED%' and (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%');

With rejectedApplicants AS (
select *,
       ROW_NUMBER() over (partition by candidateId, jobId, joinId order by eventDate) step,
       case when applicationStatus = 'REJECTED' then 1 else 0 end rejectionFlag
from CleanedValidEvents where (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%') 
),
RejectedAtStepOneApplicants AS (
    select b.* from
(
    select candidateid, jobid, joinid, min(step) as minRejectionFlag from rejectedApplicants where rejectionFlag = 1 group by candidateId, jobId, joinid
) b
where minRejectionFlag = 1
), 
RejectedAtStepMoreThanOneApplicants AS (
    select b.* from
(
    select candidateid, jobid, joinid, min(step) as minRejectionFlag from rejectedApplicants where rejectionFlag = 1 group by candidateId, jobId, joinid
) b
where minRejectionFlag > 1
), 
RejectionRateTable1 AS (
select d.jobCapability, d.funnel, d.applicationAggStatus, d.applicationAggSubStatus, count(distinct concat(candidateId, jobId, joinId)) as numApplicants from
(
select  distinct c.*, 
    bb.minRejectionFlag,
    
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
RejectedAtStepOneApplicants bb
left join rejectedApplicants c
on c.candidateId = bb.candidateId and c.jobId = bb.jobId and c.joinId = bb.joinId and c.step = bb.minRejectionFlag
) d
group by d.jobCapability, d.funnel, d.applicationAggStatus, d.applicationAggSubStatus  
),

RejectionRateTable2 AS (
select d.jobCapability, d.funnel, d.applicationAggStatus, d.applicationAggSubStatus, count(distinct concat(candidateId, jobId, joinId)) as numApplicants from
(
select  distinct c.*, 
    bb.minRejectionFlag,
    
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
RejectedAtStepMoreThanOneApplicants bb
left join rejectedApplicants c
on c.candidateId = bb.candidateId and c.jobId = bb.jobId and c.joinId = bb.joinId and c.step = bb.minRejectionFlag - 1
) d
group by d.jobCapability, d.funnel, d.applicationAggStatus, d.applicationAggSubStatus  
)  


-- IF EXISTS(SELECT * FROM dbo.RejectionRateTable) DROP TABLE dbo.RejectionRateTable
-- SELECT * INTO dbo.RejectionRateTable FROM 
-- (
-- select * from RejectionRateTable1
-- union 
-- select * from RejectionRateTable2
-- ) a
