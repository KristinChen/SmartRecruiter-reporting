----------------------------------------REJECTION----------------------------------------------
-- 4172
-- select count(distinct concat(candidateId, jobId, joinId)) from CleanedValidEvents where applicationStatus like '%REJECTED%' and (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%');
-- select count(distinct uniqueid) from outdata where applicationStatus like '%REJECTED%'

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

-- select * from RejectedAtStepOneApplicants --56 (correct)

RejectedAtStepMoreThanOneApplicants AS (
    select b.* from
(
    select candidateid, jobid, joinid, min(step) as minRejectionFlag from rejectedApplicants where rejectionFlag = 1 group by candidateId, jobId, joinid
) b
where minRejectionFlag > 1
),

-- select * from RejectedAtStepMoreThanOneApplicants --4116 (correct)

RejectionRateTable1 AS (
select d.jobCapability, 'REJECTED RIGHT THE WAY' applicationAggStatus, d.applicationAggSubStatus, count(distinct concat(candidateId, jobId, joinId)) as numApplicants from
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
group by d.jobCapability, d.applicationAggStatus, d.applicationAggSubStatus  
),

RejectionRateTable2 AS (
select d.jobCapability, d.applicationAggStatus, d.applicationAggSubStatus, count(distinct concat(candidateId, jobId, joinId)) as numApplicants from
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
group by d.jobCapability, d.applicationAggStatus, d.applicationAggSubStatus  
), 
RejectionRateTable AS (
(
select * from RejectionRateTable1
union 
select * from RejectionRateTable2
) ) 

-- select sum(numApplicants) from RejectionRateTable --4172

-- IF EXISTS(SELECT * FROM dbo.RejectionRateTable) DROP TABLE dbo.RejectionRateTable
SELECT * INTO dbo.RejectionRateTable FROM RejectionRateTable

SELECT * FROM RejectionRateTable; 