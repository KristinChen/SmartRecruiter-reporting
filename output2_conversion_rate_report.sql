WITH AnalysisTable1 AS
(select *, 
      YEAR(eventDate) AS year, 
      MONTH(eventDate) AS month, 
      DATEPART(WEEK, eventDate) as week,
      DATEPART(WEEKDAY, eventDate) as weekday,
      concat(candidateId, '&', jobId, '&', joinId) as uniqueId
from CleanedValidEvents),

OutWeeklyConvertReportTable AS (
select jobCapability, jobLevel, jobLocation, startYear, startWeek, funnel, applicationAggStatus, applicationAggSubStatus, count(distinct uniqueId) numApplicants from
( 
select e.*, 
       a2.year startYear, 
       a2.week startWeek,
       case when funnel like '%join%' or funnel like '%rejoin%' then 'NEW' 
            when funnel IS NULL then 'IN_REVIEW' 
       else applicationStatus
       end applicationAggStatus,

       case when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Recruiter Screen' 
            when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending'
            
            when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
            when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
       
            -- experienced
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%warm%' or applicationSubStatus like '%test%' or applicationSubStatus like '%interview%') then 'On-Site Interview' 
       else applicationSubStatus
       end applicationAggSubStatus
       from
(
select * from outApplicants 
) a2
left JOIN
AnalysisTable1 e
ON a2.candidateId = e.candidateId 
and a2.jobId = e.jobId 
and a2.joinid = e.joinid 
and a2.uniqueId = e.uniqueId 
and a2.jobCapability = e.jobCapability 
and a2.jobLevel = e.jobLevel 
and a2.jobLocation = e.jobLocation
) a
group by jobCapability, joblevel, joblocation, startYear, startWeek, funnel, applicationAggStatus, applicationAggSubStatus
),

OutTotalConvertReportTable AS (
select jobCapability, jobLevel, jobLocation, funnel, applicationAggStatus, applicationAggSubStatus, count(distinct uniqueId) numApplicants from
( 
select e.*, 
    --    a2.year startYear, 
    --    a2.week startWeek,
       case when funnel like '%join%' or funnel like '%rejoin%' then 'NEW' 
            when funnel IS NULL then 'IN_REVIEW' 
       else applicationStatus
       end applicationAggStatus,

       case when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Recruiter Screen' 
            when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending'
            
            when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
            when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
       
            -- experienced
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%warm%' or applicationSubStatus like '%test%' or applicationSubStatus like '%interview%') then 'On-Site Interview' 
       else applicationSubStatus
       end applicationAggSubStatus
       from
(
select * from outApplicants 
) a2
left JOIN
AnalysisTable1 e
ON a2.candidateId = e.candidateId 
and a2.jobId = e.jobId 
and a2.joinid = e.joinid 
and a2.uniqueId = e.uniqueId 
and a2.jobCapability = e.jobCapability 
and a2.jobLevel = e.jobLevel 
and a2.jobLocation = e.jobLocation
) a
group by jobCapability, joblevel, joblocation, funnel, applicationAggStatus, applicationAggSubStatus
),

InFunnelWeeklyConvertReportTable AS (
select jobCapability, jobLevel, jobLocation, startYear, startWeek, funnel, applicationAggStatus, applicationAggSubStatus, count(distinct uniqueId) numApplicants from
( 
select e.*, 
       a2.year startYear, 
       a2.week startWeek,
       case when funnel like '%join%' or funnel like '%rejoin%' then 'NEW' 
            when funnel IS NULL then 'IN_REVIEW' 
       else applicationStatus
       end applicationAggStatus,

       case when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Recruiter Screen' 
            when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending'
            
    
            -- when (e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%sql test%' or applicationSubStatus like '%tableau assessment%') then 'Skills Test'

            when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
            when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
       
            -- experienced
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%warm%' or applicationSubStatus like '%test%' or applicationSubStatus like '%interview%') then 'On-Site Interview' 
       else applicationSubStatus
       end applicationAggSubStatus
       from
(
select * from ActiveInFunnelApplicants 
) a2
left JOIN
AnalysisTable1 e
ON a2.candidateId = e.candidateId 
and a2.jobId = e.jobId 
and a2.joinid = e.joinid 
and a2.uniqueId = e.uniqueId 
and a2.jobCapability = e.jobCapability 
and a2.jobLevel = e.jobLevel 
and a2.jobLocation = e.jobLocation
) a
group by jobCapability, joblevel, joblocation, startYear, startWeek, funnel, applicationAggStatus, applicationAggSubStatus
),
InFunnelTotalConvertReportTable AS (
select jobCapability, jobLevel, jobLocation, funnel, applicationAggStatus, applicationAggSubStatus, count(distinct uniqueId) numApplicants from
( 
select e.*, 
       --a2.year startYear, 
       --a2.week startWeek,
       case when funnel like '%join%' or funnel like '%rejoin%' then 'NEW' 
            when funnel IS NULL then 'IN_REVIEW' 
       else applicationStatus
       end applicationAggStatus,

       case when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Recruiter Screen' 
            when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending'
            
    
            -- when (e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%sql test%' or applicationSubStatus like '%tableau assessment%') then 'Skills Test'

            when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
            when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
       
            -- experienced
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
            when e.joblevel not like 'all-star' and (applicationSubStatus like '%warm%' or applicationSubStatus like '%test%' or applicationSubStatus like '%interview%') then 'On-Site Interview' 
       else applicationSubStatus
       end applicationAggSubStatus
       from
(
select * from ActiveInFunnelApplicants 
) a2
left JOIN
AnalysisTable1 e
ON a2.candidateId = e.candidateId 
and a2.jobId = e.jobId 
and a2.joinid = e.joinid 
and a2.uniqueId = e.uniqueId 
and a2.jobCapability = e.jobCapability 
and a2.jobLevel = e.jobLevel 
and a2.jobLocation = e.jobLocation
) a
group by jobCapability, joblevel, joblocation, funnel, applicationAggStatus, applicationAggSubStatus
),
tbl1 AS (
select experiencedFlag, 1 OutFlag, jobCapability, jobLocation, funnel,applicationAggStatus, applicationAggSubStatus, sum(numApplicants) as totalNumApplicants from 
(SELECT *, case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
                when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag FROM OutTotalConvertReportTable) f
group by experiencedFlag, jobCapability, jobLocation, jobCapability, funnel,applicationAggStatus, applicationAggSubStatus
having experiencedFlag is not null
union
select experiencedFlag, 0 OutFlag, jobCapability, jobLocation, funnel,applicationAggStatus, applicationAggSubStatus, sum(numApplicants) as totalNumApplicants from 
(SELECT *, case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
                when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag FROM InFunnelTotalConvertReportTable) f
group by experiencedFlag, jobCapability, jobLocation, jobCapability, funnel,applicationAggStatus, applicationAggSubStatus
having experiencedFlag is not null
)

-- IF EXISTS(SELECT * FROM  dbo.ConversionRateTable) DROP TABLE dbo.ConversionRateTable;
SELECT * INTO dbo.ConversionRateTable FROM tbl1; 

-- IF EXISTS(SELECT * FROM  dbo.OutConversionRateTable) DROP TABLE dbo.OutConversionRateTable;
-- SELECT * INTO dbo.OutConversionRateTable FROM tbl1; 

select * from dbo.ConversionRateTable; 