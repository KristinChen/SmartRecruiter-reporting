-- # of rejected needs to equal to # in rejection_rate_report

WITH OutConversionRateTable1 AS (
select 
     jobCapability, 
     experiencedFlag, 
     jobLocation, 
     funnel, 
     applicationStatus,
     -- applicationAggStatus, 
     applicationSubStatus,
     -- applicationAggSubStatus, 
     count(distinct uniqueId) numApplicants 
from
( 
select e.*, 

     --   case when funnel like '%join%' or funnel like '%rejoin%' then 'NEW' 
     --        when funnel IS NULL then 'IN_REVIEW' 
     --   else applicationStatus
     --   end applicationAggStatus,

     --   case when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Recruiter Screen' 
     --        when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
     --        when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending'
            
     --        when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
     --        when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
     --        when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
       
     --        -- experienced
     --        when e.joblevel not like 'all-star' and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
     --        when e.joblevel not like 'all-star' and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
     --        when e.joblevel not like 'all-star' and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
     --        when e.joblevel not like 'all-star' and (applicationSubStatus like '%warm%' or applicationSubStatus like '%test%' or applicationSubStatus like '%interview%') then 'On-Site Interview' 
     --   else applicationSubStatus
     --   end applicationAggSubStatus,

       case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
       when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag
       from OutData e
) a
group by jobCapability, experiencedFlag, joblocation, funnel, applicationStatus, applicationSubStatus
-- having experiencedFlag is not null and jobLocation not like '%remote%'
),
OutConversionRateTable2 AS (
select 
     jobCapability, 
     experiencedFlag, 
     jobLocation, 
     applicationStatus,
     count(distinct uniqueId) numApplicants 
from
( 
select e.*, 

     --   case when funnel like '%join%' or funnel like '%rejoin%' then 'NEW' 
     --        when funnel IS NULL then 'IN_REVIEW' 
     --   else applicationStatus
     --   end applicationAggStatus,

     --   case when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Recruiter Screen' 
     --        when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
     --        when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending'
            
     --        when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
     --        when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
     --        when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
       
     --        -- experienced
     --        when e.joblevel not like 'all-star' and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
     --        when e.joblevel not like 'all-star' and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
     --        when e.joblevel not like 'all-star' and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
     --        when e.joblevel not like 'all-star' and (applicationSubStatus like '%warm%' or applicationSubStatus like '%test%' or applicationSubStatus like '%interview%') then 'On-Site Interview' 
     --   else applicationSubStatus
     --   end applicationAggSubStatus,

       case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
       when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag
       from OutData e
) a
group by jobCapability, experiencedFlag, joblocation, applicationStatus
-- having experiencedFlag is not null and jobLocation not like '%remote%'
),
ActiveInFunnelConversionRateTable AS (
select 
     jobCapability, 
     experiencedFlag, 
     jobLocation, 
     funnel, 
     applicationStatus,
     -- applicationAggStatus, 
     applicationSubStatus,
     -- applicationAggSubStatus, 
     count(distinct uniqueId) numApplicants 
from
( 
select e.*, 

     --   case when funnel like '%join%' or funnel like '%rejoin%' then 'NEW' 
     --        when funnel IS NULL then 'IN_REVIEW' 
     --   else applicationStatus
     --   end applicationAggStatus,

     --   case when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Recruiter Screen' 
     --        when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
     --        when (e.jobCapability like '%engineer%' or e.jobCapability like '%business%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending'
            
     --        when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
     --        when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
     --        when (e.jobCapability like '%science%') and (e.joblevel like 'all-star') and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
       
     --        -- experienced
     --        when e.joblevel not like 'all-star' and (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Resume Review' 
     --        when e.joblevel not like 'all-star' and (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
     --        when e.joblevel not like 'all-star' and (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
     --        when e.joblevel not like 'all-star' and (applicationSubStatus like '%warm%' or applicationSubStatus like '%test%' or applicationSubStatus like '%interview%') then 'On-Site Interview' 
     --   else applicationSubStatus
     --   end applicationAggSubStatus,

       case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
       when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag
       from ActiveInFunnelData e
) a
group by jobCapability, experiencedFlag, joblocation, funnel, applicationStatus, applicationSubStatus
-- having experiencedFlag is not null and jobLocation not like '%remote%'
),
OutConversionRateTable AS (
select * from OutConversionRateTable1 where (applicationStatus not like '%hired%' and applicationStatus not like '%withdrawn%' and applicationStatus not like '%rejected%' and applicationStatus not like '%transferred%')
union all
(select jobCapability, experiencedFlag, jobLocation,'OUT' funnel, applicationStatus,NULL applicationSubStatus, numApplicants from OutConversionRateTable2 where (applicationStatus like '%hired%' or applicationStatus like '%withdrawn%' or applicationStatus like '%rejected%' or applicationStatus like '%transferred%'))
) 
-- select sum(numapplicants) from OutConversionRateTable where applicationStatus like '%rejected%' --4052 (matched!)
-- select sum(numapplicants) from OutConversionRateTable where applicationStatus like '%withdrawn%' --176 
-- select sum(numapplicants) from OutConversionRateTable where applicationStatus like '%hired%' --58 

-- IF EXISTS(SELECT * FROM  dbo.OutConversionRateTable) DROP TABLE dbo.OutConversionRateTable;
-- SELECT * INTO dbo.OutConversionRateTable FROM OutConversionRateTable; 

-- IF EXISTS(SELECT * FROM  dbo.ActiveInFunnelConversionRateTable) DROP TABLE dbo.ActiveInFunnelConversionRateTable;
-- SELECT * INTO dbo.ActiveInFunnelConversionRateTable FROM ActiveInFunnelConversionRateTable; 