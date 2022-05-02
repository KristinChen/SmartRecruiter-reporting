WITH OutTotalConvertReportTable AS (
select 
     jobCapability, 
     experiencedFlag, 
     jobLocation, 
     funnel, 
     applicationAggStatus, 
     applicationAggSubStatus, 
     count(distinct uniqueId) numApplicants 
from
( 
select e.*, 

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
       end applicationAggSubStatus,

       case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
       when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag
       from ActiveOutData e
) a
group by jobCapability, experiencedFlag, joblocation, funnel, applicationAggStatus, applicationAggSubStatus
having experiencedFlag is not null and jobLocation not like '%remote%'
),

ActiveInFunnelTotalConvertReportTable AS (
select 
     jobCapability, 
     experiencedFlag, 
     jobLocation, 
     funnel, 
     applicationAggStatus, 
     applicationAggSubStatus, 
     count(distinct uniqueId) numApplicants 
from
( 
select e.*, 

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
       end applicationAggSubStatus,

       case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
       when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag
       from InFunnelData e
) a
group by jobCapability, experiencedFlag, joblocation, funnel, applicationAggStatus, applicationAggSubStatus
having experiencedFlag is not null and jobLocation not like '%remote%'
)

-- IF EXISTS(SELECT * FROM  dbo.OutConversionRateTable) DROP TABLE dbo.OutConversionRateTable;
-- SELECT * INTO dbo.OutConversionRateTable FROM OutTotalConvertReportTable; 

-- IF EXISTS(SELECT * FROM  dbo.OutConversionRateTable) DROP TABLE dbo.OutConversionRateTable;
-- SELECT * INTO dbo.ActiveInFunnelTotalConvertReportTable FROM ActiveInFunnelTotalConvertReportTable; 
