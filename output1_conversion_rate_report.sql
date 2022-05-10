-- # of rejected needs to equal to # in rejection_rate_report

WITH OutConversionRateTable1 AS (
select 
     jobCapability, 
     experiencedFlag, 
     jobLocation, 
     applicationStatus,
     applicationAggSubStatus, 
     count(distinct uniqueId) numApplicants 
from
( 
select e.*, 

     -- OVERALL
       case when (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Recruiter Screen' 
            when (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
   
     -- ALL-STAR
            when (e.jobCapability like '%business%' or e.jobCapability like '%engineering%') and (applicationSubStatus like '%skills test%' or applicationSubStatus like '%sql test completed%') then 'Skills Test'
     
     -- EXPERIENCED
            when (e.jobCapability like '%business%' or e.jobCapability like '%engineering%') and (jobLevel not like '%all-star%' or jobLevel not like '%intern%') and (applicationSubStatus like '%skills test%' or applicationstatus like '%keep warm%') then 'On-site Interview'
         
       else applicationSubStatus
       end applicationAggSubStatus,

       case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
       when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag
       from OutData e
) a
group by jobCapability, experiencedFlag, joblocation, applicationStatus, applicationAggSubStatus
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
       case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
       when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag
       from OutData e
) a
group by jobCapability, experiencedFlag, joblocation, applicationStatus
),
ActiveInFunnelConversionRateTable AS (
select 
     jobCapability, 
     experiencedFlag, 
     jobLocation, 
     applicationStatus,
     applicationSubStatus,
     count(distinct uniqueId) numApplicants 
from
( 
select e.*, 

     -- OVERALL
       case when (applicationSubStatus like '%resume review%' or applicationSubStatus like '%recruiter screen%') then 'Recruiter Screen' 
            when (applicationSubStatus like '%manager%' or applicationSubStatus like '%hiring team screen%') then 'Hiring Team Screen' 
            when (applicationSubStatus like '%offer%' or applicationSubStatus like '%caucus%') then 'Offer Pending' 
   
     -- ALL-STAR
            when (e.jobCapability like '%business%' or e.jobCapability like '%engineering%') and (applicationSubStatus like '%skills test%' or applicationSubStatus like '%sql test completed%') then 'Skills Test'
     
     -- EXPERIENCED
            when (e.jobCapability like '%business%' or e.jobCapability like '%engineering%') and (jobLevel not like '%all-star%' or jobLevel not like '%intern%') and (applicationSubStatus like '%skills test%' or applicationstatus like '%keep warm%') then 'On-site Interview'
         
       else applicationSubStatus
       end applicationAggSubStatus,

       case when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1 
       when jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag
       from ActiveInFunnelData e
) a
group by jobCapability, experiencedFlag, joblocation, applicationStatus, applicationSubStatus
),
OutConversionRateTable AS (
select * from OutConversionRateTable1 where (applicationStatus not like '%hired%' and applicationStatus not like '%withdrawn%' and applicationStatus not like '%rejected%' and applicationStatus not like '%transferred%')
union all
(select jobCapability, experiencedFlag, jobLocation, applicationStatus, NULL applicationAggSubStatus, numApplicants from OutConversionRateTable2 where (applicationStatus like '%hired%' or applicationStatus like '%withdrawn%' or applicationStatus like '%rejected%' or applicationStatus like '%transferred%'))
) 
-- select sum(numapplicants) from OutConversionRateTable where applicationStatus like '%rejected%' --4172 (matched!)
-- select sum(numapplicants) from OutConversionRateTable where applicationStatus like '%withdrawn%' --184 
-- select sum(numapplicants) from OutConversionRateTable where applicationStatus like '%hired%' --60 
-- select sum(numapplicants) from OutConversionRateTable where applicationStatus like '%new%' --4980 

-- IF EXISTS(SELECT * FROM  dbo.OutConversionRateTable) DROP TABLE dbo.OutConversionRateTable;
-- SELECT * INTO dbo.OutConversionRateTable FROM OutConversionRateTable; 

-- IF EXISTS(SELECT * FROM  dbo.ActiveInFunnelConversionRateTable) DROP TABLE dbo.ActiveInFunnelConversionRateTable;
-- SELECT * INTO dbo.ActiveInFunnelConversionRateTable FROM ActiveInFunnelConversionRateTable; 

