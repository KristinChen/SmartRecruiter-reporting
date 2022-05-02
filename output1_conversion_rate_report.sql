-- SELECT * FROM dbo.AggReportTable1 where jobCapability like '%eng%' AND joblevel like '%all%' and jobLocation like '%co' and week = '14';
-- select * from JoinApplicants where jobCapability like '%eng%' AND joblevel like '%all%' and jobLocation like '%co' and week = '14';
-- select * from INFunnelApplicants where jobCapability like '%eng%' AND joblevel like '%all%' and jobLocation like '%co' and week = '14'
-- select * from ActiveInFunnelApplicants where jobCapability like '%eng%' AND joblevel like '%all%' and jobLocation like '%co' and week = '14'
-- select * from InactiveInFunnelApplicants where jobCapability like '%eng%' AND joblevel like '%all%' and jobLocation like '%co' and week = '14'
-- select * from outApplicants where jobCapability like '%eng%' AND joblevel like '%all%' and jobLocation like '%co' and week = '14'

WITH AnalysisTable1 AS
(select *, 
      YEAR(eventDate) AS year, 
      MONTH(eventDate) AS month, 
      DATEPART(WEEK, eventDate) as week,
      DATEPART(WEEKDAY, eventDate) as weekday,
      concat(candidateId, '&', jobId) as uniqueId
from CleanedValidEvents),

OutConvertReportTable AS (
select jobCapability, jobLevel, jobLocation, startYear, startWeek, funnel, applicationStatus, applicationAggStatus, applicationSubStatus, applicationAggSubStatus, count(distinct uniqueId) numApplicants from
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
and a2.uniqueId = e.uniqueId 
and a2.jobCapability = e.jobCapability 
and a2.jobLevel = e.jobLevel 
and a2.jobLocation = e.jobLocation
) a
group by jobCapability, joblevel, joblocation, startYear, startWeek, funnel, applicationStatus, applicationAggStatus, applicationSubStatus, applicationAggSubStatus
),

InFunnelConvertReportTable AS (
select jobCapability, jobLevel, jobLocation, startYear, startWeek, funnel, applicationStatus, applicationAggStatus, applicationSubStatus, applicationAggSubStatus, count(distinct uniqueId) numApplicants from
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
select * from ActiveInFunnelApplicants 
) a2
left JOIN
AnalysisTable1 e
ON a2.candidateId = e.candidateId 
and a2.jobId = e.jobId 
and a2.uniqueId = e.uniqueId 
and a2.jobCapability = e.jobCapability 
and a2.jobLevel = e.jobLevel 
and a2.jobLocation = e.jobLocation
) a
group by jobCapability, joblevel, joblocation, startYear, startWeek, funnel, applicationStatus, applicationAggStatus, applicationSubStatus, applicationAggSubStatus
)

-- IF EXISTS(SELECT * FROM  dbo.ConversionRateWeeklyTable) DROP TABLE dbo.ConversionRateWeeklyTable;
-- SELECT * INTO dbo.OutDataConversionRateWeeklyTable FROM OutConvertReportTable order by jobCapability, joblevel, joblocation, startYear, startWeek;

-- IF EXISTS(SELECT * FROM  dbo.ConversionRateWeeklyTable) DROP TABLE dbo.ConversionRateWeeklyTable;
SELECT * INTO dbo.InFunnelDataConversionRateWeeklyTable FROM InFunnelConvertReportTable order by jobCapability, joblevel, joblocation, startYear, startWeek;






-- SELECT * FROM dbo.AggReportTable1 where jobCapability like '%intelligence%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14';
-- select * from JoinApplicants where jobCapability like '%intelligence%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14';
-- select * from INFunnelApplicants where jobCapability like '%intelligence%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14';
-- select * from ActiveInFunnelApplicants where jobCapability like '%intelligence%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14';
-- select * from InactiveInFunnelApplicants where jobCapability like '%intelligence%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14';
-- select * from outApplicants where jobCapability like '%intelligence%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14';

SELECT * FROM dbo.AggReportTable1 where jobCapability like '%intelligence%' AND joblevel like '%all%' and jobLocation like '%md' and week = '14';

SELECT * FROM dbo.OutDataConversionRateWeeklyTable where jobCapability like '%intelligence%' AND joblevel like '%all%' and jobLocation like '%md' and startWeek = '14';
SELECT * FROM dbo.InFunnelDataConversionRateWeeklyTable where jobCapability like '%intelligence%' AND joblevel like '%all%' and jobLocation like '%md' and startWeek = '14';
