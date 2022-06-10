WITH analyticalTable AS (
select * from 
(
select * , 
      lag(analyticalStatus, 1) over (partition by uniqueId order by eventDate) as lastStatus, 
      lag(analyticalStatus, 2) over (partition by uniqueId order by eventDate) as lastStatus2, 
      lag(analyticalStatus, 3) over (partition by uniqueId order by eventDate) as lastStatus3, 
      lag(analyticalStatus, 4) over (partition by uniqueId order by eventDate) as lastStatus4, 
      lag(analyticalStatus, 5) over (partition by uniqueId order by eventDate) as lastStatus5, 
      lag(analyticalStatus, 6) over (partition by uniqueId order by eventDate) as lastStatus6, 
      lag(analyticalStatus, 7) over (partition by uniqueId order by eventDate) as lastStatus7, 
      lag(analyticalStatus, 8) over (partition by uniqueId order by eventDate) as lastStatus8, 
      lag(analyticalStatus, 9) over (partition by uniqueId order by eventDate) as lastStatus9, 
      lag(analyticalStatus, 10) over (partition by uniqueId order by eventDate) as lastStatus10, 
      lead(analyticalStatus, 1) over (partition by uniqueId order by eventDate) as nextStatus, 
      lead(analyticalStatus, 2) over (partition by uniqueId order by eventDate) as nextStatus2, 
      lead(analyticalStatus, 3) over (partition by uniqueId order by eventDate) as nextStatus3, 
      lead(analyticalStatus, 4) over (partition by uniqueId order by eventDate) as nextStatus4, 
      lead(analyticalStatus, 5) over (partition by uniqueId order by eventDate) as nextStatus5, 
      lead(analyticalStatus, 6) over (partition by uniqueId order by eventDate) as nextStatus6, 
      lead(analyticalStatus, 7) over (partition by uniqueId order by eventDate) as nextStatus7, 
      lead(analyticalStatus, 8) over (partition by uniqueId order by eventDate) as nextStatus8, 
      lead(analyticalStatus, 9) over (partition by uniqueId order by eventDate) as nextStatus9, 
      lead(analyticalStatus, 10) over (partition by uniqueId order by eventDate) as nextStatus10, 
      case when jobLevel like '%all-star%' then 'All-Star'
           when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 'Experienced'
           else NULL
      end experienceFlag,
      max(step) over (partition by uniqueId) maxStep
from
(
select distinct eventId, candidateId, jobId, jobLevel, jobCapability, jobLocation, eventDate, eventType, applicationStatus, applicationSubstatus, applicationSource, applicationStatusReason, resubmittedFlag, sumResubmittedFlag, analyticalStatus, joinId,
      ROW_NUMBER() over (partition by candidateId, jobid, joinId order by eventDate) step, 
      concat(candidateId, '&', jobId, '&', joinId) as uniqueId
from CleanedValidEvents
) b
) c
where experienceFlag is not null 
) 

-- 1. total applicants make it to recruiter screen: 1377 + 1103
-- select jobCapability, experienceFlag, jobLocation, count(distinct uniqueId) numApplicants from
-- (
-- select * from analyticalTable where step = maxStep and analyticalStatus not like '%join%' and analyticalStatus not like '%rejoin%' and analyticalStatus not like '%rejected%'
-- union all 
-- select * from analyticalTable where step = maxStep and analyticalStatus like '%rejected%' and maxStep > 2
-- ) a
-- group by jobCapability, experienceFlag, jobLocation

-- 2. last funnel before withdrawn
-- select jobCapability, jobLocation, experienceFlag, analyticalStatus, count(distinct uniqueId) numApplicants from
-- (
-- select *, 
-- case when nextStatus like '%withdrawn%' then 1 
--       when nextStatus2 like '%withdrawn%' then 1 
--       when nextStatus3 like '%withdrawn%' then 1 
--       when nextStatus4 like '%withdrawn%' then 1 
--       when nextStatus5 like '%withdrawn%' then 1 
--       when nextStatus6 like '%withdrawn%' then 1 
--       when nextStatus7 like '%withdrawn%' then 1 
--       when nextStatus8 like '%withdrawn%' then 1 
--       when nextStatus9 like '%withdrawn%' then 1 
--       when nextStatus10 like '%withdrawn%' then 1 
--       else 0 
--       end as withdrawnFlag 
-- from analyticalTable) b where withdrawnFlag = 1 and nextStatus like '%withdrawn%'
-- group by jobCapability, jobLocation, experienceFlag, analyticalStatus

-- 3. total number of withdrawn applicants
-- select jobCapability, jobLocation, experienceFlag, count(distinct uniqueId) totalWithdrawnApplicants from analyticalTable where analyticalStatus like '%withdrawn%' group by jobCapability, jobLocation, experienceFlag; 

-- 4. 
select jobCapability, jobLocation, experienceFlag, count(distinct uniqueId) numWithdrawnApplicant_afterhiringscreen from
(
select *, 
case when nextStatus like '%withdrawn%' then 1 
      when nextStatus2 like '%withdrawn%' then 1 
      when nextStatus3 like '%withdrawn%' then 1 
      when nextStatus4 like '%withdrawn%' then 1 
      when nextStatus5 like '%withdrawn%' then 1 
      when nextStatus6 like '%withdrawn%' then 1 
      when nextStatus7 like '%withdrawn%' then 1 
      when nextStatus8 like '%withdrawn%' then 1 
      when nextStatus9 like '%withdrawn%' then 1 
      when nextStatus10 like '%withdrawn%' then 1 
      else 0 
      end as withdrawnFlag 
from analyticalTable where analyticalStatus like '%hiring team screen%') b where withdrawnFlag = 1 
group by jobCapability, jobLocation, experienceFlag


-- data entry error select * from ApplicationEvents_Merged where candidateId like '04ef4cdf-84d2-4927-940c-7b2c5b3ef05c' and jobId like 'd2cbe9b8-2a02-4fe1-a520-37116daec8c1' order by eventDate
