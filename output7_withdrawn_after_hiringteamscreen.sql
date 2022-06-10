-------------------------- report 1 ----------------------
WITH analyticalTable AS (
select *,
case when cleanedAnalyticalStatus like '%join%' then 1
     when cleanedAnalyticalStatus like '%recruiter screen%' and jobCapability like '%engineering%' then 3
     when cleanedAnalyticalStatus like '%recruiter screen%' and jobCapability not like '%engineering%' then 2
     when cleanedAnalyticalStatus like '%Skills Test%' and jobCapability like '%engineering%' then 2
     when cleanedAnalyticalStatus like '%Skills Test%' and jobCapability not like '%engineering%' then 3
     when cleanedAnalyticalStatus like '%Hiring Team Screen%' then 4
     when cleanedAnalyticalStatus like '%Tableau Assessment%' then 5
     when cleanedAnalyticalStatus like '%On-Site Interview%' then 6
     when cleanedAnalyticalStatus like '%Offer Pending%' then 7
     when cleanedAnalyticalStatus like '%HIRED%' then 8
     when cleanedAnalyticalStatus like '%rejected%' then 9
     when cleanedAnalyticalStatus like '%withdrawn%' then 10
     when cleanedAnalyticalStatus like '%transferred%' then 11
     else NULL end as idx
from 
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
      case when jobLevel like '%all-star%' then 0
           when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1
           else NULL
      end experienceFlag,
      case when analyticalStatus like '%join%' or analyticalStatus like '%rejoin%' then 'JOIN' else analyticalStatus END as cleanedAnalyticalStatus, 
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
),

-- 1. total applicants make it to recruiter screen: 1377 + 1103
WithdrawnApplicants_afterrs AS (
select jobCapability, experienceFlag, jobLocation, count(distinct uniqueId) numApplicants from
(
select * from analyticalTable where step = maxStep and cleanedAnalyticalStatus not like '%join%' and cleanedAnalyticalStatus not like '%rejoin%' and cleanedAnalyticalStatus not like '%rejected%'
union all 
select * from analyticalTable where step = maxStep and cleanedAnalyticalStatus like '%rejected%' and maxStep > 2
) a
group by jobCapability, experienceFlag, jobLocation
),

-- 3. total number of withdrawn applicants
WithdrawnApplicants_total AS (
select jobCapability, jobLocation, experienceFlag,  count(distinct uniqueId) totalWithdrawnApplicants from analyticalTable where cleanedAnalyticalStatus like '%withdrawn%' group by jobCapability, jobLocation, experienceFlag
),
 
WithdrawnApplicants_afterhs AS ( 
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
from analyticalTable where cleanedAnalyticalStatus like '%hiring team screen%') b where withdrawnFlag = 1 
group by jobCapability, jobLocation, experienceFlag
)

select a.jobCapability, a.jobLocation, a.experienceFlag, b.numApplicants numApplicants_makeittoRecruiterScreen, a.totalWithdrawnApplicants, c.numWithdrawnApplicant_afterhiringscreen from WithdrawnApplicants_total a 
join WithdrawnApplicants_afterrs b
on a.jobCapability = b.jobCapability and a.jobLocation = b.jobLocation and a.experienceFlag = b.experienceFlag
join WithdrawnApplicants_afterhs c
on a.jobCapability = c.jobCapability and a.jobLocation = c.jobLocation and a.experienceFlag = c.experienceFlag

-- 2. last funnel before withdrawn
select jobCapability, jobLocation, experienceFlag, analyticalStatus, count(distinct uniqueId) numApplicants from
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
from analyticalTable) b where withdrawnFlag = 1 and nextStatus like '%withdrawn%'
group by jobCapability, jobLocation, experienceFlag, analyticalStatus

-------------------------- report 2 ----------------------
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
      case when jobLevel like '%all-star%' then 0
           when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1
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
),

-- 1. total applicants make it to recruiter screen: 1377 + 1103
WithdrawnApplicants_afterrs AS (
select jobCapability, experienceFlag, count(distinct uniqueId) numApplicants from
(
select * from analyticalTable where step = maxStep and analyticalStatus not like '%join%' and analyticalStatus not like '%rejoin%' and analyticalStatus not like '%rejected%'
union all 
select * from analyticalTable where step = maxStep and analyticalStatus like '%rejected%' and maxStep > 2
) a
group by jobCapability, experienceFlag
),

-- 3. total number of withdrawn applicants
WithdrawnApplicants_total AS (
select jobCapability, experienceFlag, count(distinct uniqueId) totalWithdrawnApplicants from analyticalTable where analyticalStatus like '%withdrawn%' group by jobCapability, experienceFlag
),
 
WithdrawnApplicants_afterhs AS ( 
select jobCapability, experienceFlag, count(distinct uniqueId) numWithdrawnApplicant_afterhiringscreen from
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
group by jobCapability, experienceFlag
)

-- select a.jobCapability, a.experienceFlag, b.numApplicants numApplicants_makeittoRecruiterScreen, a.totalWithdrawnApplicants, c.numWithdrawnApplicant_afterhiringscreen from WithdrawnApplicants_total a 
-- join WithdrawnApplicants_afterrs b
-- on a.jobCapability = b.jobCapability  and a.experienceFlag = b.experienceFlag
-- join WithdrawnApplicants_afterhs c
-- on a.jobCapability = c.jobCapability  and a.experienceFlag = c.experienceFlag

-- 2. last funnel before withdrawn
select jobCapability, experienceFlag, analyticalStatus, count(distinct uniqueId) numApplicants from
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
from analyticalTable) b where withdrawnFlag = 1 and nextStatus like '%withdrawn%'
group by jobCapability, experienceFlag, analyticalStatus


-------------------------- report 3 ----------------------
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
      case when jobLevel like '%all-star%' then 0
           when jobLevel not like '%all-star%' and jobLevel not like '%intern%' then 1
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
),

-- 1. total applicants make it to recruiter screen: 1377 + 1103
WithdrawnApplicants_afterrs AS (
select jobCapability, jobLocation, count(distinct uniqueId) numApplicants from
(
select * from analyticalTable where step = maxStep and analyticalStatus not like '%join%' and analyticalStatus not like '%rejoin%' and analyticalStatus not like '%rejected%'
union all 
select * from analyticalTable where step = maxStep and analyticalStatus like '%rejected%' and maxStep > 2
) a
group by jobCapability, jobLocation
),

-- 3. total number of withdrawn applicants
WithdrawnApplicants_total AS (
select jobCapability, jobLocation, count(distinct uniqueId) totalWithdrawnApplicants from analyticalTable where analyticalStatus like '%withdrawn%' group by jobCapability, jobLocation
),
 
WithdrawnApplicants_afterhs AS ( 
select jobCapability, jobLocation, count(distinct uniqueId) numWithdrawnApplicant_afterhiringscreen from
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
group by jobCapability, jobLocation
)

select a.jobCapability, a.jobLocation, b.numApplicants numApplicants_makeittoRecruiterScreen, a.totalWithdrawnApplicants, c.numWithdrawnApplicant_afterhiringscreen from WithdrawnApplicants_total a 
join WithdrawnApplicants_afterrs b
on a.jobCapability = b.jobCapability  and a.jobLocation = b.jobLocation
join WithdrawnApplicants_afterhs c
on a.jobCapability = c.jobCapability  and a.jobLocation = c.jobLocation

-- 2. last funnel before withdrawn
-- select jobCapability, jobLocation, analyticalStatus, count(distinct uniqueId) numApplicants from
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
-- group by jobCapability, jobLocation, analyticalStatus