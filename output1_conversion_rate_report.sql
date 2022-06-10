select jobCapability, jobLocation, experiencedFlag, cleanedAnalyticalStatus, idx, count(distinct uniqueId) numApplicants from
(
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
select 
*,
case when analyticalStatus like '%join%' or analyticalStatus like '%rejoin%' then 'JOIN' else analyticalStatus END as cleanedAnalyticalStatus 
from 
OutData 
) b
) c
where experiencedFlag is not NULL and analyticalStatus not like '%offer accepted%'
group by jobCapability, jobLocation, experiencedFlag, idx, cleanedAnalyticalStatus
order by jobCapability, jobLocation, experiencedFlag, idx, numApplicants DESC; 

--- report 2   ---------------------------------------------------
select jobCapability, experiencedFlag, cleanedAnalyticalStatus, idx, count(distinct uniqueId) numApplicants from
(
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
select 
*,
case when analyticalStatus like '%join%' or analyticalStatus like '%rejoin%' then 'JOIN' else analyticalStatus END as cleanedAnalyticalStatus 
from 
OutData 
) b
) c
where experiencedFlag is not NULL and analyticalStatus not like '%offer accepted%'
group by jobCapability, experiencedFlag, idx, cleanedAnalyticalStatus
order by jobCapability, experiencedFlag, idx, numApplicants DESC; 

-- report 3   -------------------------------------------------------------------------
select jobCapability, jobLocation, cleanedAnalyticalStatus, idx, count(distinct uniqueId) numApplicants from
(
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
select 
*,
case when analyticalStatus like '%join%' or analyticalStatus like '%rejoin%' then 'JOIN' else analyticalStatus END as cleanedAnalyticalStatus 
from 
OutData 
) b
) c
where experiencedFlag is not NULL and analyticalStatus not like '%offer accepted%'
group by jobCapability, jobLocation, idx, cleanedAnalyticalStatus
order by jobCapability, jobLocation, idx, numApplicants DESC; 

-- report 4   -------------------------------------------------------------------------
select jobCapability, jobLocation, experiencedFlag, cleanedAnalyticalStatus, idx, count(distinct uniqueId) numApplicants from
(
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
select 
*,
case when analyticalStatus like '%join%' or analyticalStatus like '%rejoin%' then 'JOIN' else analyticalStatus END as cleanedAnalyticalStatus 
from 
ActiveInFunnelData 
) b
) c
where experiencedFlag is not NULL and analyticalStatus not like '%offer accepted%'
group by jobCapability, jobLocation, experiencedFlag, idx, cleanedAnalyticalStatus
order by jobCapability, jobLocation, experiencedFlag, idx, numApplicants DESC; 

-- report 5   -------------------------------------------------------------------------
select jobCapability, experiencedFlag, cleanedAnalyticalStatus, idx, count(distinct uniqueId) numApplicants from
(
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
select 
*,
case when analyticalStatus like '%join%' or analyticalStatus like '%rejoin%' then 'JOIN' else analyticalStatus END as cleanedAnalyticalStatus 
from 
ActiveInFunnelData 
) b
) c
where experiencedFlag is not NULL and analyticalStatus not like '%offer accepted%'
group by jobCapability, experiencedFlag, idx, cleanedAnalyticalStatus
order by jobCapability, experiencedFlag, idx, numApplicants DESC; 

-- report 6   -------------------------------------------------------------------------
select jobCapability, jobLocation, cleanedAnalyticalStatus, idx, count(distinct uniqueId) numApplicants from
(
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
select 
*,
case when analyticalStatus like '%join%' or analyticalStatus like '%rejoin%' then 'JOIN' else analyticalStatus END as cleanedAnalyticalStatus 
from 
ActiveInFunnelData 
) b
) c
where experiencedFlag is not NULL and analyticalStatus not like '%offer accepted%'
group by jobCapability, jobLocation, idx, cleanedAnalyticalStatus
order by jobCapability, jobLocation, idx, numApplicants DESC; 
