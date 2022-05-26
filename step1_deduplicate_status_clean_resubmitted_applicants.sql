-- Clean job capability -----------------------------------------------------
with CleanedJobCapabilityTable as 
(
select * from 
(select distinct tot.eventId, 
    tot.candidateId, 
    tot.jobId,
    tot.jobLevel, 
    c.cleanedJobCapability jobCapability, 
    tot.jobLocation, 
    tot.eventDate, 
    tot.eventType, 
    tot.applicationStatus, 
    tot.applicationSubStatus,
    tot.applicationSource,
    tot.applicationStatusReason 
from
(
select b.candidateId, b.jobId, b.jobCapability cleanedJobCapability from
(select distinct candidateId, jobId, jobCapability from ApplicationEvents_Merged where jobCapability is NULL) a
right join 
(select distinct candidateId, jobId, jobCapability from ApplicationEvents_Merged where jobCapability is NOT NULL) b
on a.candidateId = b.candidateId and a.jobid = b.jobid
) c
right join
ApplicationEvents_Merged tot
on
tot.candidateId = c.candidateId and tot.jobid = c.jobid 
) d
where (jobCapability like '%Data Science%' or jobCapability like '%Data Engineering%' or jobCapability like '%Business Intelligence%')
), 

-- get candidates aplied after 2022-01-01, and only three jobCapability -----------------------------------------------------
CleanedTable AS (
select distinct c.* from
(
select candidateId, jobId from 
(
select *, 
      ROW_NUMBER() over (partition by candidateId, jobid order by eventDate) step,
      case when eventDate >= '2022-01-01' then 1 else 0 end splitFlag
 from CleanedJobCapabilityTable 
) a
where step = 1 and 
      splitFlag = 1
) b 
left join 
CleanedJobCapabilityTable c on b.candidateId = c.candidateId and b.jobid = c.jobid
),

-- resummittedFlag --------------------------------------------------------------------
FlagResumbittedCasesTable AS (
select distinct *, sum(resubmittedFlag) over (partition by candidateId, jobId order by eventDate) as sumResubmittedFlag from 
(
select *, case when eventType like '%Application Re-Submitted%' then 1 else 0 end as resubmittedFlag 
from CleanedTable
) b 
),

-- remove duplicate status per candidateid, jobid -----------------------------------------------------
DeduplicatedTable AS (
select distinct * from 
(
select *, ROW_NUMBER() over (partition by candidateid, jobid, sumResubmittedFlag, analyticalStatus order by eventDate) usefulStep from 
(
select *, 
case when (applicationStatus like '%lead%' or applicationStatus like '%new%') and eventType not like '%Application Re-Submitted%' then 'JOIN'
     when eventType like '%Application Re-Submitted%' then 'REJOIN'
     when applicationStatus like '%rejected%' then 'REJECTED'
     when applicationStatus like '%hired%' then 'HIRED'
     when applicationStatus like '%withdrawn%' then 'WITHDRAWN'
     when applicationStatus like '%transferred%' then 'TRANSFERRED'
     when applicationSubStatus like '%recruiter screen%' then 'Recruiter Screen'
     when applicationSubStatus like '%resume review%' then 'Recruiter Screen'
     when applicationSubStatus like '%Hiring Team Screen%' then 'Hiring Team Screen'
     when applicationSubStatus like '%Skills Test%' then 'Skills Test'
     when applicationSubStatus like '%Submitted to Manager%' then 'Hiring Team Screen'
     when applicationSubStatus like '%SQL Test Completed%' then 'Skills Test'
     when applicationSubStatus like '%Tableau Assessment%' then 'Tableau Assessment'
     when applicationSubStatus like '%Keep Warm%' then 'On-Site Interview'
     when applicationSubStatus like '%On-Site Interview%' then 'On-Site Interview'
     when applicationSubStatus like '%On-Site Test%' then 'On-Site Interview'
     when applicationSubStatus like '%Passed Caucus%' then 'On-Site Interview'
     when applicationSubStatus like '%Offer Pending%' then 'Offer Pending'
     when applicationSubStatus like '%Offer Accepted%' then 'Offer Accepted'
     else NULL end
     as analyticalStatus
from FlagResumbittedCasesTable
) b
) d where AnalyticalStatus is not null and usefulStep = 1
),

-- remove bad records before resubmitting -----------------------------
CleanedResumbittedCasesTable AS (
select distinct d.*
from 
(
select candidateId, jobId, sumResubmittedFlag - 1 removeRecordsFlag from 
(
select *, 
    lag(analyticalStatus, 1) over (partition by candidateId, jobid order by sumResubmittedFlag, eventDate) lastStatus
    from DeduplicatedTable 
    --where (candidateId = '02d0da8f-5018-48a0-9fd7-41acd000afec' and jobid = 'f3c7c29d-cd2c-4f97-b17c-557f29c0e6ec')
    --where (candidateId = '00f25087-0475-485c-8396-9f4b7394208e' and jobid = '30dde34e-99e8-4bcd-9c82-a3528db3f308')
    --where (candidateId = '07c57a82-07ad-4144-b8ad-09d8e5ec6d3b' and jobid = '30dde34e-99e8-4bcd-9c82-a3528db3f308')

) a
where analyticalStatus like '%REJOIN%' and (lastStatus not like '%WITHDRAWN%' AND lastStatus not like '%REJECTED%' AND lastStatus not like '%TRANSFERRED%') 
) c
JOIN
DeduplicatedTable d
on c.candidateId = d.candidateid and c.jobId = d.jobId and d.sumResubmittedFlag != c.removeRecordsFlag 
),

----------- add all data 
CandidatesA AS
(select distinct concat(candidateId, jobid) uniqueId from DeduplicatedTable),
CandidatesB AS
(select distinct concat(candidateId, jobid) uniqueId from CleanedResumbittedCasesTable),

UnresumbmittedCandidatesTable AS (
select c.* from
(select a.* from CandidatesA a where uniqueId not in (select distinct uniqueId from CandidatesB)) b
left JOIN
(select *, concat(candidateId, jobid) uniqueId from DeduplicatedTable) c
on
c.uniqueId = b.uniqueId
), 

CleanedValidEvents AS (
select *, sumResubmittedFlag joinId from UnresumbmittedCandidatesTable
union all
select *, concat(candidateId, jobid) uniqueId, sumResubmittedFlag joinId from CleanedResumbittedCasesTable
)


-- select distinct concat(candidateId, jobid) uniqueId from CleanedResumbittedCasesTable 
-- select distinct concat(candidateId, jobid) uniqueId from DeduplicatedTable 
-- select count(*) from DeduplicatedTable; 
-- select count(*) from CleanedResumbittedCasesTable; 

-- IF EXISTS(SELECT * FROM dbo.ValidEvents) DROP TABLE dbo.CleanedValidEvents
SELECT * INTO dbo.CleanedValidEvents FROM CleanedValidEvents

select * from CleanedValidEvents     
-- where (candidateId = '02d0da8f-5018-48a0-9fd7-41acd000afec' and jobid = 'f3c7c29d-cd2c-4f97-b17c-557f29c0e6ec')
-- where (candidateId = '00f25087-0475-485c-8396-9f4b7394208e' and jobid = '30dde34e-99e8-4bcd-9c82-a3528db3f308')
-- where (candidateId = '07c57a82-07ad-4144-b8ad-09d8e5ec6d3b' and jobid = '30dde34e-99e8-4bcd-9c82-a3528db3f308')
-- order by eventDate