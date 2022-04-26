-- each applicant is identified by `candidateId` and `jobId`
-- each applicant join/rejoin once; and exit once

-- Clean job capability
with CleanedJobCapabilityTable as 
(
select distinct tot.eventId, tot.candidateId, tot.jobId, tot.jobLevel, c.cleanedJobCapability jobCapability, tot.jobLocation, tot.eventDate, tot.eventType, tot.applicationStatus, tot.applicationSubStatus from
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
),

-- Remove invalid events 
-- good example: select * from CleanedJobCapabilityTable where candidateid = '001baac4-baec-40ca-aac6-c9c8cecd477c' order by eventDate; 

CleanedStatusTable as 
(
select distinct eventId, candidateId, jobId, jobLevel, jobCapability, jobLocation, eventDate, cleanedEventType, cleanedApplicationStatus, applicationSubStatus from 
(
    select
    *,
    case when eventType like '%application created%' and applicationStatus like '%lead%' then 'NEW' 
        when eventType like '%application re-submitted%' and applicationStatus like '%lead%' then 'NEW' 
    else applicationStatus end cleanedApplicationStatus,
    case when applicationStatus like '%hired%' then 'Application Status Updated'
        when applicationStatus like '%rejected%' and eventType not like '%application created%' then 'Application Status Updated'
        when applicationStatus like '%offered%' and applicationSubStatus like '%offer pending%' then 'Application Status Updated'
        when applicationStatus like '%transferred%' then 'Application Status Updated'
        when applicationStatus like '%withdrawn%' then 'Application Status Updated'
    else eventType end cleanedEventType
    from CleanedJobCapabilityTable
) a 
),
validCases as (
    select distinct * from
    (
    select *,
    case when applicationStatus like'%in_review%' and applicationSubStatus IS NULL then 1
    when applicationStatus like '%interview%' and applicationSubStatus IS NULL then 1
    when applicationStatus like '%application fields updated%' and applicationSubStatus like '%LEAD%' then 1
    when applicationStatus like '%NEW%' and (eventType not like '%application created%' and eventType not like '%application re-submitted%') then 1
    when applicationStatus like '%LEAD%' and (eventType not like '%application created%' and eventType not like '%application re-submitted%') then 1
    else 0 end invalidFlag from 
    (
        select distinct eventId, candidateId, jobId, jobLevel, jobCapability, jobLocation, eventDate, cleanedApplicationStatus applicationStatus, cleanedEventType eventType, applicationSubStatus from CleanedStatusTable
    )  a
    )  b
    where invalidFlag = 0
)

-- Remove dupuplicate status records caused by different eventDate and eventType; 
-- good example: select * from validCases where candidateid = '001baac4-baec-40ca-aac6-c9c8cecd477c' order by eventDate; 
-- select * from validCases where candidateid = '001baac4-baec-40ca-aac6-c9c8cecd477c' order by eventDate; 

-- IF EXISTS(SELECT * FROM dbo.ValidEvents) DROP TABLE dbo.ValidEvents
SELECT * INTO dbo.ValidEvents FROM validCases;