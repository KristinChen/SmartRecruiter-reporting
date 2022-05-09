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

CleanedStatusTable as 
(
select distinct eventId, candidateId, jobId, jobLevel, jobCapability, jobLocation, eventDate, cleanedEventType, cleanedApplicationStatus, applicationSubStatus from 
(
    select
    *,
    case when eventType like '%application created%' and applicationStatus like '%lead%' then 'NEW' 
         when eventType like '%application re-submitted%' and applicationStatus like '%lead%' then 'NEW' 
         when applicationStatus like '%interview%' then 'IN_REVIEW' 

    else applicationStatus end cleanedApplicationStatus,
    
    case when eventType like '%application fields updated%' or eventType like '%application source updated%' or eventType like '%application screening answers updated%' then 'Application Status Updated'

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
    when eventType like '%application status updated%' and applicationStatus like '%LEAD%' then 1
    when eventType like '%application status updated%' and applicationStatus like '%NEW%' then 1

    else 0 end invalidFlag from 
    (
        select distinct eventId, candidateId, jobId, jobLevel, jobCapability, jobLocation, eventDate, cleanedApplicationStatus applicationStatus, cleanedEventType eventType, applicationSubStatus from CleanedStatusTable
    )  a
    )  b
    where invalidFlag = 0
)

-- IF EXISTS(SELECT * FROM dbo.ValidEvents) DROP TABLE dbo.ValidEvents
SELECT * INTO dbo.ValidEvents FROM validCases;

