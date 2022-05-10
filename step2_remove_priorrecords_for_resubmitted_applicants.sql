-- before clean -----------------------------------
-- good resubmitted example
-- select distinct eventId, candidateId, jobId, jobLevel, jobCapability, jobLocation, eventDate, EventType, ApplicationStatus, applicationSubStatus from ApplicationEvents_Merged where candidateid = '0067828a-1eb5-4a3e-9afa-330b72f30bb2' and jobid = 'ace13ce4-eb17-4094-8576-39c821029a90' order by eventDate; 
-- bad resubmitted example
-- select distinct eventId, candidateId, jobId, jobLevel, jobCapability, jobLocation, eventDate, EventType, ApplicationStatus, applicationSubStatus from ApplicationEvents_Merged where candidateid = '02d0da8f-5018-48a0-9fd7-41acd000afec' order by eventDate; 
-- duplicate events
-- select distinct eventId, candidateId, jobId, jobLevel, jobCapability, jobLocation, eventDate, EventType, ApplicationStatus, applicationSubStatus from ApplicationEvents_Merged where candidateid = '0203df3c-4a3b-43c9-aaca-2501bf49dc28' and jobid = 'b0471062-acb5-474e-be73-cce25b9726dd' order by eventDate

-- after clean -----------------------------------
-- good resubmitted example - after clean
-- select distinct eventId, candidateId, jobId, joinId, jobLevel, jobCapability, jobLocation, eventDate, EventType, ApplicationStatus, applicationSubStatus from deduplicatedValidEvents where candidateid = '0067828a-1eb5-4a3e-9afa-330b72f30bb2' and jobid = 'ace13ce4-eb17-4094-8576-39c821029a90' order by eventDate; 

-- bad resubmitted example -- after clean
-- select distinct eventId, candidateId, jobId, joinId, jobLevel, jobCapability, jobLocation, eventDate, EventType, ApplicationStatus, applicationSubStatus from deduplicatedValidEvents where candidateid = '02d0da8f-5018-48a0-9fd7-41acd000afec' order by eventDate; 

-- deduplicate
-- select distinct eventId, candidateId, jobId, joinId, jobLevel, jobCapability, jobLocation, eventDate, EventType, ApplicationStatus, applicationSubStatus from deduplicatedValidEvents where candidateid = '0203df3c-4a3b-43c9-aaca-2501bf49dc28' and jobid = 'b0471062-acb5-474e-be73-cce25b9726dd' order by eventDate


With deDuplicatedNewData AS (
select distinct * from
(
select *,
    max(sumNewFlag) over (partition by candidateId, jobId) as maxNewFlag
    -- max(sumUpdatedRejectionFlag) over (partition by candidateId, jobId) as maxUpdatedRejectionFlag,
    -- max(sumUpdatedHiredFlag) over (partition by candidateId, jobId) as maxUpdatedHiredFlag,
    -- max(sumUpdatedWithdrawnFlag) over (partition by candidateId, jobId) as maxUpdatedWithdrawnFlag,
    -- max(sumUpdatedTransferredFlag) over (partition by candidateId, jobId) as maxUpdatedTransferredFlag,
    -- min(sumNewFlag) over (partition by candidateId, jobId) as minNewFlag,
    -- min(sumUpdatedRejectionFlag) over (partition by candidateId, jobId) as minUpdatedRejectionFlag,
    -- min(sumUpdatedHiredFlag) over (partition by candidateId, jobId) as minUpdatedHiredFlag,
    -- min(sumUpdatedWithdrawnFlag) over (partition by candidateId, jobId) as minUpdatedWithdrawnFlag,
    -- min(sumUpdatedTransferredFlag) over (partition by candidateId, jobId) as minUpdatedTransferredFlag
from 
(
select *,
    SUM(newFlag) OVER (PARTITION BY candidateId, jobId order by eventDate) as sumNewFlag
    -- SUM(updatedRejectionFlag) OVER (PARTITION BY candidateId, jobId order by eventDate) sumUpdatedRejectionFlag,
    -- SUM(updatedHiredFlag) OVER (PARTITION BY candidateId, jobId order by eventDate) sumUpdatedHiredFlag,
    -- SUM(updatedWithdrawnFlag) OVER (PARTITION BY candidateId, jobId order by eventDate) sumUpdatedWithdrawnFlag,
    -- SUM(updatedTransferredFlag) OVER (PARTITION BY candidateId, jobId order by eventDate) sumUpdatedTransferredFlag
FROM
(select *, 
        case when eventtype like '%application created%' and applicationStatus like '%new%' then 1 else 0 end newFlag
        -- case when eventtype like '%application status updated%' and applicationStatus like '%rejected%' then 1 else 0 end updatedRejectionFlag,
        -- case when eventtype like '%application status updated%' and applicationStatus like '%hired%' then 1 else 0 end updatedHiredFlag, 
        -- case when eventtype like '%application status updated%' and applicationStatus like '%withdrawn%' then 1 else 0 end updatedWithdrawnFlag,
        -- case when eventtype like '%application status updated%' and applicationStatus like '%transferred%' then 1 else 0 end updatedTransferredFlag  
from ValidEvents
) a
) b
) c
where sumNewFlag = maxNewFlag --!= 229; = 26618
)

,ResubmittedCases as
(
select *, max(sumStartFlag) over (partition by candidateId, jobId) as maxStartFlag from 
(
select *,
    SUM(startFlag) OVER (PARTITION BY candidateId, jobId order by eventDate) as sumStartFlag, 
    SUM(outFlag) OVER (PARTITION BY candidateId, jobId order by eventDate) sumOutFlag
FROM
(select *, 
        case when funnel = 'JOIN' or funnel = 'REJOIN' then 1 else 0 end startFlag,
        case when funnel = 'OUT' then 1 else 0 end outFlag
from
(
select 
*,
case when eventType like '%Application Re-Submitted%' then 'REJOIN' 
    when eventType like '%Application Created%' then 'JOIN' 
    when applicationStatus = 'HIRED' or applicationStatus = 'TRANSFERRED' or applicationStatus = 'WITHDRAWN' or applicationStatus = 'REJECTED' then 'OUT' 
else NULL 
end funnel from deDuplicatedNewData
) a
) b
) c
),

ClosedResubmittedCases as 
(
select v.*, v.sumStartFlag joinId from 
(
select distinct candidateId, jobId, sumStartFlag from ResubmittedCases where funnel like 'REJOIN' and sumOutFlag = sumStartFlag - 1
) b
left join 
resubmittedCases v
on v.candidateId = b.candidateId and v.jobId = b.jobid and v.sumStartFlag < b.sumStartFlag
),

CleanedUnclosedResubmittedCases AS 
(
select v.*, v.sumStartFlag joinId from 
(
select distinct candidateId, jobId, sumStartFlag from ResubmittedCases where funnel like 'REJOIN' and sumOutFlag != sumStartFlag - 1
) b
inner join resubmittedCases v
on v.candidateId = b.candidateId and v.jobId = b.jobid and v.sumStartFlag >= b.sumStartFlag
),

UnResumbittedCases AS (
select v.*, sumStartFlag joinId from
(
select distinct a.candidateId, a.jobid from ResubmittedCases a  --un-resubmitted cases
left join
(select distinct candidateid, jobid from ResubmittedCases where eventtype like '%application re-submitted%') b
on a.candidateId = b.candidateId and a.jobid = b.jobid
where b.candidateId is NULL and b.jobid is NULL
) c
left join 
resubmittedCases v
on v.candidateId = c.candidateId and v.jobId = c.jobid
),

-- union ---------------------------------------
cleanedTable AS 
(
select eventId, candidateId, jobId, joinId, jobLevel, jobCapability, jobLocation, eventDate, eventType, applicationStatus, applicationSubStatus
, funnel,startFlag, outFlag, sumStartFlag, sumOutFlag, maxStartFlag 
from 
(
select * from ClosedResubmittedCases
UNION
select * from CleanedUnclosedResubmittedCases
UNION
select * from UnResumbittedCases 
) tot 
),
deduplicatedValidEvents AS (
select * from 
(select *, ROW_NUMBER() over (partition by candidateId, jobid, joinId, joblevel, jobCapability, jobLocation, applicationStatus, eventType, applicationSubStatus order by eventDate) uniqueStep from cleanedTable) a
where uniqueStep = 1
)

-- IF EXISTS(SELECT * FROM dbo.CleanedValidEvents) DROP TABLE dbo.CleanedValidEvents
SELECT * INTO dbo.CleanedValidEvents FROM deduplicatedValidEvents;


