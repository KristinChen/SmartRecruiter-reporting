-- select distinct candidateid, jobid from ValidEvents where eventtype like '%application re-submitted%' ---526

with ResubmittedCases as
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
end funnel from dbo.ValidEvents
) a
) b
) c
),

-- good ones: end the funnel before rejoinning
-- good resubmitted example: select * from ClosedResubmittedCases where candidateid = '0067828a-1eb5-4a3e-9afa-330b72f30bb2'; 
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

-- select * from ClosedResubmittedCases where candidateid = '0067828a-1eb5-4a3e-9afa-330b72f30bb2'; 

-- bad ones...remove ---------------------------
-- bad resubmitted example: select * from ResubmittedCases where candidateid = '02d0da8f-5018-48a0-9fd7-41acd000afec'; 

CleanedUnclosedResubmittedCases AS 
(
select v.*, v.sumStartFlag joinId from 
(
select distinct candidateId, jobId, sumStartFlag from ResubmittedCases where funnel like 'REJOIN' and sumOutFlag != sumStartFlag - 1
) b
inner join resubmittedCases v
on v.candidateId = b.candidateId and v.jobId = b.jobid and v.sumStartFlag >= b.sumStartFlag
),  

-- other cases
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
)
--distinct concat(candidateid, jobid): 9955; concat(candidateid, jobid, joinId): 10534
-- select * from ClosedResubmittedCases; 
--select * from CleanedUnclosedResubmittedCases; 
-- select * from UnResumbittedCases where joinId != 1; 
-- select * from UnResumbittedCases where candidateid = '001baac4-baec-40ca-aac6-c9c8cecd477c' --needed to be further clean; 

-- IF EXISTS(SELECT * FROM dbo.CleanedValidEvents) DROP TABLE dbo.CleanedValidEvents
SELECT * INTO dbo.CleanedValidEvents FROM cleanedTable;
