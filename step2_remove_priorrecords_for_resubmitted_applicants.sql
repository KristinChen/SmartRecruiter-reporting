-- example has reached out OUT before resubmitting
-- select candidateId, jobId, applicationId, eventId, eventDate, eventType, applicationStatus, applicationSubStatus, jobCapability, jobLevel, jobLocation from dbo.ValidEvents where candidateid = '0ab7b172-31ae-4197-ba3b-cf4f8678cca6' and jobid = 'ace13ce4-eb17-4094-8576-39c821029a90' order by eventDate;

-- example haven't reached out OUT before resubmitting
-- select candidateId, jobId, applicationId, eventId, eventDate, eventType, applicationStatus, applicationSubStatus, jobCapability, jobLevel, jobLocation from dbo.ValidEvents where candidateid = '38d0354c-cc93-4f65-b470-adc52b7bad12' and jobid = '30dde34e-99e8-4bcd-9c82-a3528db3f308' order by eventDate;
-- select candidateId, jobId, applicationId, eventId, eventDate, eventType, applicationStatus, applicationSubStatus, jobCapability, jobLevel, jobLocation from dbo.ValidEvents where candidateid = '0067828a-1eb5-4a3e-9afa-330b72f30bb2' and jobid = 'ace13ce4-eb17-4094-8576-39c821029a90' order by eventDate;
-- select candidateId, jobId, applicationId, eventId, eventDate, eventType, applicationStatus, applicationSubStatus, jobCapability, jobLevel, jobLocation from dbo.ValidEvents where candidateid = '46b22810-8557-4e94-8156-d22cb1b60594' and jobid = '305e3700-8f54-4499-9d3e-2f0ddfbed930' order by eventDate;
 
with resubmitted_case as
(
select *, row_number() over (PARTITION BY candidateId, jobid order by eventDate) as step
FROM
(select *, case when funnel = 'JOIN' or funnel = 'REJOIN' then 1 else 0 end startFlag
          from
(
select 
*,
case when eventType like '%Application Re-Submitted%' then 'REJOIN' 
    when eventType like '%Application Created%' then 'JOIN' 
    when applicationStatus = 'HIRED' or applicationStatus = 'TRANSFERRED' or applicationStatus = 'WITHDRAWN' or applicationStatus = 'REJECTED' then 'OUT' 
else NULL 
end funnel from dbo.ValidEvents
) a) b),

-- remove prior records before resubmitting, ensuring every applicant has ONE and ONLY ONE starting point
cleaned_cases as (
select r.*, row_number() over (PARTITION BY r.candidateId, r.jobid order by r.eventDate) as cleanStep, maxStartFlag from resubmitted_case r
inner JOIN
(
    select candidateid, jobid, max(step) as maxStartFlag from resubmitted_case where startFlag = 1 group by candidateId, jobId
) max_vle
on r.candidateId = max_vle.candidateId and r.jobId = max_vle.jobId
and r.step >= maxStartFlag
)
-- IF EXISTS(SELECT * FROM dbo.CleanedValidEvents) DROP TABLE dbo.CleanedValidEvents
SELECT * INTO dbo.CleanedValidEvents FROM cleaned_cases;

-- select count(distinct concat(candidateid, jobid)) from dbo.ValidEvents; --9684
-- select count(distinct concat(candidateid, jobid)) from dbo.CleanedValidEvents; --9684
-- select funnel, count(distinct concat(candidateid, jobid)) from dbo.CleanedValidEvents group by funnel; --join: 9158 + rejoin: 526 = 9684
