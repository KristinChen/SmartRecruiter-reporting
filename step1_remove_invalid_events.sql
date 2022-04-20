--select count(distinct eventid) from dbo.ApplicationEvents_Merged --25166
--select count(*) from dbo.ApplicationEvents_Merged --25166
--select count(distinct concat(candidateId, jobid)) from dbo.ApplicationEvents_Merged --9684
--select count(distinct concat(candidateId, jobid)) from dbo.ApplicationEvents_Merged where eventType like '%application created%' --9684
--select count(distinct concat(candidateId, jobid)) from dbo.ApplicationEvents_Merged where eventType like '%application re-submitted%' --526

-- select distinct eventtype, applicationstatus, applicationsubstatus from dbo.ApplicationEvents_Merged order by applicationStatus; 

-- Remove invalid events 
with validCases as (
    select * from
    (
    select *,
    case when applicationStatus like'%in_review%' and applicationSubStatus IS NULL then 1
    when applicationStatus like '%interview%' and applicationSubStatus IS NULL then 1
    when applicationStatus like '%application fields updated%' and applicationSubStatus like '%LEAD%' then 1
    when applicationStatus like '%NEW%' and (eventType not like '%application created%' and eventType not like '%application re-submitted%') then 1
    when applicationStatus like '%LEAD%' and (eventType not like '%application created%' and eventType not like '%application re-submitted%') then 1

    else 0 end invalidFlag
    from dbo.ApplicationEvents_Merged
    ) a
    where invalidFlag = 0
)
-- IF EXISTS(SELECT * FROM dbo.ValidEvents) DROP TABLE dbo.ValidEvents
SELECT * INTO dbo.ValidEvents FROM validCases;
-- select distinct eventtype, applicationstatus, applicationsubstatus from dbo.ValidEvents order by applicationStatus

-- ????? skillset, tableau assessment, sql? 
-- select * from ApplicationEvents_Merged where candidateId = 'a1988f59-376f-4655-b5b2-407a724d7709' and jobid = 'f3c7c29d-cd2c-4f97-b17c-557f29c0e6ec' order by EventDate
-- select * from ApplicationEvents_Merged where candidateId = 'c0010a7c-36cf-4487-88eb-0f0be4aaa159' and jobid = '30dde34e-99e8-4bcd-9c82-a3528db3f308' order by EventDate
-- select * from ApplicationEvents_Merged where candidateId = '9f34aa71-1fd3-417b-a079-b73e64e31290' and jobid = 'f3c7c29d-cd2c-4f97-b17c-557f29c0e6ec' order by EventDate

