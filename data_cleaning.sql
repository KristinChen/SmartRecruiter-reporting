-- 1. Clean job capability
-- select distinct candidateId, jobId, jobCapability from ApplicationEvents_Merged where jobCapability is NULL

-- 2. remove invalid events (table: ValidEvents)
-- select eventType, applicationStatus, applicationSubStatus from ApplicationEvents_Merged group by eventType, applicationStatus, applicationSubStatus order by applicationStatus

-- 3. remove prior unclosure history before re-submitting
-- good one: 
-- select * from ValidEvents where candidateid = '0067828a-1eb5-4a3e-9afa-330b72f30bb2' order by eventDate; 

-- bad one:
-- select * from ValidEvents where candidateid = '02d0da8f-5018-48a0-9fd7-41acd000afec'; 

-- 4. assign `joinId` to identify the # of join for resubmitted applicants (table: CleanedValidEvents)
select * from CleanedValidEvents where candidateid = '0067828a-1eb5-4a3e-9afa-330b72f30bb2' order by eventDate; 
select * from CleanedValidEvents where candidateid = '02d0da8f-5018-48a0-9fd7-41acd000afec' order by eventDate; 

-- 5. remove records with same status but different eventId and eventDate (haven't done on my end)
select * from CleanedValidEvents  where candidateid = '001baac4-baec-40ca-aac6-c9c8cecd477c' order by eventDate; 