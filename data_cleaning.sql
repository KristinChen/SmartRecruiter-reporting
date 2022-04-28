-- 1. Clean job capability
-- select * from ApplicationEvents_Merged where jobCapability is NULL 

-- 2. remove invalid events (table #1: ValidEvents; code: step1)
-- select eventType, applicationStatus, applicationSubStatus from ApplicationEvents_Merged group by eventType, applicationStatus, applicationSubStatus order by applicationStatus

-- 3. remove prior unclosure history before re-submitting
-- good one: 
-- select * from ValidEvents where candidateid = '0067828a-1eb5-4a3e-9afa-330b72f30bb2' and jobid = 'ace13ce4-eb17-4094-8576-39c821029a90' order by eventDate; 

-- bad one:
-- select * from ValidEvents where candidateid = '02d0da8f-5018-48a0-9fd7-41acd000afec' order by eventdate; 

-- 4. assign `joinId` to identify the # of join for resubmitted applicants (table 2: CleanedValidEvents; code: step2) --handle by the analysis team
select * from CleanedValidEvents where candidateid = '0067828a-1eb5-4a3e-9afa-330b72f30bb2' order by eventDate; 
select * from CleanedValidEvents where candidateid = '02d0da8f-5018-48a0-9fd7-41acd000afec' order by eventDate; 

-- 5. remove records with same status but different eventId and eventDate 
select * from ApplicationEvents_Merged  where candidateid = '001baac4-baec-40ca-aac6-c9c8cecd477c' order by eventDate; 