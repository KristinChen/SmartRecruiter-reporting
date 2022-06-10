# SmartRecruiter-reporting

## Data Cleaning
This repo contains queries to clean recruiting data and produce reports. 

The preprocess process includes:
- step 1. clean status and remove invalid events that are not for analyses. The valid funnel is shown in `valid_funnel_viz.png`.
- step 2. remove history of unclosed re-submitted applicants

```console
-- good resubmitted example
select distinct eventId, candidateId, jobId, jobLevel, jobCapability, jobLocation, eventDate, EventType, ApplicationStatus, applicationSubStatus from ApplicationEvents_Merged where candidateid = '0067828a-1eb5-4a3e-9afa-330b72f30bb2' and jobid = 'ace13ce4-eb17-4094-8576-39c821029a90' order by eventDate; 
-- bad resubmitted example
select distinct eventId, candidateId, jobId, jobLevel, jobCapability, jobLocation, eventDate, EventType, ApplicationStatus, applicationSubStatus from ApplicationEvents_Merged where candidateid = '02d0da8f-5018-48a0-9fd7-41acd000afec' order by eventDate; 

```
- step 3. remove duplicated events

```console
-- duplicate events
-- select distinct eventId, candidateId, jobId, jobLevel, jobCapability, jobLocation, eventDate, EventType, ApplicationStatus, applicationSubStatus from ApplicationEvents_Merged where candidateid = '0203df3c-4a3b-43c9-aaca-2501bf49dc28' and jobid = 'b0471062-acb5-474e-be73-cce25b9726dd' order by eventDate
```

## Data Understanding (up to 06/09/2022)

The output data includes:
- `CleanedValidEvents`: after step 1, 2, and 3 listed above
- `OutData`: history of applicants reached to closure
- `InfunnelData`: history of applicants haven't reached to closure
- `ActivelyInFunnelData`: history of applicants actively making progresses in process

### Total: 6723 applicants: 4484 out + 2239 infunnel Applicants
```console 
select count(distinct uniqueid) from CleanedValidEvents where (jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering') 
```
### Out from funnel: 4484 applicants
```console
select count(distinct uniqueId) from OutData
```

### In funnel actively making progress: 440 applicants
```console
select count(distinct uniqueId) from ActiveInFunnelData
```

### In funnel (actively and inactively): 2239 applicants
```console
select count(distinct uniqueId) from InFunnelData; --2239
```
