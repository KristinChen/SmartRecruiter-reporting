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

## Data Understanding (up to 05/10/2022)

The output data includes:
- `ValidEvents`: after step 1 listed above
- `CleanedValidEvents`: after step 1, 2, and 3 listed above
- `OutData`: history of applicants reached to closure
- `InfunnelData`: history of applicants haven't reached to closure
- `ActivelyInFunnelData`: history of applicants actively making progresses in process

### Total: 7366 applicants: 5036 + 520 + 1810 
```console 
select count(distinct concat(joinId, candidateId, jobId)) from CleanedValidEvents where (jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering') 
```
### Out from funnel: 5036 applicants
```console
select count(distinct uniqueId) from OutData
```

### In funnel actively making progress: 520 Applicants
```console
select count(distinct uniqueId) from ActiveInFunnelData
```

### In funnel inactively: 1810 Applicants
```console
select count(distinct uniqueId) from InFunnelData where uniqueId not in (select uniqueId from ActiveInFunnelData)
```

## Deliverables 

### `OutConversionRateTable`: conversion rate for out applicants

```console
-- 5064
select sum(numapplicants) from OutConversionRateTable where applicationStatus like '%rejected%' or applicationStatus like '%transferred%' or applicationStatus like '%withdrawn%' or applicationStatus like '%hired%' --one applicants could got transferred and then got rejected
-- 5036
select count(distinct uniqueId) from OutData
```

### `ActiveInFunnelConversionRateTable`: conversion rate for actively in funnel applicants

```console
--520
select sum(numapplicants) from ActiveInFunnelConversionRateTable where applicationStatus like '%new%'
--520
select count(distinct uniqueId) from activeInFunnelData
```

### `WithdrawnRateTable`: from which funnel the applicant withdrew 
```console
select count(distinct concat(candidateId, jobId, joinId)) from CleanedValidEvents where applicationStatus like '%WITHDRAWN%' and (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%'); --184

select count(distinct concat(candidateId, jobId, joinId)) from outData where applicationStatus like '%WITHDRAWN%' and (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%'); --184

select sum(numApplicants) from WithdrawnRateTable; --184
```

### `RejectionRateTable`: from which funnel the applicant got rejected 
```console
select count(distinct concat(candidateId, jobId, joinId)) from CleanedValidEvents where applicationStatus like '%REJECTED%' and (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%'); 

select count(distinct uniqueid) from outdata where applicationStatus like '%REJECTED%'
select sum(numApplicants) from RejectionRateTable
```

### `ActiveInFunnelLastStatusTable`: active applicants currently at which funnel 
```console
select count(distinct uniqueId) from ActiveInFunnelData; 
select sum(numApplicants) from ActiveInFunnelLastStatusTable; 
```


