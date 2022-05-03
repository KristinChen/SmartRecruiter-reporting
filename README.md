# SmartRecruiter-reporting

## 1. Data Understanding
### Total: 7342 applicants: 4897  + 503  + 1942  (include intern and remote applicants)
```console 
select count(distinct concat(joinId, candidateId, jobId)) from CleanedValidEvents where (jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering') --total: 7342
```
### Out from funnel: 4897 applicants
```console
select count(distinct uniqueId) from OutData
```

### In funnel actively making progress: 503 Applicants
```console
select count(distinct uniqueId) from InFunnelData where funnel is NULL --503 actively in funnel

select count(distinct uniqueId) from InFunnelData where funnel is NULL and jobLevel not like '%intern%' and joblocation not like  '%remote%' -- 483

```

### In funnel inactively: 1942 Applicants
```console
select count(distinct uniqueId) from InFunnelData where uniqueId not in (select uniqueId from InFunnelData where funnel is NULL) --1942
```

## 2. Deliverables
### `OutConversionRateTable`: conversion rate for out applicants

```console
select * OutConversionRateTable

select sum(numapplicants) from OutConversionRateTable where applicationAggStatus like '%rejected%' --4008 != 4052 (BUG)
```

### `ActiveInFunnelConversionRateTable`: conversion rate for actively in funnel applicants

```console
select * ActiveInFunnelConversionRateTable

select sum(numApplicants) from ActiveInFunnelConversionRateTable WHERE FUNNEL like '%join%' or funnel like '%rejoin%' -- 503
select sum(numApplicants) from ActiveInFunnelConversionRateTable WHERE (FUNNEL like '%join%' or funnel like '%rejoin%') and experiencedFlag is not null and jobLocation not like '%remote%' --483

```

### `WithdrawnRateTable`: from which funnel the applicant withdrew (include intern and remote applicants)
```console
select count(distinct concat(candidateId, jobId, joinId)) from CleanedValidEvents where applicationStatus like '%WITHDRAWN%' and (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%'); --176

select count(distinct concat(candidateId, jobId, joinId)) from outData where applicationStatus like '%WITHDRAWN%' and (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%'); --176

select sum(numApplicants) from WithdrawnRateTable; --176 
```

### `RejectionRateTable`: from which funnel the applicant got rejected (include intern and remote applicants)
```console
select count(distinct concat(candidateId, jobId, joinId)) from CleanedValidEvents where applicationStatus like '%REJECTED%' and (jobCapability like '%science%' or jobCapability like '%engineering%' or jobCapability like '%intelligence%'); --4052

select count(distinct uniqueid) from outdata where applicationStatus like '%REJECTED%' --4052
select sum(numApplicants) from RejectionRateTable --4052
```

### `ActiveInFunnelLastStatusTable`: active applicants currently at which funnel (include intern and remote applicants)
```
select count(distinct uniqueId) from ActiveInFunnelData; --503
select sum(numApplicants) from ActiveInFunnelLastStatusTable; --503
```


