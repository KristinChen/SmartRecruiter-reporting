# SmartRecruiter-reporting

## Total: 7342 applicants: 4527  + 44  + 509 + 2262 
```console select count(distinct concat(joinId, candidateId, jobId)) from CleanedValidEvents where (jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering') --total: 7342```

## Out from funnel actively*: 4527 applicants
```
select count(distinct uniqueId) from ActiveOutData where (jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering') --active out: 4527
```

## Out/Rejected from funnel right the way*: 44 applicants
```
select applicationStatus, count(distinct uniqueId)  from InFunnelData where eventType like '%Application Created%' and (applicationStatus not like '%new%' and applicationStatus not like '%lead%') and (jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering') group by applicationStatus--44: rejected right the way
```

## In funnel actively making progress: 509 Applicants
```
select count(distinct uniqueId) from ActiveInFunnelData where (jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering')--active in funnel 509
```

## In funnel inactively: 2262 Applicants
```console
select count(distinct uniqueId) from InFunnelData 
where uniqueId not in (select distinct uniqueId InFunnelData where eventType like '%Application Created%' and (applicationStatus not like '%new%' and applicationStatus not like '%lead%') and (jobCapability = 'Business Intelligence' or jobCapability = 'Data Science' or jobCapability = 'Data Engineering')) --rejected right the way
and 
uniqueId not in (select distinct uniqueId from ActiveInFunnelData) --inactive in funnel: 2262
```