SELECT jobCapability, jobLocation, experiencedFlag, eventType, applicationStatus, applicationSubStatus, count(distinct uniqueId) as numApplicants from
(
SELECT c.*, 
    case when c.jobLevel not like '%all-star%' and c.jobLevel not like '%intern%' then 1 
         when c.jobLevel like '%all-star%' then 0 else NULL end as experiencedFlag
FROM ActiveInFunnelData c where Step = maxStep
) b
group by jobCapability, jobLocation, experiencedFlag, eventType, applicationStatus, applicationSubStatus; 

--518
select count(distinct uniqueId) from ActiveInFunnelData