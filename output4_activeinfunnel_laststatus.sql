-- report 1 -----------------------------------
SELECT jobCapability, jobLocation, experiencedFlag, analyticalStatus, count(distinct uniqueId) as numApplicants from
(
SELECT c.*
FROM ActiveInFunnelData c where Step = maxStep
) b
group by jobCapability, jobLocation, experiencedFlag, analyticalStatus
