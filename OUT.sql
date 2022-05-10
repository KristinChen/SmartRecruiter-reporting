select * from OutConversionRateTable
select * from ActiveInFunnelConversionRateTable
select * from WithdrawnRateTable; --176 
select * from RejectionRateTable --4052 (include intern and remote applicants)
select *  from ActiveInFunnelLastStatusTable; --503

select distinct step from outData where (funnel like '%join%' or funnel like '%rejoin%') --2, 3?

select distinct uniqueId from outData where (funnel like '%join%' or funnel like '%rejoin%') and step = 1


select * from outData where uniqueId not in 
(select distinct uniqueId from outData where (funnel like '%join%' or funnel like '%rejoin%') and step = 1)
order by uniqueId, eventDate; 

---------------------------- duplicate rejection records (rejection rate is wrong; out conversion rate is confusing) -----------------------------------
select * from outData where uniqueId in 
(
select * from 
(
select *, sum(QAoutFlag) over (partition by uniqueId) QASumOutFlag, sum(QARejectionFlag) over (partition by uniqueId) QASumRejectionFlag from 
(select *, case when funnel like '%out%' then 1 else 0 end QAoutFlag, case when applicationStatus like '%rejected%' then 1 else 0 end QARejectionFlag from outData) a 
) b
where QASumOutFlag > 0 and QASumRejectionFlag > 1 and step = maxstep - 2 and maxstep != 1 --duplicated rejected
) c and step = maxstep - 2
order by uniqueId, eventDate --if its rejected, then means you have to go upper
