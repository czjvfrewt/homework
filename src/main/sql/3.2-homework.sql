--1
select team from (
select team,(year-row_number() over(PARTITION by team order by year)) gid
from t1 where team is not null and team <> '') a 
group by gid,team
having count(*)  > 2 


--2
select id,time,price,feature
from 
(select id,
case when ((price - lag(price) over(partition by id order by time))>0 and (price - lead(price) over(partition by id order by time))<0) then '波峰'
     when ((price - lag(price) over(partition by id order by time))<0 and (price - lead(price) over(partition by id order by time))>0) then '波谷' end feature,
time,
price
from t2) a 
where 
feature is not null;

--3.1
select  
id,
(max(unix_timestamp(dt,'yyyy/MM/dd HH:mm'))-min(unix_timestamp(dt,'yyyy/MM/dd HH:mm')))/60 time, 
count(1) nums
from t3
group by id ;

--3.2
select id,max(dt)-min(dt),count(1) from(
select id,dt,sum(type) over(partition by id order by dt) mark
from (
select  
  id,
  unix_timestamp(dt,'yyyy/MM/dd HH:mm')/60 dt,
  if((unix_timestamp(dt,'yyyy/MM/dd HH:mm') - unix_timestamp(lag(dt) over(partition by id order by dt),'yyyy/MM/dd HH:mm'))/60<30,0,1) type
from t3) a
) t
group by id,mark



