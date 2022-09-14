--Создаем таблицу
CREATE TABLE student43.user_dim (
	user_key int8 NULL,
	user_id varchar(40) NULL,
	first_name varchar(10) NULL,
	last_name varchar(10) NULL,
	address varchar(100) NULL,
	zipcode varchar(10) NULL,
	date_from date NULL,
	date_to date NULL,
	created_date timestamp NULL,
	updated_date timestamp NULL
)
DISTRIBUTED BY (user_id);

--Вставка данных
INSERT INTO student43.user_dim (user_key,user_id,first_name,last_name,address,zipcode,date_from,date_to,created_date,updated_date) VALUES
	 (1000,'b0cc9fde-a29a-498e-824f-e52399991beb','john','doe','world','10027','1990-01-15','1990-03-01','1900-01-15','1900-01-15 10:00:00.000'),
	 (1100,'b0cc9fde-a29a-498e-824f-e52399991beb','john','doe','world','1002777','1990-01-15','1990-03-01','1900-01-14','1900-01-15 00:00:00.000'),
	 (1200,'b0cc9fde-a29a-498e-824f-e52399991beb','john','doe','world','10012','1990-01-01','1990-02-01','1900-01-01','1900-01-01 00:00:00.000'),
	 (1300,'x4d93qe-v12z-745b-098y-w12387652gun','clara','fluff','world','111111','1990-01-01','1990-02-01','1900-01-01','1900-01-01 00:00:00.000'),
	 (1400,'x4d93qe-v12z-745b-098y-w12387652gun','clara','fluff','world','222222','1990-01-15','1990-02-01','1900-01-15','1900-01-15 00:00:00.000'),
	 (1500,'x4d93qe-v12z-745b-098y-w12387652gun','clara','fluff','world','333333','1990-02-01','1990-03-01','1900-02-01','1900-02-01 00:00:00.000');
	 
	 
--Создание витрины	
	
CREATE TABLE student43.user_dm (
	user_id varchar(40) NULL,
	first_name varchar(10) NULL,
	last_name varchar(10) NULL,
	address varchar(100) NULL,
	zipcode varchar(10) NULL,
	updated_date date NULL
)
DISTRIBUTED BY (user_id);

--Выборка последних записей по клиенту
select user_id,
	first_name,
	last_name,
	address,
	zipcode,
	now () as updated_date
from (select *,  rank () over(partition by user_id order by date_to desc, updated_date desc)
from user_dim) user_dim
where rank = 1;


--+первая и последняя запись, даты с и по не изменяются
with rank_tmp as (
select 
	user_id,
	first_name,
	last_name,
	address,
	zipcode,
	date_from,
	date_to,
	rank () over(partition by user_id order by date_to asc, updated_date asc
	)
from user_dim
)
, new_dates as (
select user_id,
	first_name,
	last_name,
	address,
	zipcode,
	date_from,
	date_to,
	rank,
	case 
		when rank = 1 then '1900-01-01' 
		when rank = (select max(rank) from rank_tmp) then date_to +1
	end as date_from_new,
	case 
		when rank = 1 then date_from - 1
		when rank = (select max(rank) from rank_tmp) then '9999-12-31' 
	end as date_to_new
from rank_tmp)
select 
	user_id,
	first_name,
	last_name,
	address,
	zipcode,
	date_from,
	date_to,
	now() as upd_dttm
from rank_tmp
union 
select 
	user_id,
	first_name,
	last_name,
	address,
	zipcode,
	date_from_new as date_from,
	date_to_new as date_to,
	now() as upd_dttm
from new_dates
where rank in ((select max(rank) from rank_tmp), 1)
order by user_id, date_from

--+первая и последняя запись, даты с и по изменяются
with rank_tmp as (
select 
	user_id,
	first_name,
	last_name,
	address,
	zipcode,
	date_from,
	date_to,
	rank () over(partition by user_id order by date_to asc, updated_date asc),
	lead(date_from) over (order by date_from) -1 as prev_date
from user_dim
)
, new_dates as (
select user_id,
	first_name,
	last_name,
	address,
	zipcode,
	date_from,
	date_to,
	rank,
	case 
		when rank = 1 then '1900-01-01' 
		when rank = (select max(rank) from rank_tmp) then date_to +1
	end as date_from_new,
	case 
		when rank = 1 then date_from - 1
		when rank = (select max(rank) from rank_tmp) then '9999-12-31' 
	end as date_to_new
from rank_tmp)
select 
	user_id,
	first_name,
	last_name,
	address,
	zipcode,
	date_from,
	case when prev_date is null then date_to else prev_date end as date_to
	from rank_tmp
union 
select 
	user_id,
	first_name,
	last_name,
	address,
	zipcode,
	date_from_new as date_from,
	date_to_new as date_to
from new_dates
where rank in ((select max(rank) from rank_tmp), 1)
order by user_id, date_from 


