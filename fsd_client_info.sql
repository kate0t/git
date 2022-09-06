---Insert into dm.cust_org
with tax_registration as (
select cl.id,
concat_ws(';', ins.c_reg_doc_ser, ins.c_reg_doc_numb, ins.c_reg_doc_date, clorg.c_name, ct.c_name, rgn.c_name   ) as fns_registration_doc
from client cl
left join tax_insp ins on ins.collection_id = cl.c_inspect
left join tax_inspect insp on insp.id = ins.c_name
left join client clorg on clorg.id = insp.c_name
left join names_city ct on ct.id = insp.c_city
left join region rgn on rgn.id = insp.c_district)
, client_legal_address as (
select cl_id,
replace(replace(string_agg(address,','),',','#'),'|',',') as list_address
from (select cl.id as cl_id,
replace(ads1.c_post_code || ', ' || ci.c_name || ',
' ||ads1.c_street || ', ' || ads1.c_house || ', ' ||
coalesce(ads1.c_korpus,ads1.c_building_number) ||
', ' || ads1.c_flat,',','|') address
from personal_address ads1
join client cl on cl.c_addresses = ads1.collection_id::text
join address_type at1 on ads1.c_type = at1.id::text
left join names_city ci on ads1.c_city = ci.id::text
where at1.c_kod = 'CORP') a
group by cl_id
order by cl_id)
, client_fact_address as (
select cl_id,
replace(replace(string_agg(address,','),',','#'),'|',',') as fact_address
from (select cl.id as cl_id,
replace(ads1.c_post_code || ', ' || ci.c_name || ',
' ||ads1.c_street || ', ' || ads1.c_house || ', ' ||
coalesce(ads1.c_korpus,ads1.c_building_number) ||
', ' || ads1.c_flat,',','|') address
from personal_address ads1
join client cl on cl.c_addresses = ads1.collection_id::text
join address_type at1 on ads1.c_type = at1.id::text
left join names_city ci on ads1.c_city = ci.id::text
where at1.c_kod = 'FACT') a
group by cl_id
order by cl_id)
, phone_list as (
select cnt.collection_id,
replace(string_agg(cnt.c_numb,','),',','#') as list_phone
from contacts cnt
left join comunication cmnc on cmnc.id::text = cnt.c_type
where cmnc.c_code in ('PHONE','MOBILEPHONE')
group by cnt.collection_id),
email_list as (
select cnt.collection_id,
replace(string_agg(cnt.c_numb,','),',','#') as list_email
from contacts cnt
left join comunication cmnc on cmnc.id::text = cnt.c_type
where cmnc.c_code in ('MAIL')
group by cnt.collection_id),
fax_list as (
select cnt.collection_id,
replace(string_agg(cnt.c_numb,','),',','#') as list_fax
from contacts cnt
left join comunication cmnc on cmnc.id::text = cnt.c_type
where cmnc.c_code in ('FAX')
group by cnt.collection_id)
, chief as (
select pers.collection_id,
string_agg(clfl.c_name,',') as director_name
from persons_pos pers
left join client clfl on pers.c_fase=clfl.id::text
join cl_corp clc on clc.c_all_boss = pers.collection_id::text
where pers.c_chief::int = 1
group by pers.collection_id
)
, chief_accountant_name as (
select pers.collection_id,
string_agg(clfl.c_name,',') as chief_accountant_name
from persons_pos pers
left join client clfl on pers.c_fase=clfl.id::text
join cl_corp clc on clc.c_all_boss = pers.collection_id::text
where (pers.c_general_acc::int = 1 or (select 1
from casta c where c.id::text = pers.c_range and
upper(c.c_value) = ' ÃËÀÂÍÛÉ ÁÓÕÃÀËÒÅÐ')::int = 1)
group by pers.collection_id
)
,bank_and_elim_info as (
select * from (
select ir.id as id_client,
row_number () over
(partition by ir.id order by stc.c_lim_date desc,
stc.id desc) as ord,
case when ir.c_code like 'BANKRUPT%' then 'b' when ir.c_code like 'LIQUIDATION%' then 'l'
end as code,
concat_ws(';', stc.id, stc.c_kind_limit, stc.c_reason, ir.c_name, stc.c_date_begin, stc.c_date_end, stc.c_lim_num, stc.c_lim_date, stc.c_dop_info) as info
from ins_restrict ir
join st_client stc on stc.c_kind_limit = ir.id::text
join client c on stc.collection_id = c.c_state_stage
where (stc.c_date_begin <= to_char(current_date, 'DD.MM.YY') or stc.c_date_begin is null)
and (stc.c_date_end > to_char(current_date, 'DD.MM.YY') or stc.c_date_end is null)
and (ir.c_code like 'BANKRUPT%' or ir.c_code like 'LIQUIDATION%')
) info)
------------------------------------------------
--insert into student43.cust_org
select
now() as tech_change_time
, null as tech_session_id
, client.id as client_id
, cl_corp.c_long_name as name
, client.c_name as short_name
, client.c_i_name as eng_name
, form_property.c_code as okopf_code
, form_property.c_name as okopf_name
, ownership_type.c_short_name as okfc_code
, ownership_type.c_name as okfc_name
, cl_org.c_business as type_of_activity
, country.c_name as country_name
, client.c_inn  as inn
, '' as inn_hist
, client.c_kio as kio
, '' as kio_hist
, case when client.c_crr is null then client.c_kpp else client.c_crr end as kpp_main
, '' as kpp_main_hist
, cl_corp.c_register_gos_reg_num_rec as ogrn
, '' as ogrn_hist
, to_char(case when cl_corp.c_register_date_reg <> '29.02.09' then concat_ws('-', concat('20', substr(cl_corp.c_register_date_reg, 7, 2)), substr(cl_corp.c_register_date_reg, 4, 2), substr(cl_corp.c_register_date_reg, 1, 2))::date else '1900-01-01' end, 'YYYYMMDD') as registartion_date
, concat_ws(' ', cl_corp.c_register_ser_svid, cl_corp.c_register_num_svid) as registration_doc
, ''  as registration_authority_name
, to_char((case when tax_insp.c_date <> '29.02.09' or tax_insp.c_date is not null then concat_ws('-', concat('20', substr(tax_insp.c_date, 7, 2), substr(tax_insp.c_date, 4, 2), substr(tax_insp.c_date, 1, 2))) else '1900-01-01' end)::date, 'YYYYMMDD')  as fns_registration_date
, taxr.fns_registration_doc
, client.c_okato_code as okato_code
, null as okato_name
, null as list_okved_code
, null as list_okved_name
, case when  cl_corp.c_register_declare_uf is null then cl_corp.c_register_paid_uf else cl_corp.c_register_declare_uf end as authorized_capital_amt
, client_legal_address.list_address as list_legal_address
, client_fact_address.fact_address as client_fact_address
, pl.list_phone as list_phone
, fl.list_fax as list_fax
, el.list_email as list_email
, cl_bank.c_swift_c as swift
, case when client.c_resident not in('0','1') then '-' else client.c_resident end as is_currency_residence
, case when client.c_taxr not in('0','1') then '-' else client.c_resident end as is_tax_resident
, chief.director_name as director_name
, chief_accountant_name.chief_accountant_name as chief_accountant_name
, cl_group.c_name as business_segment_name
, bank_and_elim_info.info as bankruptcy_info
, bank_and_elim_info1.info as elimination_info
, client.c_crt_dat as service_start_date
, cl_bank_n.c_bic as bic
, cl_bank_n.c_reg_num as reg_num
, cl_bank_n.c_ks as corr_acc_num
, '' as list_natural_client_id
from student43.client
join student43.cl_corp on client.id=cl_corp.id
join student43.form_property on form_property.id = cl_corp.c_forma
left join student43.ownership_type on ownership_type.id = cl_corp.c_ownership
left join student43.cl_org on cl_org.id = client.id
left join student43.country on country.id = client.c_country
left join student43.tax_insp on client.c_inspect = tax_insp.collection_id
left join tax_registration taxr on client.id = taxr.id
left join client_legal_address on client.id = client_legal_address.cl_id
left join client_fact_address on client.id = client_fact_address.cl_id 
left join phone_list pl on client.c_contacts = pl.collection_id::text
left join email_list el on client.c_contacts = el.collection_id::text
left join fax_list fl on client.c_contacts = fl.collection_id::text
left join cl_bank on cl_bank.id = client.id
left join chief on chief.collection_id::text = cl_corp.c_all_boss
left join chief_accountant_name on chief_accountant_name.collection_id::text = cl_corp.c_all_boss
left join cl_categories on cl_categories.collection_id::text = client.c_vids_cl
left join cl_group on  cl_group.id::text = cl_categories.c_category
left join bank_and_elim_info on bank_and_elim_info.id_client = client.id and bank_and_elim_info.code = 'b' and bank_and_elim_info.ord = '1'
left join bank_and_elim_info  bank_and_elim_info1 on bank_and_elim_info.id_client = client.id 
left join cl_bank_n on cl_bank_n.id = client.id;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------

--drop table student43.cust_org;

CREATE TABLE student43.cust_org ( 
tech_change_time  timestamp NULL
, tech_session_id int8 NULL
, client_id int8 NULL
,  name text NULL
, clshort_name text NULL
, eng_name text NULL
, okopf_code text NULL
, okopf_name text NULL
, okfc_code text NULL
, okfc_name text NULL
, type_of_activity text NULL
, country_name text NULL
, inn text NULL
, inn_hist text NULL
, kio text NULL
, kio_hist text NULL
, kpp_main text NULL
, kpp_main_hist text NULL
, ogrn text NULL
, ogrn_hist text NULL
, registartion_date text NULL
, registration_doc text NULL
, registration_authority_name text NULL
, fns_registration_date text NULL
, fns_registration_doc text NULL
, okato_code text NULL
, okato_name text NULL
, list_okved_code text NULL
, list_okved_name text NULL
, authorized_capital_amt text NULL
, list_legal_address text NULL
, client_fact_address text NULL
, list_phone text NULL
, list_fax text NULL
, list_email text NULL
, swift text NULL
, is_currency_residence text NULL
, is_tax_resident text NULL
, director_name text NULL
, chief_accountant_name text NULL
, business_segment_name text NULL
, bankruptcy_info text NULL
, elimination_info text NULL
, service_start_date text NULL
, bic text NULL
, reg_num text NULL
, corr_acc_num text NULL
, list_natural_client_id text null
)
DISTRIBUTED BY (client_id);

---------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE adb.student43.client (
	id text,
	c_inspect text,
	c_state_stage text,
	c_country text,
	c_contacts text,
	c_okved_array text,
	c_vids_cl text,
	c_okved_in_period text,
	c_addresses text,
	c_okato_code text,
	c_name text,
	c_i_name text,
	c_inn text,
	c_kio text,
	c_crr text,
	c_kpp text,
	c_resident text,
	c_taxr text,
	c_crt_dat text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/CLIENT.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';


CREATE EXTERNAL TABLE adb.student43.st_client (
	id text,
	c_kind_limit text,
	collection_id text,
	c_date_begin text,
	c_date_end text,
	c_reason text,
	c_lim_num text,
	c_dop_info text,
	c_lim_date text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/ST_CLIENT.csv'
) ON ALL
FORMAT 'CSV' ( delimiter ',' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

--drop external table adb.student43.client;
--drop external table adb.student43.cl_corp;

CREATE EXTERNAL TABLE adb.student43.cl_corp (
	id text,
	c_all_boss text,
	c_register_reg_body text,
	c_ownership text,
	c_forma text,
	c_register_declare_uf text,
	c_register_paid_uf text,
	c_register_ser_svid text,
	c_register_num_svid text,
	c_register_date_reg text,
	c_register_gos_reg_num_rec text,
	c_long_name text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/CL_CORP.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.personal_address definition

CREATE EXTERNAL TABLE adb.student43.personal_address (
	id text,
	collection_id text,
	c_type text,
	c_city text,
	c_post_code text,
	c_street text,
	c_house text,
	c_korpus text,
	c_building_number text,
	c_flat text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/PERSONAL_ADDRESS.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.address_type definition

-- Drop table

-- DROP EXTERNAL TABLE student43.address_type;

CREATE EXTERNAL TABLE adb.student43.address_type (
	id text,
	c_kod text,
	c_name text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/ADDRESS_TYPE.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.names_city definition

-- Drop table

-- DROP EXTERNAL TABLE student43.names_city;

CREATE EXTERNAL TABLE adb.student43.names_city (
	id text,
	c_name text,
	c_cod_city text,
	c_country text,
	c_status text,
	c_people_place text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/NAMES_CITY.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';


-- student43.contacts definition

-- Drop table

-- DROP EXTERNAL TABLE student43.contacts;

CREATE EXTERNAL TABLE adb.student43.contacts (
	id text,
	collection_id text,
	c_type text,
	c_numb text,
	c_dat_edt text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/CONTACTS.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.comunication definition

-- Drop table

-- DROP EXTERNAL TABLE student43.comunication;

CREATE EXTERNAL TABLE adb.student43.comunication (
	id text,
	c_code text,
	c_value text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/COMUNICATION.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.persons_pos definition

-- Drop table

-- DROP EXTERNAL TABLE student43.persons_pos;

CREATE EXTERNAL TABLE adb.student43.persons_pos (
	id text,
	c_fase text,
	collection_id text,
	c_chief text,
	c_range text,
	c_general_acc text,
	c_work_end text,
	c_work_begin text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/PERSONS_POS.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.casta definition

-- Drop table

-- DROP EXTERNAL TABLE student43.casta;

CREATE EXTERNAL TABLE adb.student43.casta (
	id text,
	c_value text,
	c_code text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/CASTA.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '' quote '' header )
ENCODING 'UTF8';

-- student43.tax_insp definition

-- Drop table

--DROP EXTERNAL TABLE student43.tax_insp;

CREATE EXTERNAL TABLE adb.student43.tax_insp (
	id text,
	collection_id text,
	c_name text,
	c_reg_doc_ser text,
	c_reg_doc_numb text,
	c_date text,
	c_reg_doc_date text,
	c_inspector text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/TAX_INSP.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';


-- student43.tax_inspect definition

-- Drop table


--DROP EXTERNAL TABLE student43.tax_inspect;

CREATE EXTERNAL TABLE adb.student43.tax_inspect (
	id text,
	c_name text,
	c_city text,
	c_district text,
	c_num text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/TAX_INSPECT.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';


-- student43.region definition

-- Drop table

-- DROP EXTERNAL TABLE student43.region;

CREATE EXTERNAL TABLE adb.student43.region (
	id text,
	c_name text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/REGION.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.ins_restrict definition

-- Drop table

-- DROP EXTERNAL TABLE student43.ins_restrict;

CREATE EXTERNAL TABLE adb.student43.ins_restrict (
	id text,
	c_code text,
	c_name text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/INS_RESTRICT.csv'
) ON ALL
FORMAT 'CSV' ( delimiter ',' null '' escape '"' quote '"' header )
ENCODING 'UTF8';


-- student43.okved_ref definition

-- Drop table

-- DROP EXTERNAL TABLE student43.okved_ref;

CREATE EXTERNAL TABLE adb.student43.okved_ref (
	id text,
	collection_id text,
	c_value text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/OKVED_REF.csv'
) ON ALL
FORMAT 'CSV' ( delimiter ',' null '' escape '"' quote '"' header )
ENCODING 'UTF8';


-- student43.okved definition

-- Drop table

-- DROP EXTERNAL TABLE student43.okved;

CREATE EXTERNAL TABLE adb.student43.okved (
	id text,
	c_code text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/OKVED.csv'
) ON ALL
FORMAT 'CSV' ( delimiter ',' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.cl_bank definition

-- Drop table

-- DROP EXTERNAL TABLE student43.cl_bank;

CREATE EXTERNAL TABLE adb.student43.cl_bank (
	id text,
	c_swift_c text,
	class_id text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/CL_BANK.csv'
) ON ALL
FORMAT 'CSV' ( delimiter ',' null '' escape '"' quote '"' header )
ENCODING 'UTF8';


-- student43.cl_group definition

-- Drop table

-- DROP EXTERNAL TABLE student43.cl_group;

CREATE EXTERNAL TABLE adb.student43.cl_group (
	id text,
	c_name text,
	c_code text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/CL_GROUP.csv'
) ON ALL
FORMAT 'CSV' ( delimiter ',' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.cl_categories definition

-- Drop table

-- DROP EXTERNAL TABLE student43.cl_categories;

CREATE EXTERNAL TABLE adb.student43.cl_categories (
	id text,
	c_category text,
	collection_id text,
	c_date_end text,
	c_date_begin text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/CL_CATEGORIES.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';


-- student43.cl_org definition

-- Drop table

--DROP EXTERNAL TABLE student43.cl_org;

CREATE EXTERNAL TABLE adb.student43.cl_org (
	id text,
	c_business text,
	c_date_liquid text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/CL_ORG.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.country definition

-- Drop table

-- DROP EXTERNAL TABLE student43.country;

CREATE EXTERNAL TABLE adb.student43.country (
	id text,
	c_name text,
	c_code text,
	c_end_date text,
	c_begin_date text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/COUNTRY.csv'
) ON ALL
FORMAT 'CSV' ( delimiter ',' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.cl_bank_n definition

-- Drop table

-- DROP EXTERNAL TABLE student43.cl_bank_n;

CREATE EXTERNAL TABLE adb.student43.cl_bank_n (
	id text,
	c_ks text,
	c_bic text,
	c_reg_num text,
	c_ks_old text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/CL_BANK_N.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.form_property definition

-- Drop table

-- DROP EXTERNAL TABLE student43.form_property;

CREATE EXTERNAL TABLE adb.student43.form_property (
	id text,
	c_short_name text,
	c_code text,
	c_name text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/FORM_PROPERTY.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';

-- student43.ownership_type definition

-- Drop table

-- DROP EXTERNAL TABLE student43.ownership_type;

CREATE EXTERNAL TABLE adb.student43.ownership_type (
	id text,
	c_short_name text,
	c_name text
)
LOCATION (
	'gpfdist://10.30.104.24:8081/OWNERSHIP_TYPE.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '^' null '' escape '"' quote '"' header )
ENCODING 'UTF8';
