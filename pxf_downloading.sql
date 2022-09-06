--pxf

--В базе создается внешняя таблица, (напр. pzrs_ttc), нужно указать сервер, далее insert into таблица select * from таблица_ext
--Создание таблицы-приемника
CREATE TABLE ods_bwp.pzrs_ttc (
	zrs_ttc int4 NULL,
	zrs_typec varchar(1) NULL,
	zfisccalc varchar(1) NULL,
	order_id int8 NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
TABLESPACE ts_data
DISTRIBUTED BY (zrs_ttc);

--Создание таблицы-источника
CREATE EXTERNAL TABLE adb.ods_bwp.pzrs_ttc_ext (
 zrs_ttc int4,
	zrs_typec varchar,
	zfisccalc varchar,
	order_id int8 
)
LOCATION (  'pxf://ods_bwp.pzrs_ttc?PROFILE=Jdbc&server=gp-prod') ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

--Загрузка данных
insert into ods_bwp.pzrs_ttc select * from ods_bwp.pzrs_ttc_ext

--------------------------------------------------------------------------------------------
--Создание таблицы
create external table dma_dm_ledger_account_s
(
chapter text,
chapter_name text,
section_number integer,
section_name text,
subsection_name text,
ledger1_account text,
ledger1_account_name text,
ledger_account text,
ledger_account_name text,
characteristic text,
is_resident integer,
is_reserve integer,
is_reserved integer,
is_loan integer,
is_reserved_assets integer,
is_overdue integer,
is_interest integer,
pair_account text,
start_date date,
end_date date,
is_rub_only integer,
min_term nteger,
min_term_measure text,
max_term integer,
max_term_measure text,
ledger_acc_full_name_translit text,
is_revaluation text,
is_correct text,
is_correct_assets text)
location ('pxf://DMA.DM_LEDGER_ACCOUNT_S?PROFILE=Jdbc&SERVER=oracle_db')
format 'custom' (formatter = 'pxfwritable_import')
encoding ='UTF8';