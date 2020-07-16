# ��ũ��Ʈ ����
�Ϲ� ������ ��� command center�� ���� �����丮 �α� ���̺� ���簡 ������, 
���ν���(�Լ�)�� �̿��� ��쿡�� command center���� ���� �αװ� ���� ����

���ν��� ȣ��� ���� �����αװ� ������ ����� �α׿��� ������ ������, ���׸�Ʈ�� �α׿� ���̴� ���� �̿��Ͽ�
�α׸� �м��� �� �ֵ��� ���ø� ����.

# ���� ����
���ν����� truncate, analyze ���� �α״� ���׸�Ʈ �ν��Ͻ��� �α׿� ������ �ʱ� ������, DB�α� ���⿡�� �Ѱ� �߻�
�̸� ���ؼ��� �Լ����� ���ڷ� ó���ؾ� ��.

# ��Ÿ ����
���ν��� �ۼ��� ���� �޽���,���� ���� ���� ���� ���� Ȯ���� �� �ֵ��� ���� ó���� �Ͽ�����, �̸� Ȱ���ϸ� ���߽� ������ �� ������ �Ǵ�.
���ô� ������ ����� �� ���� DB �α׿��� ������ �������� �����̸�, ���� ��. 
���� ���ÿ��� ���� �������� �α׸� ������ ����  SP�α׿� DB�α׸� ���� ID�� ���� ��ȣ�� �����ؼ� �����ϸ� ��. (�̰��� ȿ����) 
��ȣ��, ������ ���� �ּż� �����մϴ�.

# ���� �غ� 
���׸�Ʈ �ν��Ͻ��� postgresql.conf�� �Ʒ� 2���� �Ķ���� �߰�    
log_statement = 'all'			# none, mod, ddl, all
log_duration = on
  => ������ ��忡�� gpstate -c ���� Ȯ���ϸ� ���� �� ������ ��ΰ� ����. ���� gpseg0 �� ���ؼ� ����.. sdw1����� ��ΰ� ����.
  => �׽�Ʈ ��ũ��Ʈ������ gpxeg0 ���� �ֱ� ������ ���� ��
DB ����� 

#���̺� �α׼� ���̺� ���� 
Greenplum ��������� version�� ���ν��� ȣ��� 1Ʈ������̱� ������, �α׸� �׵��� �ϴ��� ��������� ���� ����.
�׷��� ���� ���·�, write external table�� Ȱ��. ������ Ʈ��������� �α׸� ��
-- ����� �α� ���̺�
create schema dba;
drop table if exists dba.tb_splog;
create table dba.tb_splog (
  cts  timestamp,
  spnm varchar(63),
  usr  varchar(63),
  ssid integer,
  ccnt bigint,
  rows bigint,
  query text,
  err_msg text
) distributed randomly ;

-- �α� ���̺� ����ϴ� external table  
drop external table if exists dba.ext_tb_wsplog;
create writable external web table dba.ext_tb_wsplog (
  cts  timestamp,
  spnm varchar(63),
  usr  varchar(63),
  ssid integer,
  ccnt integer,  
  rows bigint,
  query text,
  err_msg text 
)
execute 'psql -h mdw -p 5432 -d edu -c "copy dba.tb_splog from stdin delimiter ''|'' " '
format 'text' (delimiter '|' null '\N' escape '\')
encoding 'UTF8' ;

-- external table�� ����ϴ� �Լ� ����
drop function if exists dba.udf_wsplog (v_cts timestamp, v_spnm text, v_usr text, v_ssid integer, v_ccnt integer, v_rows integer, v_query text, v_err_msg text)  ;
create or replace function dba.udf_wsplog
(v_cts timestamp, v_spnm text, v_usr text, v_ssid integer, v_ccnt integer, v_rows integer, v_query text, v_err_msg text) 
returns void
as
$$
begin
      insert into dba.ext_tb_wsplog values(v_cts, v_spnm, v_usr, v_ssid, v_ccnt, v_rows, v_query, v_err_msg ) ;
end;
$$
language plpgsql;


-- ���׸�Ʈ �α׸� �д� external table (segment 0)
-- ��� �ʿ���� �κ�������, ����Ǿ��� ������ �����ϱ� ���� �������� ���, 
-- duration�� �̿��ϸ�, ���� ����ð��� ������ �� ����. 
drop external table if exists dba.ext_db_seglog0;
CREATE EXTERNAL WEB TABLE dba.ext_db_seglog0 (
  event_time       text,
  user_name        text,
  database_name    text,
  process_id    text,
  thread_id    text,
  remote_host    text,
  remote_port    text,
  session_start_time    text,
  transaction_id     text,
  gp_session_id    text,
  gp_command_count    text,
  gp_segment    text,
  slice_id    text,
  distr_tranx_id    text,
  local_tranx_id    text,
  sub_tranx_id    text,
  event_severity    text,
  sql_state_code    text,
  event_message     text,
  event_detail    text,
  event_hint    text,
  internal_query    text,
  internal_query_pos    text,
  event_context    text,
  debug_query_string    text,
  error_cursor_pos    text,
  func_name    text,
  file_name    text,
  file_line    text,
  stack_trace    text
)
EXECUTE E'cat /data/primary/gpxeg0/pg_log/gpdb-`date +%Y-%m-%d`_*.csv 2> /dev/null || true' on segment 0
FORMAT 'csv' (delimiter as ',' quote as '"')
ENCODING 'utf8'
SEGMENT REJECT LIMIT 100000;


--���� ���� �Լ�
--���� ��ȣ�� ���� ��ȣ�� ������, ������ ������. ���׸�Ʈ 0�� DB�α׿��� ���� ��. 
create or replace function dba.udf_get_query(v_ssid integer, v_ccnt integer) 
returns text
as
$$
declare 
        v_sql text;
begin
	  select debug_query_string
	    into v_sql
	    from dba.ext_db_seglog0
	   where gp_session_id = 'con' || v_ssid
	     and gp_command_count = 'cmd' || v_ccnt
	     and debug_query_string is not null
	     --and cast(to_timestamp(event_time, 'yyyy-mm-dd hh24:mi:ss.us') as timestamp(6))>= (clock_timestamp() - interval '10 seconds')
	   order by event_time desc
	   limit 1;   
	  
	  return v_sql;
end;
$$
language plpgsql;


-- �׽�Ʈ�� ���� ���̺�, ���ν��� ������ delete/insert �� ��
drop table if exists public.test_tab ;
create table public.test_tab (
  col1 int,
  col2 varchar(10)
) 
with (appendonly=true, compresslevel=7, compresstype=zstd)
distributed randomly ;

select * from pg_stat_activity

-- �׽�Ʈ ���ν���
create or replace function public.udf_sp_test() 
returns text
as
$$
declare
  
  v_tmp  text;
  v_cur_spnm text;
  v_usr  text;
  v_ssid integer;  --session id
  v_ccnt integer;  --query number of session id
  v_cts  timestamp; --timestamp of query

  v_rows integer;   
  v_query varchar(1000);
 
  v_err_msg  text;
  v_err_cd   TEXT;
  v_err_context TEXT;
   
begin

  --���� ���ν��� ���� ��	
  GET DIAGNOSTICS v_tmp = PG_CONTEXT;
  v_cur_spnm := split_part((substring(v_tmp from 'function (.*?) line'))::regprocedure::text, '(', 1);
 
  --���� ���� ID�� ���� 
  v_ccnt :=  0;
  select usename, sess_id into v_usr, v_ssid from pg_stat_activity where pid = pg_backend_pid(); 
  
  --������ ���� ��ȣ(cmd)�� �����ϰ� +1 �� �ϸ� ���� ������ ��ȣ�� �� �� ����. �� �Ʒ��� v_ccnt�� delete ���� ��ȣ��.
  v_ccnt := current_setting ('gp_command_count')::int + 1;-- ���� ��ġ�� cmd ��ȣ + 1 => ������ ����� �Ʒ� SQL�� cmd ��ȣ
  delete from  public.test_tab;
  get diagnostics v_rows := row_count; -- ó�� �Ǽ�
  select dba.udf_get_query(v_ssid, v_ccnt) into v_query;  --���� ID�� ���� ��ȣ�� ���� ������ ��
  perform dba.udf_wsplog (cast(clock_timestamp() as timestamp(6)), v_cur_spnm, v_usr, v_ssid,v_ccnt,v_rows,v_query,v_err_msg) ; -- �α� �Լ� ȣ�� 
 
  v_ccnt := current_setting ('gp_command_count')::int + 1;
  insert into public.test_tab 
  select i, i::text 
  from   generate_series (1, 2000) i;
  get diagnostics v_rows := row_count; -- ó�� �Ǽ�
  select dba.udf_get_query(v_ssid, v_ccnt) into v_query;
  perform dba.udf_wsplog (cast(clock_timestamp() as timestamp(6)), v_cur_spnm, v_usr, v_ssid,v_ccnt,v_rows,v_query,v_err_msg) ; -- �α� �Լ� ȣ�� 

  v_ccnt := current_setting ('gp_command_count')::int + 1;
  insert into public.test_tab 
  select a.*
  from   public.test_tab a, public.test_tab b;
  get diagnostics v_rows := row_count; -- ó�� �Ǽ�
  select dba.udf_get_query(v_ssid, v_ccnt) into v_query;
  perform dba.udf_wsplog (cast(clock_timestamp() as timestamp(6)), v_cur_spnm, v_usr, v_ssid,v_ccnt,v_rows,v_query,v_err_msg) ; -- �α� �Լ� ȣ�� 

  v_ccnt := current_setting ('gp_command_count')::int + 1;
  --analyze�� ��쿡�� �α׿� ������ ����. �׷��� ������ �������� �� ����,v_query�� 'analyze public.test_tab' ���� �����ϴ� ���� ����.
  analyze public.test_tab;
  get diagnostics v_rows := row_count; -- ó�� �Ǽ�
  select dba.udf_get_query(v_ssid, v_ccnt) into v_query;
  perform dba.udf_wsplog (cast(clock_timestamp() as timestamp(6)), v_cur_spnm, v_usr, v_ssid,v_ccnt,v_rows,v_query,v_err_msg) ; -- �α� �Լ� ȣ�� 

  return 'OK';

  exception 
     when others then
	    get stacked diagnostics
	        v_err_cd    = returned_sqlstate,
	        v_err_msg   = message_text,
	        v_err_context   = pg_exception_context;
	    select dba.udf_get_query(v_ssid, v_ccnt) into v_query;   
        perform dba.udf_wsplog (cast(clock_timestamp() as timestamp(6)), v_cur_spnm, v_usr, v_ssid,v_ccnt,v_rows,v_query
                               ,v_err_cd||'>>'||v_err_msg||'>>'||v_err_context) ; -- �α� �Լ� ȣ�� 
	    raise notice E'Got exception: 
            session : %
	        err_cd  : %
	        err_msg : %
	        err_context: %', (v_ssid)::text, v_err_cd, v_err_msg, v_err_context;
	       
	    return 'ERROR:'||v_err_msg;
end;
$$
language plpgsql;

--������ ���� �߻��� select a.* ���� �ؾ��ϴµ� ���� �߻���Ű���� select a.*, b.* ���� ��.
--DB�� �α׷� ����.
edu=# select public.udf_sp_test();
NOTICE:  Got exception:
DETAIL:
            session : 2811
	        err_cd  : 42601
	        err_msg : INSERT has more expressions than target columns
	        err_context: SQL statement "insert into public.test_tab
  select a.*, b.*
  from   public.test_tab a, public.test_tab b"
PL/pgSQL function udf_sp_test() line 44 at SQL statement
                      udf_sp_test
-------------------------------------------------------
 ERROR:INSERT has more expressions than target columns
(1 row)

Time: 298.988 ms

-- select a.*, b.* �� select a.*  ���� ���� �� ������ �� �׽�Ʈ ��
edu=# select public.udf_sp_test();
 udf_sp_test
-------------
 OK
(1 row)

Time: 3098.605 ms
edu=#

--SP ���� �α� 
edu=# select *
from dba.tb_splog
where ssid = 2811
order by cts;
            cts             |    spnm     |   usr   | ssid | ccnt |  rows   |                     query                     |
      err_msg
----------------------------+-------------+---------+------+------+---------+-----------------------------------------------+-----------------------------------------
------------------------------------------------------------
 2020-07-16 10:31:21.40958  | udf_sp_test | gpadmin | 2811 |  242 |       0 | delete from  public.test_tab                  |
 2020-07-16 10:31:21.507344 | udf_sp_test | gpadmin | 2811 |  250 |    2000 | insert into public.test_tab                  +|
                            |             |         |      |      |         |   select i, i::text                          +|
                            |             |         |      |      |         |   from   generate_series (1, 2000) i          |
 2020-07-16 10:31:21.566102 | udf_sp_test | gpadmin | 2811 |  258 |    2000 |                                               | 42601>>INSERT has more expressions than
target columns>>SQL statement "insert into public.test_tab +
                            |             |         |      |      |         |                                               |   select a.*, b.*
                                                           +
                            |             |         |      |      |         |                                               |   from   public.test_tab a, public.test_
tab b"                                                     +
                            |             |         |      |      |         |                                               | PL/pgSQL function udf_sp_test() line 44
at SQL statement
 2020-07-16 10:31:45.736913 | udf_sp_test | gpadmin | 2811 |  269 |       0 | delete from  public.test_tab                  |
 2020-07-16 10:31:45.820574 | udf_sp_test | gpadmin | 2811 |  277 |    2000 | insert into public.test_tab                  +|
                            |             |         |      |      |         |   select i, i::text                          +|
                            |             |         |      |      |         |   from   generate_series (1, 2000) i          |
 2020-07-16 10:31:48.404578 | udf_sp_test | gpadmin | 2811 |  285 | 4000000 | insert into public.test_tab                  +|
                            |             |         |      |      |         |   select a.*                                 +|
                            |             |         |      |      |         |   from   public.test_tab a, public.test_tab b |
 2020-07-16 10:31:48.707383 | udf_sp_test | gpadmin | 2811 |  293 |       0 |                                               |
 2020-07-16 10:34:01.126235 | udf_sp_test | gpadmin | 2811 |  307 | 4002000 | delete from  public.test_tab                  |
 2020-07-16 10:34:01.215819 | udf_sp_test | gpadmin | 2811 |  315 |    2000 | insert into public.test_tab                  +|
                            |             |         |      |      |         |   select i, i::text                          +|
                            |             |         |      |      |         |   from   generate_series (1, 2000) i          |
 2020-07-16 10:34:04.233753 | udf_sp_test | gpadmin | 2811 |  323 | 4000000 | insert into public.test_tab                  +|
                            |             |         |      |      |         |   select a.*                                 +|
                            |             |         |      |      |         |   from   public.test_tab a, public.test_tab b |
 2020-07-16 10:34:04.690743 | udf_sp_test | gpadmin | 2811 |  331 |       0 |                                               |
(11 rows)

Time: 12.501 ms
edu=#

edu=# select public.udf_sp_test() ;
 udf_sp_test
-------------
 OK
(1 row)

Time: 11609.844 ms
edu=#

select public.udf_sp_test() ;
 test_func
-----------

 CAB
 CTTTM, 
 Goal setting
 

   select debug_query_string, cast(to_timestamp(event_time, 'yyyy-mm-dd hh24:mi:ss.us') as timestamp(6))
    --into v_query, v_cts
    from dba.ext_db_seglog0
   where gp_session_id = 'con446'
     and gp_command_count >= 'cmd74'  
     and debug_query_string is not null
     --and cast(to_timestamp(event_time, 'yyyy-mm-dd hh24:mi:ss.us') as timestamp(6))>= (clock_timestamp() - interval '10 seconds')
   order by event_time desc
   limit 1;
     ;
    
     and event_time::timestamp >= (clock_timestamp() - interval '5 seconds');
    
(1 row)

truncate table dba.tb_splog ;

select * 
from dba.tb_splog  
where ssid = 2811
order by cts;

42601>>INSERT has more expressions than target columns>>SQL statement "insert into public.test_tab 
  select a.*, b.*
  from   public.test_tab a, public.test_tab b"
PL/pgSQL function udf_sp_test() line 41 at SQL statement

insert into public.test_tab 
  select i, i::text 
  from   generate_series (1, 10000) i
  
