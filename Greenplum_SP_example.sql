# 스크립트 목적
일반 쿼리의 경우 command center의 쿼리 히스토리 로그 테이블에 적재가 되지만, 
프로시져(함수)를 이용한 경우에는 command center에서 개별 로그가 남지 않음

프로시져 호출시 쿼리 개별로그가 마스터 노드의 로그에는 쌓이지 않지만, 세그먼트의 로그에 쌓이는 것을 이용하여
로그를 분석할 수 있도록 예시를 만듬.

# 제약 조건
프로시저에 truncate, analyze 등의 로그는 세그먼트 인스턴스의 로그에 쌓이지 않기 때문에, DB로그 추출에는 한개 발생
이를 위해서는 함수에서 인자로 처리해야 함.

# 기타 사항
프로시저 작성시 에러 메시지,에러 라인 등을 보다 쉽게 확인할 수 있도록 예외 처리를 하였으며, 이를 활용하면 개발시 도움이 될 것으로 판단.
예시는 쿼리가 수행될 때 마다 DB 로그에서 쿼리를 가져오는 구조이며, 예시 임. 
실제 사용시에는 개별 쿼리마다 로그를 남기지 말고  SP로그와 DB로그를 세션 ID와 쿼리 번호로 조인해서 추출하면 됨. (이것이 효율적) 
영호님, 종현님 도움 주셔서 감사합니다.

# 사전 준비 
세그먼트 인스턴스의 postgresql.conf에 아래 2개의 파라미터 추가    
log_statement = 'all'			# none, mod, ddl, all
log_duration = on
  => 마스터 노드에서 gpstate -c 으로 확인하면 서버 및 파일의 경로가 나옴. 이중 gpseg0 에 대해서 해줌.. sdw1노드의 경로가 나옴.
  => 테스트 스크립트에서는 gpxeg0 으로 있기 때문에 주의 필
DB 재시작 

#테이블 로그성 테이블 생성 
Greenplum 현재까지의 version은 프로시저 호출시 1트랜잭션이기 때문에, 로그를 쌓도록 하더라도 진행과정을 보기 힘듬.
그래서 예외 형태로, write external table을 활용. 별도의 트랜잭션으로 로그를 남
-- 기록할 로그 테이블
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

-- 로그 테이블에 기록하는 external table  
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

-- external table에 기록하는 함수 생성
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


-- 세그먼트 로그를 읽는 external table (segment 0)
-- 사실 필요없는 부분이지만, 실행되었던 쿼리를 추출하기 위한 목적으로 사용, 
-- duration을 이용하면, 쿼리 수행시간을 추출할 수 있음. 
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


--쿼리 추출 함수
--세션 번호와 쿼리 번호를 넣으면, 쿼리를 추출함. 세그먼트 0의 DB로그에서 추출 함. 
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


-- 테스트를 위한 테이블, 프로시저 내에서 delete/insert 할 예
drop table if exists public.test_tab ;
create table public.test_tab (
  col1 int,
  col2 varchar(10)
) 
with (appendonly=true, compresslevel=7, compresstype=zstd)
distributed randomly ;

select * from pg_stat_activity

-- 테스트 프로시저
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

  --현재 프로시져 명을 추	
  GET DIAGNOSTICS v_tmp = PG_CONTEXT;
  v_cur_spnm := split_part((substring(v_tmp from 'function (.*?) line'))::regprocedure::text, '(', 1);
 
  --현재 세션 ID를 추출 
  v_ccnt :=  0;
  select usename, sess_id into v_usr, v_ssid from pg_stat_activity where pid = pg_backend_pid(); 
  
  --현재의 쿼리 번호(cmd)를 추출하고 +1 을 하면 다음 쿼리의 번호를 알 수 있음. 즉 아래의 v_ccnt는 delete 쿼리 번호임.
  v_ccnt := current_setting ('gp_command_count')::int + 1;-- 현재 위치의 cmd 번호 + 1 => 다음에 사용할 아래 SQL의 cmd 번호
  delete from  public.test_tab;
  get diagnostics v_rows := row_count; -- 처리 건수
  select dba.udf_get_query(v_ssid, v_ccnt) into v_query;  --세션 ID와 쿼리 번호로 부터 쿼리를 추
  perform dba.udf_wsplog (cast(clock_timestamp() as timestamp(6)), v_cur_spnm, v_usr, v_ssid,v_ccnt,v_rows,v_query,v_err_msg) ; -- 로깅 함수 호출 
 
  v_ccnt := current_setting ('gp_command_count')::int + 1;
  insert into public.test_tab 
  select i, i::text 
  from   generate_series (1, 2000) i;
  get diagnostics v_rows := row_count; -- 처리 건수
  select dba.udf_get_query(v_ssid, v_ccnt) into v_query;
  perform dba.udf_wsplog (cast(clock_timestamp() as timestamp(6)), v_cur_spnm, v_usr, v_ssid,v_ccnt,v_rows,v_query,v_err_msg) ; -- 로깅 함수 호출 

  v_ccnt := current_setting ('gp_command_count')::int + 1;
  insert into public.test_tab 
  select a.*
  from   public.test_tab a, public.test_tab b;
  get diagnostics v_rows := row_count; -- 처리 건수
  select dba.udf_get_query(v_ssid, v_ccnt) into v_query;
  perform dba.udf_wsplog (cast(clock_timestamp() as timestamp(6)), v_cur_spnm, v_usr, v_ssid,v_ccnt,v_rows,v_query,v_err_msg) ; -- 로깅 함수 호출 

  v_ccnt := current_setting ('gp_command_count')::int + 1;
  --analyze의 경우에는 로그에 쌓이지 않음. 그래서 쿼리를 가져오는 것 보다,v_query에 'analyze public.test_tab' 으로 기입하는 것이 좋음.
  analyze public.test_tab;
  get diagnostics v_rows := row_count; -- 처리 건수
  select dba.udf_get_query(v_ssid, v_ccnt) into v_query;
  perform dba.udf_wsplog (cast(clock_timestamp() as timestamp(6)), v_cur_spnm, v_usr, v_ssid,v_ccnt,v_rows,v_query,v_err_msg) ; -- 로깅 함수 호출 

  return 'OK';

  exception 
     when others then
	    get stacked diagnostics
	        v_err_cd    = returned_sqlstate,
	        v_err_msg   = message_text,
	        v_err_context   = pg_exception_context;
	    select dba.udf_get_query(v_ssid, v_ccnt) into v_query;   
        perform dba.udf_wsplog (cast(clock_timestamp() as timestamp(6)), v_cur_spnm, v_usr, v_ssid,v_ccnt,v_rows,v_query
                               ,v_err_cd||'>>'||v_err_msg||'>>'||v_err_context) ; -- 로깅 함수 호출 
	    raise notice E'Got exception: 
            session : %
	        err_cd  : %
	        err_msg : %
	        err_context: %', (v_ssid)::text, v_err_cd, v_err_msg, v_err_context;
	       
	    return 'ERROR:'||v_err_msg;
end;
$$
language plpgsql;

--강제로 에러 발생시 select a.* 으로 해야하는데 에러 발생시키려고 select a.*, b.* 으로 함.
--DB에 로그로 쌓임.
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

-- select a.*, b.* 를 select a.*  으로 수정 및 컴파일 후 테스트 결
edu=# select public.udf_sp_test();
 udf_sp_test
-------------
 OK
(1 row)

Time: 3098.605 ms
edu=#

--SP 실행 로그 
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
  
