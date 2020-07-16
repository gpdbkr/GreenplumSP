# ��ũ��Ʈ ����
�Ϲ� ������ ��� command center�� ���� �����丮 �α� ���̺��� ���� ������ ���ҽ��� ���� ������,  
���ν���(�Լ�)�� �̿��� ��쿡�� ���� ������ �ý��� ���ҽ� ������ command center���� ���� �αװ� ���� ����

pidstat�� �̿��Ͽ� ���� ���� ID(conxxxx)�� ���� ��ȣ(cmdxxx)�� �������� cpu, memory, disk�� ���ҽ��� 1�ʸ��� ����
���ν��� �α׿� �����Ͽ� ���� ������ ���ҽ��� ���� ��.
���� ��ũ: http://gpdbkr.blogspot.com/search/label/GPDB6_SP
�̸� Ȱ���Ͽ�, ���ν������� ��� �κ��� ������ �߻��Ǵ��� Ȯ�� ������.

# ���� ����
1�� �̸��� ������ ���ҽ��� ���� ���� �� ������, �ý����� ���ϰ� �ɸ� ��� �ý��� ���ҽ� �������� ���ʰ� �ɸ� �� ����.

# ��Ÿ ����
pidstat���� �ּ� ������ 1���̱� ������, 1�� ������ ���ҽ��� ����, 
���� �̻��� ������ ���ؼ��� �ý��� ���ҽ��� ����� ���� ��.
pidstat���� cpu �����ϱ� ������, ���� ���󼭴� cpu ������ŭ �����Ⱑ �ʿ� ��. 

# ���� �غ� 
1) ��� ��忡 pidstat �� ����� �ֵ��� sysstat�� ��ġ�ؾ� ��.
# yum install sysstat
[root@mdw ~]# pidstat
-bash: pidstat: command not found
[root@mdw ~]# yum install sysstat
[root@mdw ~]# pidstat
Linux 3.10.0-693.el7.x86_64 (mdw) 	07/16/2020 	_x86_64_	(1 CPU)

11:34:29 AM   UID       PID    %usr %system  %guest    %CPU   CPU  Command
11:34:29 AM     0         1    0.04    0.04    0.00    0.08     0  systemd

2) ��� ����� �ð� ����ȭ 

# ��ũ��Ʈ ���� �� crontab ���
��� ��忡 ���� (������/���Ĺ��̸�����/���׸�Ʈ ��� ���)

[gpadmin@sdw1 utilities]$ pwd
/data/dba/utilities
[gpadmin@sdw1 utilities]$ cat sscmd_rsc.sh
#!/bin/bash

LOGDIR=/data/dba/utilities/statlog
mkdir -p $LOGDIR
LOGDT=`date +"%Y-%m-%d"`
HOSTNAME=`hostname`
LIMITPCNT=10
for i in `seq 1 59`
do
    TIME=`date "+%Y-%m-%d_%H:%M:%S"`
    PCNT=`ps -ef | grep pidstat | grep -v grep | wc -l`
    if [ $PCNT -lt $LIMITPCNT ]; then
        ## disk (Time, DB user, Session id, CMD no, disk r_mb/s, disk w_mb/s )
        pidstat -dl 1 1 | grep Average | grep postgres | grep con | grep cmd | awk -v host=$HOSTNAME -v date=$TIME '{print host"|"date"|"$9"|"$12"|"$14" "$4" "$5}' | awk '{MB_rd[$1] += $2/1024}{MB_wr[$1] += $3/1024} END {for ( i in MB_rd) print i"|" MB_rd[i]"|"MB_wr[i]}'  >> $LOGDIR/sscmd_disk_${LOGDT}.log 2>&1 &

        ## cpu (Time, DB user, Session id, CMD no, cpu usr%, cpu sys%, cpu tot%)
        pidstat -ul 1 1 | grep Average | grep postgres | grep con | grep cmd | awk -v host=$HOSTNAME -v date=$TIME '{print host"|"date"|"$11"|"$14"|"$16" "$4" "$5" "$7}' | awk '{usr[$1] += $2}{sys[$1] +=$3}{tot[$1] +=$4} END {for (i in usr) print i"|"usr[i]"|"sys[i]"|"tot[i]}' >> $LOGDIR/sscmd_cpu_${LOGDT}.log 2>&1 &

        ## mem (Time, DB user, Session id, CMD no, VSZ MB, RSS MB, %MEM)
        pidstat -rl 1 1 | grep Average | grep postgres | grep con | grep cmd | awk -v host=$HOSTNAME -v date=$TIME '{print host"|"date"|"$11"|"$14"|"$16" "$6" "$7" "$8}' | awk '{vsz[$1] += $2/1024}{rss[$1] +=$3/1024}{memp[$1] +=$4} END {for (i in vsz) print i"|"vsz[i]"|"rss[i]"|"memp[i]}' >> $LOGDIR/sscmd_mem_${LOGDT}.log 2>&1 &
    fi
    sleep 1
done
[gpadmin@sdw1 utilities]$ crontab -l
## disk io resource log of query session and cmd level
* * * * * /bin/bash /data/dba/utilities/sscmd_rsc.sh &
[gpadmin@sdw1 utilities]$

# �α� ����
[gpadmin@sdw1 statlog]$ cat sscmd_cpu_2020-07-16.log
sdw1|2020-07-16_11:41:16|gpadmin|con3618|cmd6|54.72|6.6|61.31
sdw1|2020-07-16_11:41:17|gpadmin|con3618|cmd22|48.54|11.59|60.14
sdw1|2020-07-16_11:41:18|gpadmin|con3618|cmd22|61.73|35.18|96.91
sdw1|2020-07-16_11:41:20|gpadmin|con3618|cmd22|36.45|19.33|55.8
[gpadmin@sdw1 statlog]$ cat sscmd_disk_2020-07-16.log
sdw1|2020-07-16_11:41:17|gpadmin|con3618|cmd22|0.0244141|0.167852
sdw1|2020-07-16_11:41:18|gpadmin|con3618|cmd22|0|0.859375
[gpadmin@sdw1 statlog]$ cat sscmd_mem_2020-07-16.log
sdw1|2020-07-16_11:41:16|gpadmin|con3618|cmd6|2496.09|46.9531|2.56
sdw1|2020-07-16_11:41:17|gpadmin|con3618|cmd22|2501.71|52.5312|2.89
sdw1|2020-07-16_11:41:18|gpadmin|con3618|cmd22|1876.51|44.6758|2.45
[gpadmin@sdw1 statlog]$

# external table ���̺� ���� 

drop external table if exists dba.ext_sscmd_mem;
create external web  table dba.ext_sscmd_mem (
   hostname varchar(20),
   log_dttm varchar(20),
   usr      varchar(63),
   ssid     varchar(63),
   sscmd    varchar(63),
   vsz_mb    numeric,
   rss_mb    numeric,
   mem_rate  numeric
) 
EXECUTE E'cat /data/dba/utilities/statlog/sscmd_mem_*.log' ON all
FORMAT 'text' (delimiter as '|')
ENCODING 'utf8'
SEGMENT REJECT LIMIT 100000;

drop external table if exists dba.ext_sscmd_cpu;
create external web  table dba.ext_sscmd_cpu (
   hostname varchar(20),
   log_dttm varchar(20),
   usr      varchar(63),
   ssid     varchar(63),
   sscmd    varchar(63),
   cpu_usr    numeric,
   cpu_sys    numeric,
   cpu_tot    numeric
) 
EXECUTE E'cat /data/dba/utilities/statlog/sscmd_cpu_*.log' ON all
FORMAT 'text' (delimiter as '|')
ENCODING 'utf8'
SEGMENT REJECT LIMIT 10;

drop external table if exists dba.ext_sscmd_disk;    
create external web  table dba.ext_sscmd_disk (
   hostname varchar(20),
   log_dttm varchar(20),
   usr      varchar(63),
   ssid     varchar(63),
   sscmd    varchar(63),
   disk_r_mb    numeric,
   disk_w_mb    numeric
) 
EXECUTE E'cat /data/dba/utilities/statlog/sscmd_disk_*.log' ON all
FORMAT 'text' (delimiter as '|')
ENCODING 'utf8'
SEGMENT REJECT LIMIT 10;

create or replace view dba.v_sscmd_rsc_detail
as
select hostname, log_dttm, usr, ssid, sscmd
	, sum(cpu_usr) cpu_usr, sum(cpu_sys) cpu_sys, sum(cpu_tot) cpu_tot
	, sum(disk_r_mb) disk_r_mb, sum(disk_w_mb) disk_w_mb
	, sum(vsz_mb) vsz_mb, sum(rss_mb) rss_mb, sum(mem_rate) mem_rate
from (
		SELECT hostname, log_dttm, usr, ssid, sscmd, cpu_usr, cpu_sys, cpu_tot, 0 disk_r_mb, 0 disk_w_mb, 0 vsz_mb, 0 rss_mb, 0 mem_rate
		FROM dba.ext_sscmd_cpu
		union all
		SELECT hostname, log_dttm, usr, ssid, sscmd, 0 cpu_usr, 0 cpu_sys, 0 cpu_tot, disk_r_mb, disk_w_mb, 0 vsz_mb, 0 rss_mb, 0 mem_rate
		FROM dba.ext_sscmd_disk
		union all
		SELECT hostname, log_dttm, usr, ssid, sscmd, 0 cpu_usr, 0 cpu_sys, 0 cpu_tot, 0 disk_r_mb, 0 disk_w_mb, vsz_mb, rss_mb, mem_rate
		FROM dba.ext_sscmd_mem
	 ) a 
group by hostname, log_dttm, usr, ssid, sscmd;



## ���� ����� ���ҽ� �α� �ǽð� Ȯ�� 
edu=# select * from dba.v_sscmd_rsc_detail
edu-# order by log_dttm, hostname;
 host|     log_dttm      |  usr  | ssid  |sscmd|cpu_usr|cpu_sys|cpu_tot |disk_r_mb|disk_w_mb| vsz_mb | rss_mb  |mem_rate
-----+-------------------+-------+-------+-----+-------+-------+--------+---------+---------+--------+---------+---------
 sdw2|2020-07-16_11:41:15|gpadmin|con3618|cmd6 |   56.4|   6.82|  63.26 |        0|        0| 4992.18| 92.8204 |    5.08
 sdw1|2020-07-16_11:41:16|gpadmin|con3618|cmd6 | 109.44|   13.2| 122.62 |        0|        0| 4992.18| 93.9062 |    5.12
 sdw1|2020-07-16_11:41:17|gpadmin|con3618|cmd22|  97.08|  23.18| 120.28 |0.0488282| 0.335704| 5003.42|105.0624 |    5.78
 sdw1|2020-07-16_11:41:18|gpadmin|con3618|cmd22| 123.46|  70.36| 193.82 |        0| 1.718750| 3753.02| 89.3516 |    4.90
 sdw2|2020-07-16_11:41:18|gpadmin|con3618|cmd22|  94.34|  35.84| 130.18 |        0| 1.147696| 5003.56|106.3984 |    5.82
 sdw2|2020-07-16_11:41:19|gpadmin|con3618|cmd22| 101.36|  56.14| 157.54 |        0|  1.77164| 2502.62| 68.0234 |    3.74
 sdw1|2020-07-16_11:41:20|gpadmin|con3618|cmd22|  72.90|  38.66|  111.6 |        0|        0|       0|       0 |       0
(7 rows)

Time: 168.865 ms
edu=#

## ���� ����� ����ID, ���� ��ȣ�� ��� view
drop view dba.v_sscmd_rsc_sum;
create or replace view dba.v_sscmd_rsc_sum
as
select  usr
      , replace(ssid, 'con', '')::int ssid
      , replace(sscmd, 'cmd', '')::int sscmd 
      , min(log_dttm) start_dttm, max(log_dttm) end_dttm
      , round(avg(cpu_usr)) avg_cpu_usr, round(avg(cpu_sys)) avg_cpu_sys, round(avg(cpu_tot)) avg_cpu_tot
      , round(sum(disk_r_mb)) sum_disk_r_mb, round(sum(disk_w_mb)) sum_disk_w_mb
      , round(max(vsz_mb)) max_vsz_mb, round(max(rss_mb)) max_rss_mb, round(max(mem_rate)) max_mem_rate
from  (
		 select hostname, log_dttm, usr, ssid, sscmd
		  , sum(cpu_usr) cpu_usr, sum(cpu_sys) cpu_sys, sum(cpu_tot) cpu_tot
		  , sum(disk_r_mb) disk_r_mb, sum(disk_w_mb) disk_w_mb
		  , sum(vsz_mb) vsz_mb, sum(rss_mb) rss_mb, sum(mem_rate) mem_rate
		 from (
				  SELECT hostname, log_dttm, usr, ssid, sscmd, cpu_usr, cpu_sys, cpu_tot
				                     , 0 disk_r_mb, 0 disk_w_mb, 0 vsz_mb, 0 rss_mb, 0 mem_rate
				  FROM dba.ext_sscmd_cpu
				  where  hostname like 'sdw%'
				  union all
				  SELECT hostname, log_dttm, usr, ssid, sscmd, 0 cpu_usr, 0 cpu_sys, 0 cpu_tot
				                     , disk_r_mb, disk_w_mb, 0 vsz_mb, 0 rss_mb, 0 mem_rate
				  FROM dba.ext_sscmd_disk
				  where  hostname like 'sdw%'
				  union all
				  SELECT hostname, log_dttm, usr, ssid, sscmd, 0 cpu_usr, 0 cpu_sys, 0 cpu_tot
				                    , 0 disk_r_mb, 0 disk_w_mb, vsz_mb, rss_mb, mem_rate
				  FROM dba.ext_sscmd_mem
				  where  hostname like 'sdw%'
		      ) a 
		 group by hostname, log_dttm, usr, ssid, sscmd
		   ) b 
group by 1,2,3
;
 
 -- ���ν��� ������ ���ҽ� Ȯ��
 SELECT cts, spnm, a.usr, a.ssid, ccnt, rows
     --, query
     --, err_msg
     --, start_dttm, end_dttm
 	--, avg_cpu_usr, avg_cpu_sys
 	, avg_cpu_tot
 	--, sum_disk_r_mb, sum_disk_w_mb
	, sum_disk_r_mb + sum_disk_w_mb  as sum_disk_mb
	--, max_vsz_mb, max_rss_mb, max_mem_rate
	, max_vsz_mb
FROM dba.tb_splog a
left outer join dba.v_sscmd_rsc_sum b 
on   a.ssid = b.ssid 
and  a.ccnt = b.sscmd ;

            cts     |    spnm     |   usr   | ssid | ccnt |  rows   | avg_cpu_tot | sum_disk_mb | max_vsz_mb
--------------------+-------------+---------+------+------+---------+-------------+-------------+------------
 2020-07-16 13:14:36| udf_sp_test | gpadmin | 3618 |   82 | 4002000 |         115 |           0 |       4992
 2020-07-16 13:11:28| udf_sp_test | gpadmin | 3618 |   70 |       0 |             |             |
 2020-07-16 13:11:24| udf_sp_test | gpadmin | 3618 |   54 |    2000 |             |             |
 2020-07-16 13:11:27| udf_sp_test | gpadmin | 3618 |   62 | 4000000 |         145 |           9 |       5004
 2020-07-16 13:14:36| udf_sp_test | gpadmin | 3618 |   90 |    2000 |             |             |
 2020-07-16 13:14:40| udf_sp_test | gpadmin | 3618 |   98 | 4000000 |         150 |           6 |       5004
 2020-07-16 13:11:24| udf_sp_test | gpadmin | 3618 |   46 | 4002000 |          64 |           0 |       4992
 2020-07-16 13:14:40| udf_sp_test | gpadmin | 3618 |  106 |       0 |             |             |
(8 rows)

-- ���ν��� ������ ���ҽ� Ȯ��
SELECT cts, spnm, a.usr, a.ssid, ccnt, rows
    , query
    --, err_msg
    --, start_dttm, end_dttm
	--, avg_cpu_usr, avg_cpu_sys
	, avg_cpu_tot
	--, sum_disk_r_mb, sum_disk_w_mb
	, sum_disk_r_mb + sum_disk_w_mb  as sum_disk_mb
	--, max_vsz_mb, max_rss_mb, max_mem_rate
	, max_vsz_mb
FROM dba.tb_splog a
left outer join dba.v_sscmd_rsc_sum b 
on   a.ssid = b.ssid 
and  a.ccnt = b.sscmd ;

            cts     |    spnm     |   usr   | ssid | ccnt |  rows   |                     query                     | avg_cpu_tot | sum_disk_mb | max_vsz_mb
--------------------+-------------+---------+------+------+---------+-----------------------------------------------+-------------+-------------+------------
 2020-07-16 13:14:36| udf_sp_test | gpadmin | 3618 |   82 | 4002000 | delete from  public.test_tab                  |         115 |           0 |       4992
 2020-07-16 13:11:28| udf_sp_test | gpadmin | 3618 |   70 |       0 |                                               |             |             |
 2020-07-16 13:11:24| udf_sp_test | gpadmin | 3618 |   54 |    2000 | insert into public.test_tab                  +|             |             |
                    |             |         |      |      |         |   select i, i::text                          +|             |             |
                    |             |         |      |      |         |   from   generate_series (1, 2000) i          |             |             |
 2020-07-16 13:11:27| udf_sp_test | gpadmin | 3618 |   62 | 4000000 | insert into public.test_tab                  +|         145 |           9 |       5004
                    |             |         |      |      |         |   select a.*                                 +|             |             |
                    |             |         |      |      |         |   from   public.test_tab a, public.test_tab b |             |             |
 2020-07-16 13:14:36| udf_sp_test | gpadmin | 3618 |   90 |    2000 | insert into public.test_tab                  +|             |             |
                    |             |         |      |      |         |   select i, i::text                          +|             |             |
                    |             |         |      |      |         |   from   generate_series (1, 2000) i          |             |             |
 2020-07-16 13:14:40| udf_sp_test | gpadmin | 3618 |   98 | 4000000 | insert into public.test_tab                  +|         150 |           6 |       5004
                    |             |         |      |      |         |   select a.*                                 +|             |             |
                    |             |         |      |      |         |   from   public.test_tab a, public.test_tab b |             |             |
 2020-07-16 13:11:24| udf_sp_test | gpadmin | 3618 |   46 | 4002000 | delete from  public.test_tab                  |          64 |           0 |       4992
 2020-07-16 13:14:40| udf_sp_test | gpadmin | 3618 |  106 |       0 |                                               |             |             |
(8 rows)

Time: 183.141 ms






