
# Greenplum_SP_example.sql

스크립트 목적일반 쿼리의 경우 command center의 쿼리 히스토리 로그 테이블에 적재가 되지만, 
프로시져(함수)를 이용한 경우에는 command center에서 개별 로그가 남지 않음

프로시져 호출시 쿼리 개별로그가 마스터 노드의 로그에는 쌓이지 않지만, 세그먼트의 로그에 쌓이는 것을 이용하여
로그를 분석할 수 있도록 예시를 만듬.
참조 링크: http://gpdbkr.blogspot.com/search/label/GPDB6_SP 

# Greenplum_SP_get_resource.sql
일반 쿼리의 경우 command center의 쿼리 히스토리 로그 테이블에서 개별 쿼리의 리소스가 적재 되지만,  
프로시져(함수)를 이용한 경우에는 개별 쿼리의 시스템 리소스 정보가 command center에서 개별 로그가 남지 않음

pidstat를 이용하여 쿼리 세션 ID(conxxxx)와 쿼리 번호(cmdxxx)를 기준으로 cpu, memory, disk의 리소스를 1초마다 추출
프로시저 로그와 맵핑하여 개별 쿼리의 리소스를 추출 함. Greenplum_SP_example.sql 구현 후 Greenplum_SP_get_resource.sql 적용
참조 링크: http://gpdbkr.blogspot.com/search/label/GPDB6_%EC%BF%BC%EB%A6%AC%EB%B3%84%EB%A6%AC%EC%86%8C%EC%8A%A4
