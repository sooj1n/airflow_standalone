USE history_db;

INSERT INTO cmd_useage
SELECT
    STR_TO_DATE(dt, '%Y-%m-%d') AS dt,
    command,
    cnt
FROM tmp_cmd_usage
WHERE dt = '{{ ds }}' ;
;
