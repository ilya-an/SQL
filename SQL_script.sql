WITH message_data AS (
    SELECT 
        message_id,
        CASE 
            WHEN created_by = 0 THEN 'incoming_chat_message'
            ELSE type
        END AS type,
        entity_id,
        created_by,
        created_at_datetime,
        LAG(created_at_datetime) OVER (PARTITION BY entity_id ORDER BY created_at_datetime) AS prev_datetime,
        LAG(type) OVER (PARTITION BY entity_id ORDER BY created_at_datetime) AS prev_type
    FROM (
        SELECT 
            message_id,
            type,
            entity_id,
            created_by,
            TO_TIMESTAMP(created_at) AS created_at_datetime
        FROM test.chat_messages
    ) AS q1
),
time_diffs AS (
    SELECT
        created_by,
        entity_id,
        created_at_datetime,
        prev_datetime,
        CASE 
            WHEN (prev_datetime::time BETWEEN '00:00:00' AND '09:30:00') AND created_at_datetime::time >= '09:30:00' THEN 
                created_at_datetime::time - '09:30:00'
            WHEN prev_datetime < DATE_TRUNC('day', created_at_datetime) AND created_at_datetime >= DATE_TRUNC('day', created_at_datetime) + INTERVAL '09:30:00' THEN 
                (created_at_datetime - prev_datetime - INTERVAL '09:30:00')
            ELSE
                (created_at_datetime - prev_datetime)
        END AS time_diff
    FROM message_data
    WHERE type = 'outgoing_chat_message'
      AND prev_type != 'outgoing_chat_message'
      AND created_at_datetime::time NOT BETWEEN '00:00:00' AND '09:30:00'
)
SELECT
    q6.mop_id,
    q6.name_mop,
    q7.rop_name,
    ROUND(
        EXTRACT(EPOCH FROM AVG(time_diff)) / 60,  -- Преобразуем среднее время в минуты
        1
    ) AS avg_minutes_for_answer
FROM time_diffs
RIGHT JOIN (
    SELECT 
        mop_id, 
        name_mop, 
        CAST(rop_id AS INTEGER) AS rop_id
    FROM test.managers
) AS q6
ON time_diffs.created_by = q6.mop_id
JOIN (
    SELECT *
    FROM test.rops
) AS q7
ON q6.rop_id = q7.rop_id
GROUP BY q6.mop_id, q6.name_mop, q7.rop_name  -- Группировка по mop_id, name_mop и rop_name
ORDER BY avg_minutes_for_answer;