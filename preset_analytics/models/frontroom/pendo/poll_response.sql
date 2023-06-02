SELECT
    CAST(SUBSTR(B.visitor_id, 0, INSTR(B.visitor_id, '-') - 1) AS INT) AS user_id,
    A.id AS question_id,
    A.question,
    B.poll_response AS response_id,
    D.poll_text_response AS response,
    B.timestamp,
FROM {{ source('pendo', 'poll') }} AS A
INNER JOIN {{ source('pendo', 'poll_event') }} AS B
    ON A.id = B.poll_id
INNER JOIN (
    -- Getting only latest answer per person/question
    SELECT
        poll_id,
        visitor_id,
        MAX(timestamp) AS max_timestamp,
    FROM {{ source('pendo', 'poll_event') }}
    GROUP BY 1, 2
) AS BB
    ON BB.poll_id = B.poll_id
        AND BB.visitor_id = B.visitor_id
        AND BB.max_timestamp = B.timestamp
LEFT JOIN {{ ref('pendo_poll_response') }} AS D
    ON B.poll_response = D.poll_response
