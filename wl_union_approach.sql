WITH idle_time
    AS (SELECT
            anonymous_id
          , sent_at
          , LAG(sent_at)
                OVER (
                    PARTITION BY anonymous_id
                    ORDER BY sent_at)
          , DATEDIFF(MINUTES, LAG(sent_at)
                                  OVER (
                                      PARTITION BY anonymous_id
                                      ORDER BY sent_at), sent_at) AS idle_time
        FROM "SEGMENT"."DE_WHITELABEL_WEB_APP"."PAGES"
       )
   ,
    sessions
    AS (SELECT
                ROW_NUMBER()
                        OVER (
                            PARTITION BY lag.anonymous_id
                            ORDER BY lag.sent_at)
                || '-'
                || lag.anonymous_id                                    AS session_id
          ,     lag.anonymous_id
          ,     lag.sent_at                                            AS
                                                                          session_start
          ,     ROW_NUMBER()
                        OVER (
                            PARTITION BY lag.anonymous_id
                            ORDER BY lag.sent_at)                      AS
                                                                          session_seq_number
          ,     COALESCE(LEAD(lag.sent_at)
                              OVER (
                                  PARTITION BY lag.anonymous_id
                                  ORDER BY lag.sent_at), '3000-01-01') AS
                                                                          next_session_start
        FROM idle_time AS lag
        WHERE (lag.idle_time > 30
            OR lag.idle_time IS NULL)
       ),
    events_union AS (

    SELECT sent_at
     ,event
    ,split_part(CONTEXT_PAGE_PATH
   , '/'
   , 3) AS listing_id
   , source AS source
   , anonymous_id AS anonymous_id
    FROM "SEGMENT"."DE_WHITELABEL_WEB_APP"."PHONE_LEAD_CTA_BUTTON_CLICK"
    UNION
    SELECT sent_at
    ,event
    ,split_part(CONTEXT_PAGE_PATH
   , '/'
   , 3) AS listing_id
   , source AS source
   , anonymous_id AS anonymous_id FROM
    "SEGMENT"."DE_WHITELABEL_WEB_APP"."CALLBACK_LEAD_CTA_BUTTON_CLICK"
    ),

    report as (
SELECT --s.ANONYMOUS_ID, count(s.ANONYMOUS_ID)
       s.*
  ,    'de'                                    AS country_code
  ,    e.source                                AS source
  ,    e.listing_id
  ,    CASE
           WHEN e.anonymous_id IS NOT NULL AND e.event='phone_lead_cta_button_click' THEN 1
           ELSE 0
       END                                     AS call_lead_flag

  ,    CASE
           WHEN e.anonymous_id IS NOT NULL AND e.event='callback_lead_cta_button_click' THEN 1
           ELSE 0
       END                                     AS callback_lead_flag
--- sum up key events
FROM sessions               s
     LEFT JOIN events_union e
     ON e.anonymous_id = s.anonymous_id
         AND e.sent_at >= s.session_start
         AND e.sent_at < s.next_session_start)
--select * from events_union
--GROUP  BY 1
SELECT *
--     session_id,
--     sum(call_lead_flag),
--     sum(callback_lead_flag)

FROM report
WHERE anonymous_id = 'dbb77a42-dde4-41f7-a782-d1a506cf3eea'
--GROUP BY 1
ORDER BY 1 ASC
---dbb77a42-dde4-41f7-a782-d1a506cf3eea initial example
---'3ba7c53e-7408-41ad-b2d1-d3ac190b4705' example with 90 sessions
