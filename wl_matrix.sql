WITH idle_time AS
(
         SELECT   anonymous_id,
                  sent_at,
                  path,
                  Lag(sent_at) OVER( partition BY anonymous_id ORDER BY sent_at),
                  Datediff(minutes, Lag(sent_at) OVER( partition BY anonymous_id ORDER BY sent_at), sent_at) AS idle_time
         FROM     "SEGMENT"."DE_WHITELABEL_WEB_APP"."PAGES" --WHERE    anonymous_id = 'd74141e0-1aa6-4261-a177-7605592dbbad' limit 100
),                                                          --'10519ccd-6427-4adc-9568-03b25a2e32c9' --14sessions d74141e0-1aa6-4261-a177-7605592dbbad
sessions AS
(
         SELECT   Row_number() OVER( partition BY lag.anonymous_id ORDER BY lag.sent_at)
                           || '-'
                           || lag.anonymous_id AS session_id,
                  lag.anonymous_id,
                  lag.sent_at                                                                                          AS session_start,
                  Row_number() OVER( partition BY lag.anonymous_id ORDER BY lag.sent_at)                               AS session_seq_number,
                  COALESCE(Lead(lag.sent_at) OVER ( partition BY lag.anonymous_id ORDER BY lag.sent_at), '3000-01-01') AS next_session_start
         FROM     idle_time                                                                                            AS lag
         WHERE    (
                           lag.idle_time > 30
                  OR       lag.idle_time IS NULL )), call_leads AS
(
          SELECT    s.*,
                    e.source AS source,
                    'de' AS country_code ,
                    split_part(e.CONTEXT_PAGE_PATH,'/',3) as listing_id,
                    CASE
                              WHEN e.anonymous_id IS NULL THEN 0
                              ELSE 1
                    END AS call_lead_flag,
                    0   AS callback_lead_flag
          FROM      sessions s
          LEFT JOIN "SEGMENT"."DE_WHITELABEL_WEB_APP"."PHONE_LEAD_CTA_BUTTON_CLICK" e
          ON        e.anonymous_id = s.anonymous_id
          AND       e.sent_at >= s.session_start
          AND       e.sent_at < s.next_session_start)



          , callback_leads AS
(
          SELECT    s.* ,
                    f.source AS source,
                    'de' AS country_code ,
                    split_part(f.CONTEXT_PAGE_PATH,'/',3) as listing_id,
                    0    AS call_lead_flag ,
                    CASE
                              WHEN f.anonymous_id IS NULL THEN 0
                              ELSE 1
                    END AS callback_lead_flag
          FROM      sessions s
          LEFT JOIN "SEGMENT"."DE_WHITELABEL_WEB_APP"."CALLBACK_LEAD_CTA_BUTTON_CLICK" f
          ON        f.anonymous_id = s.anonymous_id
          AND       f.sent_at >= s.session_start
          AND       f.sent_at < s.next_session_start), report_union AS
(
       SELECT *
       FROM   call_leads
       UNION ALL
       SELECT *
       FROM   callback_leads)
SELECT   *
FROM     report_union
--WHERE    anonymous_id='dbb77a42-dde4-41f7-a782-d1a506cf3eea'
ORDER BY 3 DESC
