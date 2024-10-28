WITH final AS (
    SELECT
        sms.match_id,
        fma.home_team_id,
        fma.away_team_id,
        fma.home_team_name,
        fma.away_team_name,
        sms.period,
        sms.type,
        sms.category,
        sms.home_team,
        sms.away_team
    FROM
        {{ ref("stg_match_statistics") }} sms
    LEFT JOIN 
        {{ ref("fact_matches") }} fma
        ON fma.match_id = sms.match_id
)

SELECT * FROM final