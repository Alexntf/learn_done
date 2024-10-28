WITH final AS (
    SELECT
        match_id,
        period,
        type, 
        category,
        home_team,
        away_team,
    FROM
        {{ source("tennis_api", "match_statistics")}}
)

SELECT * FROM final 