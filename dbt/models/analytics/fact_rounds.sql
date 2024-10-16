WITH final AS (
    SELECT
        round_id,
        season_id,
        end_date,
        start_date,
        round_name,
        season_name, 
        round
    FROM 
        {{ source("tennis_api", "rounds") }}
)

SELECT * FROM final