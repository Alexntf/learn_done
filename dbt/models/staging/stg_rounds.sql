WITH final AS (
    SELECT
        id AS round_id,
        season_id,
        CAST(end_time AS DATE) AS end_date,
        CAST(start_time AS DATE) AS start_date,
        name AS round_name,
        season_name As season_name, 
        round
    FROM 
        {{ source("tennis_api", "rounds") }}
)

SELECT * FROM final