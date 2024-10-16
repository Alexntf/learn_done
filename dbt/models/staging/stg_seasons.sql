WITH final AS (
    SELECT
        id AS season_id,
        league_id,
        start_time AS start_date,
        year,
        name AS season_name,
        league_name, 
        league_hash_image,
    FROM 
        {{ source("tennis_api", "seasons" )}}
)

SELECT * FROM final