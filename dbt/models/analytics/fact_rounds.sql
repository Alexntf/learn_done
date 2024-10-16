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
        {{ ref("stg_rounds") }}
)

SELECT * FROM final