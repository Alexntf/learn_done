WITH final AS (
    SELECT
        id AS tournament_id,
        class_id,
        name, 
        class_name,
        importance,
    FROM 
        {{ source("tennis_api", "tournaments")}}
)

SELECT * FROM final