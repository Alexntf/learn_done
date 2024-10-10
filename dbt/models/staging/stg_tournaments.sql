WITH final AS (
    SELECT
        id AS tournament_id,
        class_id,
        name AS tournament_name, 
        class_name,
        importance,
    FROM 
        {{ source("tennis_api", "tournaments")}}
)

SELECT * FROM final