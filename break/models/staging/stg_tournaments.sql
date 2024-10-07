WITH final AS (
    SELECT
        id,
        class_id,
        name, 
        class_name,
        importance,
    FROM 
        {{ source("tennis_api", "tournaments")}}
)

SELECT * FROM final