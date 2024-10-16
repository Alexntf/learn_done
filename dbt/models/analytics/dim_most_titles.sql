WITH final AS (
    SELECT
        league_id,
        team_id,
        league_name,
        team_name,
        most_titles,
        team_hash_image
    FROM
        {{ref("stg_most_titles")}}
)

SELECT * FROM final