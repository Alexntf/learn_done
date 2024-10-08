WITH final AS (
    SELECT
        DISTINCT
        league_id,
        current_champion_team_id,
        start_league, 
        end_league,
        EXTRACT(YEAR FROM start_league) AS year,
        league_name,
        current_champion_team_name,
        current_champion_team_hash_image,
        most_titles,
        ground,
        number_of_sets,
        max_points,
        primary_color,
        secondary_color
    FROM
        {{ref("stg_leagues")}}
)

SELECT * FROM final