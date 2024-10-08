WITH final as (
    select
        id AS league_id,
        name AS league_name,
        importance,
        current_champion_team_id,
        current_champion_team_name,
        current_champion_team_hash_image,
        most_titles,
        ground,
        number_of_sets,
        max_points,
        primary_color,
        secondary_color,
        start_league,
        end_league,
        hash_image,
        class_id,
        class_name,
        teams_most_titles
    FROM
        {{ source('tennis_api', 'leagues') }}
)

SELECT * FROM final