WITH base AS (
    SELECT
        id AS league_id,
        name AS league_name,
        most_titles,
        teams_most_titles
    FROM
        {{ source("tennis_api", "leagues") }}
    WHERE json_array_length(teams_most_titles) > 0
),
unnested_leagues AS (
    -- Extract first element
    SELECT
        league_id,
        league_name,
        most_titles,
        0 AS element_index,
        json_extract(teams_most_titles, '$[0].team_id')::INT AS team_id,
        json_extract(teams_most_titles, '$[0].team_name')::VARCHAR AS team_name,
        json_extract(teams_most_titles, '$[0].team_hash_image')::VARCHAR AS team_hash_image
    FROM base
    
    UNION ALL
    
    -- Extract second element (if exists)
    SELECT
        league_id,
        league_name,
        most_titles,
        1 AS element_index,
        json_extract(teams_most_titles, '$[1].team_id')::INT AS team_id,
        json_extract(teams_most_titles, '$[1].team_name')::VARCHAR AS team_name,
        json_extract(teams_most_titles, '$[1].team_hash_image')::VARCHAR AS team_hash_image
    FROM base
    WHERE json_array_length(teams_most_titles) > 1
),

final AS (
    SELECT 
        league_id,
        league_name,
        team_id,
        team_name,
        most_titles,
        team_hash_image
    FROM 
        unnested_leagues
    WHERE 
        team_id IS NOT NULL
)

SELECT * FROM final