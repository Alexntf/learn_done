# main.py

from extract_load import (
    tournaments,
    leagues,
    seasons,
    rounds,
    matchs
)

if __name__ == "__main__":
    print("Starting ETL pipeline...")
    
    print("1. Processing tournaments...")
    tournaments.main()
    
    print("2. Processing leagues...")
    leagues.main()
    
    print("3. Processing seasons...")
    seasons.main()
    
    print("4. Processing rounds...")
    rounds.main()

    print("5. Processing matches...")
    matches.main()
    
    print("ETL pipeline completed!")