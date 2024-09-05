from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2, scoreboardv2

from google.cloud import storage

import pandas as pd
import os
from datetime import datetime, timedelta

BUCKET_NAME = "nba_bucket_1_200724"

# Upload file to GCS
def upload_file_to_gcs(bucket_name, source_file_path, source_file_name):
    """Uploads a file to the bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_file_path)
    
        blob.upload_from_filename(source_file_name, if_generation_match=0)
        print(f"File {source_file_name} uploaded.")
    except Exception as e:
        print(f"Failed to upload {source_file_name} to GCS: {e}")

# Define the function to fetch NBA team and player data through history
def fetch_player_team_data():
    try:
        nba_players = players.get_players()
        nba_teams = teams.get_teams()

        players_df = pd.DataFrame(nba_players)
        teams_df = pd.DataFrame(nba_teams)

        current_date = datetime.now().strftime("%Y%m%d")

        if not players_df.empty:
            
            try:
                player_file_name = f"players_{current_date}.csv"
                players_file_path = f"{player_file_name}"

                players_df.to_csv(players_file_path, index=False)

                print(f"Files saved as {players_file_path}")

                upload_player_file_path = f"nba_players/{player_file_name}"

                upload_file_to_gcs(BUCKET_NAME, upload_player_file_path, players_file_path)
                os.remove(players_file_path)
                print(f"File {players_file_path} deleted after upload.")
            except Exception as e:
                print(f"Error processing players data: {e}")
        
        if not teams_df.empty:

            try:
                team_file_name = f"teams_{current_date}.csv"
                teams_file_path = f"{team_file_name}"

                teams_df.to_csv(teams_file_path, index=False)

                print(f"Files saved as {teams_file_path}")

                upload_team_file_path = f"nba_teams/{team_file_name}"

                upload_file_to_gcs(BUCKET_NAME, upload_team_file_path, teams_file_path)
                os.remove(teams_file_path)
                print(f"File {teams_file_path} deleted after upload.")
            except Exception as e:
                print(f"Error processing teams data: {e}")
        else:
            print(f"No games on {current_date}.")
    except Exception as e:
        print(f"Error fetching player or team data from API: {e}")

# Define the function to fetch NBA games for 2023 NBA season and upload them to GCS
def fetch_and_save_team_game_data():
    start_date = datetime.strptime("2023-10-01", "%Y-%m-%d")  # Season start, adjust as necessary
    end_date = datetime.strptime("2024-04-15", "%Y-%m-%d")  # Season end, adjust as necessary
    current_date = start_date
    
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable="2023-24", season_type_nullable="Regular Season")
    games = gamefinder.get_data_frames()[0]

    while current_date <= end_date:
        specific_date = current_date.strftime("%Y-%m-%d")
        
        # Filter the DataFrame to only include games on this specific date
        filtered_games = games[games["GAME_DATE"] == specific_date]
        if not filtered_games.empty:
            file_name = f"nba_teams_stats_{specific_date}.csv"
            file_path = f"{file_name}"
            filtered_games.to_csv(file_path, index=False)
            print(f"Data for games on {specific_date} written to {file_path}.")
            
            upload_file_path = f"teams_games/{file_name}"
            # Upload to GCS and delete file
            upload_file_to_gcs(BUCKET_NAME, upload_file_path, file_path)
            os.remove(file_path)
            print(f"File {file_path} deleted after upload.")
        else:
            print(f"No games on {specific_date}.")

        current_date += timedelta(days=1)

# Define the function to fetch NBA games team data for current date and upload them to GCS
def fetch_team_game_data():
    try:
        current_date = datetime.now().strftime("%m/%d/%Y")
        gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2023-24', season_type_nullable='Regular Season', # Change season_nullable to current season
                                                        date_from_nullable=current_date, date_to_nullable=current_date)
        games = gamefinder.get_data_frames()[0]
        
        if not games.empty:
            file_name = f"nba_teams_stats_{current_date}.csv"
            file_path = f"{file_name}"
            games.to_csv(file_path, index=False)
            print(f"Data for games on {current_date} written to {file_path}.")
            
            upload_file_path = f"teams_games/{file_name}"
            # Upload to GCS and delete file
            upload_file_to_gcs(BUCKET_NAME, upload_file_path, file_path)
            os.remove(file_path)
            print(f"File {file_path} deleted after upload.")
        else:
            print(f"No games on {current_date}.")
    except Exception as e:
        print(f"Error processing games data: {e}")

# Define the function to fetch NBA games player data for 2023 NBA season and upload them to GCS
def fetch_and_save_player_game_data():
    # Define the season start and end dates
    start_date = datetime.strptime("2023-10-01", "%Y-%m-%d")  # Season start, adjust as necessary
    end_date = datetime.strptime("2024-04-15", "%Y-%m-%d")  # Season end, adjust as necessary
    current_date = start_date

    # Loop through each date in the range
    while current_date <= end_date:
        specific_date = current_date.strftime("%Y-%m-%d")

        # Attempt to fetch the scoreboard for the current date
        try:
            scoreboard = scoreboardv2.ScoreboardV2(game_date=specific_date)
            game_header = scoreboard.game_header.get_data_frame()
            game_ids = game_header["GAME_ID"].tolist()

            # Create a list to hold player stats for the day
            all_player_stats = []

            # Loop through each game ID to fetch detailed stats
            for game_id in game_ids:
                try:
                    # Fetch box score for the game
                    boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
                    player_stats_df = boxscore.player_stats.get_data_frame()

                    # Add game ID to the DataFrame
                    player_stats_df["GAME_ID"] = game_id

                    # Append the stats to the list
                    all_player_stats.append(player_stats_df)
                except Exception as e:
                    print(f"An error occurred for game ID {game_id}: {e}")

            # Combine player stats into a single DataFrame
            if all_player_stats:
                all_player_stats_df = pd.concat(all_player_stats, ignore_index=True)

                # Write the combined DataFrame to a CSV file
                file_name = f"nba_player_stats_{specific_date}.csv"
                file_path = f"{file_name}"
                all_player_stats_df.to_csv(file_path, index=False)

                upload_file_path = f"players_games/{file_name}"
                # Upload to GCS and delete file
                upload_file_to_gcs(BUCKET_NAME, upload_file_path, file_path)
                os.remove(file_path)
                print(f"File {file_path} deleted after upload.")
                

        except Exception as e:
            print(f"No data found or an error occurred for {specific_date}: {e}")

        # Increment the date by one day
        current_date += timedelta(days=1)

# Define the function fetch NBA games player data for current date and upload them to GCS
def fetch_player_game_data():
    # Attempt to fetch the scoreboard for the current date
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        scoreboard = scoreboardv2.ScoreboardV2(game_date=current_date)
        game_header = scoreboard.game_header.get_data_frame()
        game_ids = game_header["GAME_ID"].tolist()

        # Create a list to hold player stats for the day
        all_player_stats = []

        # Loop through each game ID to fetch detailed stats
        for game_id in game_ids:
            try:
                # Fetch box score for the game
                boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
                player_stats_df = boxscore.player_stats.get_data_frame()

                # Add game ID to the DataFrame
                player_stats_df["GAME_ID"] = game_id

                # Append the stats to the list
                all_player_stats.append(player_stats_df)
            except Exception as e:
                print(f"An error occurred for game ID {game_id}: {e}")

        # Combine player stats into a single DataFrame
        if all_player_stats:
            all_player_stats_df = pd.concat(all_player_stats, ignore_index=True)

            # Write the combined DataFrame to a CSV file
            file_name = f"nba_player_stats_{current_date}.csv"
            file_path = f"{file_name}"
            all_player_stats_df.to_csv(file_path, index=False)

            upload_file_path = f"players_games/{file_name}"
            # Upload to GCS and delete file
            upload_file_to_gcs(BUCKET_NAME, upload_file_path, file_path)
            os.remove(file_path)
            print(f"File {file_path} deleted after upload.")
            

    except Exception as e:
        print(f"No data found or an error occurred for {current_date}: {e}")
