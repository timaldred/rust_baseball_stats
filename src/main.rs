// install tools

// this one reads cvs files
use csv::ReaderBuilder;

// this one helps import cvs data
use serde::Deserialize;

// this one is a standard error handler
use std::error::Error;

// this one helps us work with different file paths
use std::path::Path;

// tool for looking up data
use std::collections::HashMap;

// clap is what reads command line arguments, it also needs adding as a dependency to cargo.toml
use clap::{Parser, Subcommand};

// set up the framework for the data we're going to import
#[derive(Debug, Deserialize, Clone)]
struct PlayerSeason {
    season: u32,
    first_name: Option<String>,  // option because some are null
    last_name: String,
    link: String,
    position: String,
    team: String,
    games_played: u32,
    at_bats: u32,
    runs: u32,
    hits: u32,
    doubles: u32,
    triples: u32,
    homeruns: u32,
    rbi: String,  // should be a number, but the raw data is string, we'll fix this later
    walks: u32,
    strikeouts: Option<f64>,  // some are null
    stolen_bases: String,
    caught_stealing: String,
    batting_average: f64,
    on_base_percentage: String,
    slugging_percentage: f64,
    on_base_plus_slugging: String,
}

// create a new framework with the correct formats
#[derive(Debug, Clone)]
struct CleanPlayerSeason {
    season: u32,
    first_name: Option<String>,
    last_name: String,
    link: String,
    position: String,
    team: String,
    games_played: u32,
    at_bats: u32,
    runs: u32,
    hits: u32,
    doubles: u32,
    triples: u32,
    homeruns: u32,
    rbi: Option<u32>,  // now a proper number (or "none" if missing)
    walks: u32,
    strikeouts: Option<f64>,
    stolen_bases: Option<u32>,  // now a proper number
    caught_stealing: Option<u32>,  // now a proper number
    batting_average: f64,
    on_base_percentage: Option<f64>,  // now a proper number
    slugging_percentage: f64,
    on_base_plus_slugging: Option<f64>,  // now a proper number
}

// pair of functions to convert messy string data to clean numbers
fn parse_optional_number(value: &str) -> Option<u32> {
    if value == "--" || value.trim().is_empty() {
        None  // set as missing data
    } else {
        value.trim().parse().ok()  // try to convert to number
    }
}

fn parse_optional_float(value: &str) -> Option<f64> {
    if value == "--" || value.trim().is_empty() {
        None  // set as missing data
    } else {
        value.trim().parse().ok()  // try to convert to float
    }
}

// function to convert raw data to clean data
fn clean_player_data(raw: PlayerSeason) -> CleanPlayerSeason {
    CleanPlayerSeason {
        season: raw.season,
        first_name: raw.first_name,
        last_name: raw.last_name,
        link: raw.link,
        position: raw.position,
        team: raw.team,
        games_played: raw.games_played,
        at_bats: raw.at_bats,
        runs: raw.runs,
        hits: raw.hits,
        doubles: raw.doubles,
        triples: raw.triples,
        homeruns: raw.homeruns,
        rbi: parse_optional_number(&raw.rbi),
        walks: raw.walks,
        strikeouts: raw.strikeouts,
        stolen_bases: parse_optional_number(&raw.stolen_bases),
        caught_stealing: parse_optional_number(&raw.caught_stealing),
        batting_average: raw.batting_average,
        on_base_percentage: parse_optional_float(&raw.on_base_percentage),
        slugging_percentage: raw.slugging_percentage,
        on_base_plus_slugging: parse_optional_float(&raw.on_base_plus_slugging),
    }
}

// the main function
fn main() -> Result<(), Box<dyn Error>> {

    // read and parse command line arguments
    let cli = Cli::parse();

    println!("Loading baseball data...");
    
    // tell it where the data is
    let file_path = "mlb_season_data.csv";
    
    // check if it exists
    if !Path::new(file_path).exists() {
        println!("Error: {} not found. Please put your CSV file in the project root folder.", file_path);
        return Ok(());
    }
    
    // create CSV reader
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)?;
    
    // create a new empty list called raw_records
    let mut raw_records = Vec::new();
    let mut error_count = 0;
    
    // read each record
    for (line_num, result) in reader.deserialize().enumerate() {
        match result {
            Ok(record) => {
                let player: PlayerSeason = record;
                raw_records.push(player);
            }
            Err(e) => {
                error_count += 1;
                if error_count <= 5 { 
                    println!("Error on line {}: {}", line_num + 2, e);
                }
            }
        }
    }

    println!("Successfully loaded {} raw records", raw_records.len());
    
    // clean the data
    println!("Cleaning data...");
    let mut clean_records = Vec::new();
    
    for raw_record in &raw_records {
        let clean_record = clean_player_data(raw_record.clone());
        clean_records.push(clean_record);
    }

    println!("Successfully cleaned {} records", clean_records.len());

#[derive(Debug, Clone)]
struct AggregatedPlayer {
    first_name: String,
    last_name: String,
    first_season: u32,      // lowest season number
    last_season: u32,       // highest season number  
    link: String,
    seasons_played: u32,    // count of seasons
    positions: String, // all unique positions
    teams: String,     // all unique teams
    total_games_played: u32,
    total_at_bats: u32,
    total_runs: u32,
    total_hits: u32,
    total_doubles: u32,
    total_triples: u32,
    total_homeruns: u32,
    total_rbi: u32,
    total_walks: u32,
    total_strikeouts: f64,
    total_stolen_bases: u32,
    total_caught_stealing: u32,
}

// group players by their unique link
println!("Grouping players by the link column...");
// create a new data set, using strings (vecs) from the cleanplayerseason dataset as the identifiers, but for now it's blank
let mut player_groups: HashMap<String, Vec<CleanPlayerSeason>> = HashMap::new();

// for every row in the clean_players dataset
for player in &clean_records {
    let link = player.link.clone();
    // either add it to an existing record in the player_groups dataset (where it matches the link column) or create a new record
    player_groups.entry(link).or_insert(Vec::new()).push(player.clone());
}

println!("Found {} unique players", player_groups.len());


// populate aggregated player records
println!("Creating aggregated player records...");
let mut aggregated_players = Vec::new();

for (link, seasons) in &player_groups {
    // get basic info from first season
    let first_season_record = &seasons[0];
    
    // find min and max seasons
    let first_season = seasons.iter().map(|s| s.season).min().unwrap();
    let last_season = seasons.iter().map(|s| s.season).max().unwrap();
    
    // collect unique positions and teams
    let mut unique_positions = Vec::new();
    let mut unique_teams = Vec::new();
    
    for season in seasons {
        if !unique_positions.contains(&season.position) {
            unique_positions.push(season.position.clone());
        }
        if !unique_teams.contains(&season.team) {
            unique_teams.push(season.team.clone());
        }
    }

    // calculate career totals - iterate through each line and add them up
    let total_games_played: u32 = seasons.iter().map(|s| s.games_played).sum();
    let total_at_bats: u32 = seasons.iter().map(|s| s.at_bats).sum();
    let total_runs: u32 = seasons.iter().map(|s| s.runs).sum();
    let total_hits: u32 = seasons.iter().map(|s| s.hits).sum();
    let total_doubles: u32 = seasons.iter().map(|s| s.doubles).sum();
    let total_triples: u32 = seasons.iter().map(|s| s.triples).sum();
    let total_homeruns: u32 = seasons.iter().map(|s| s.homeruns).sum();
    let total_walks: u32 = seasons.iter().map(|s| s.walks).sum();
    
    // handle the optional fields (treat None as 0)
    let total_rbi: u32 = seasons.iter().map(|s| s.rbi.unwrap_or(0)).sum();
    let total_strikeouts: f64 = seasons.iter().map(|s| s.strikeouts.unwrap_or(0.0)).sum();
    let total_stolen_bases: u32 = seasons.iter().map(|s| s.stolen_bases.unwrap_or(0)).sum();
    let total_caught_stealing: u32 = seasons.iter().map(|s| s.caught_stealing.unwrap_or(0)).sum();

    // populate the aggregated player record
    let aggregated_player = AggregatedPlayer {
        link: link.clone(),
        first_name: first_season_record.first_name.as_deref().unwrap_or("N/A").to_string(),
        last_name: first_season_record.last_name.clone(),
        first_season,
        last_season,
        seasons_played: seasons.len() as u32,
        positions: unique_positions.join(", "),
        teams: unique_teams.join(", "),
        total_games_played,
        total_at_bats,
        total_runs,
        total_hits,
        total_doubles,
        total_triples,
        total_homeruns,
        total_rbi,
        total_walks,
        total_strikeouts,
        total_stolen_bases,
        total_caught_stealing,
    };
    
    aggregated_players.push(aggregated_player);
}


// reading the command line arguments  
#[derive(Parser)]
#[command(name = "baseball-stats")]
#[command(about = "A CLI tool for analyzing baseball statistics")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

// define the available commands
#[derive(Subcommand)]
enum Commands {
    /// show home run records
    Homeruns,
    /// show season records 
    Seasons,
    /// show career records
    Careers,
}


    // handle the command line argument
    match cli.command {
        Some(Commands::Homeruns) => {

            // create top 10 home run seasons
            println!();

            // sort players by home runs (highest first)
            let mut sorted_by_homeruns = clean_records.clone();
            sorted_by_homeruns.sort_by(|a, b| b.homeruns.cmp(&a.homeruns));

            // take the top 10
            let top_10_homeruns = &sorted_by_homeruns[0..10];

            // display the results
            println!("\nTop 10 home runs in a season:");
            println!("{:<4} {:<15} {:<15} {:<6} {:<8} {:<3}", "Rank", "First Name", "Last Name", "Team", "Season", "HR");
            println!("{}", "-".repeat(60));

            for (i, player) in top_10_homeruns.iter().enumerate() {
                let first_name = player.first_name.as_deref().unwrap_or("N/A");
                println!("{:<4} {:<15} {:<15} {:<6} {:<8} {:<3}", 
                        i + 1, 
                        first_name, 
                        player.last_name, 
                        player.team, 
                        player.season, 
                        player.homeruns);
            }
            
            
            
            // create top 10 homerun career
            println!();

            // sort players by homeruns (highest first)
            let mut sorted_career_by_homeruns = aggregated_players.clone();
            sorted_career_by_homeruns.sort_by(|a, b| b.total_homeruns.cmp(&a.total_homeruns));

            // take the top 10
            let top_10_career_homeruns = &sorted_career_by_homeruns[0..10];

            // display the results
            println!("\nTop 10 homeruns in a career:");
            println!();
            println!("{:<4} {:<15} {:<15} {:<6} {:<6} {:<6} {:<3}", "Rank", "First Name", "Last Name", "From", "To", "Total", "Home runs");
            println!("{}", "-".repeat(67));

            for (i, player) in top_10_career_homeruns.iter().enumerate() {
                println!("{:<4} {:<15} {:<15} {:<6} {:<6} {:<6} {:<3}", 
                        i + 1, 
                        player.first_name, 
                        player.last_name, 
                        player.first_season, 
                        player.last_season,
                        player.seasons_played,
                        player.total_homeruns);
    }
            


        }
        Some(Commands::Seasons) => {
            
            // create top 10 hit seasons
            println!();

            // sort players by hits (highest first)
            let mut sorted_by_hits = clean_records.clone();
            sorted_by_hits.sort_by(|a, b| b.hits.cmp(&a.hits));

            // take the top 10
            let top_10_hits = &sorted_by_hits[0..10];

            // display the results
            println!("\nTop 10 hits in a season:");
            println!("{:<4} {:<15} {:<15} {:<6} {:<8} {:<3}", "Rank", "First Name", "Last Name", "Team", "Season", "Hits");
            println!("{}", "-".repeat(60));

            for (i, player) in top_10_hits.iter().enumerate() {
                let first_name = player.first_name.as_deref().unwrap_or("N/A");
                println!("{:<4} {:<15} {:<15} {:<6} {:<8} {:<3}", 
                        i + 1, 
                        first_name, 
                        player.last_name, 
                        player.team, 
                        player.season, 
                        player.hits);
            }
        }
        Some(Commands::Careers) => {
            
            // create top 10 games played
            println!();

            // sort players by homeruns (highest first)
            let mut sorted_career_by_games = aggregated_players.clone();
            sorted_career_by_games.sort_by(|a, b| b.total_games_played.cmp(&a.total_games_played));

            // take the top 10
            let top_10_career_games = &sorted_career_by_games[0..10];

            // display the results
            println!("\nTop 10 games played in a career:");
            println!("{:<4} {:<15} {:<15} {:<6} {:<6} {:<3}", "Rank", "First Name", "Last Name", "From", "To", "Games Played");
            println!("{}", "-".repeat(63));

            for (i, player) in top_10_career_games.iter().enumerate() {
                println!("{:<4} {:<15} {:<15} {:<6} {:<6} {:<3}", 
                        i + 1, 
                        player.first_name, 
                        player.last_name, 
                        player.first_season, 
                        player.last_season,
                        player.total_games_played);
            }
        }
        
        None => {
            println!("Baseball Statistics Tool");
            println!("========================");
            println!();
            println!("Available commands:");
            println!("  homeruns  - Show home run records (single season and career)");
            println!("  seasons   - Show single season records");  
            println!("  careers   - Show career records");
            println!();
            println!("Usage: cargo run -- <command>");
            println!("For more help: cargo run -- --help");
        }
    }


    Ok(())
}