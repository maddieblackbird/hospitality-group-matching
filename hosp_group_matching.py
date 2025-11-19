#!/usr/bin/env python3
"""
Restaurant Hospitality Group Finder
This script processes a CSV of restaurants and uses Claude API with web search to identify 
if each restaurant is part of a larger hospitality group.
"""

import pandas as pd
import requests
import time
import os
import json
from typing import Tuple

# Configuration
INPUT_CSV = "signed_restaurants.csv"
OUTPUT_CSV = "restaurants_with_hospitality_groups.csv"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")  # Set this environment variable

# Rate limiting
REQUEST_DELAY = 2  # seconds between requests (Claude API is generous but let's be respectful)


def search_hospitality_group(restaurant_name: str, location: str = "", domain: str = "") -> Tuple[str, str]:
    """
    Use Claude API with web search to determine if a restaurant is part of a hospitality group.
    
    Args:
        restaurant_name: Name of the restaurant
        location: Geographic location/market (optional)
        domain: Restaurant's domain name (optional)
    
    Returns:
        Tuple of (hospitality_group_name, total_locations)
    """
    if not ANTHROPIC_API_KEY:
        return "ERROR: No API key", ""
    
    # Construct search query for Claude
    location_str = f" in {location}" if location else ""
    domain_str = f" (website: {domain})" if domain else ""
    
    prompt = f"""Search for information about "{restaurant_name}" restaurant{location_str}{domain_str}.

Please determine:
1. Is this restaurant part of a larger hospitality/restaurant group or management company?
2. If yes, what is the exact name of the parent company or restaurant group?
3. Approximately how many total restaurant locations does this group operate?

Respond in this exact format:
Group Name: [exact name of parent company/group, or "Independent" if it's a standalone restaurant]
Total Locations: [number, or "1" if independent, or "Unknown" if you can't find this info]

Be concise and only provide the requested information."""
    
    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1000,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            },
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            answer = result['content'][0]['text'].strip()
            
            # Parse the structured response
            group_name = "Unknown"
            total_locations = ""
            
            for line in answer.split('\n'):
                line = line.strip()
                if line.startswith("Group Name:"):
                    group_name = line.replace("Group Name:", "").strip()
                elif line.startswith("Total Locations:"):
                    total_locations = line.replace("Total Locations:", "").strip()
            
            return group_name, total_locations
        else:
            print(f"API Error {response.status_code}: {response.text}")
            return f"ERROR: {response.status_code}", ""
            
    except Exception as e:
        print(f"Error processing {restaurant_name}: {str(e)}")
        return f"ERROR: {str(e)}", ""


def process_restaurants(input_file: str, output_file: str):
    """
    Process the restaurant CSV file and add hospitality group information.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
    """
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    # Add new columns if they don't exist
    if "Hospitality Group" not in df.columns:
        df["Hospitality Group"] = ""
    if "Total Locations" not in df.columns:
        df["Total Locations"] = ""
    
    total_rows = len(df)
    print(f"Found {total_rows} restaurants to process")
    
    # Process each restaurant
    for idx, row in df.iterrows():
        restaurant_name = row.get("Company name", "")
        location = row.get("Macro Geo (NYC, SF, CHS, DC, LA, NASH, DEN)", "")
        domain = row.get("Company Domain Name", "")
        
        # Skip if already processed (has a value)
        if pd.notna(df.at[idx, "Hospitality Group"]) and df.at[idx, "Hospitality Group"]:
            print(f"[{idx+1}/{total_rows}] Skipping {restaurant_name} (already processed)")
            continue
        
        print(f"[{idx+1}/{total_rows}] Searching for: {restaurant_name}...")
        
        hospitality_group, total_locations = search_hospitality_group(restaurant_name, location, domain)
        df.at[idx, "Hospitality Group"] = hospitality_group
        df.at[idx, "Total Locations"] = total_locations
        
        print(f"  → Result: {hospitality_group} ({total_locations} locations)")
        
        # Save progress after each row (in case of interruption)
        df.to_csv(output_file, index=False)
        
        # Rate limiting
        if idx < total_rows - 1:  # Don't delay after last item
            time.sleep(REQUEST_DELAY)
    
    print(f"\n✓ Complete! Results saved to {output_file}")
    
    # Print summary statistics
    print("\n=== Summary ===")
    if "Hospitality Group" in df.columns:
        total = len(df)
        independent = len(df[df["Hospitality Group"] == "Independent"])
        groups = len(df[(df["Hospitality Group"] != "Independent") & 
                       (df["Hospitality Group"] != "") & 
                       (~df["Hospitality Group"].str.contains("ERROR", na=False))])
        errors = len(df[df["Hospitality Group"].str.contains("ERROR", na=False)])
        
        print(f"Total restaurants: {total}")
        print(f"Independent: {independent} ({independent/total*100:.1f}%)")
        print(f"Part of groups: {groups} ({groups/total*100:.1f}%)")
        if errors > 0:
            print(f"Errors: {errors}")


def main():
    """Main execution function."""
    # Check for API key
    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set!")
        print("\nPlease set your API key using:")
        print("  export ANTHROPIC_API_KEY='your-api-key-here'")
        print("\nOr edit the script to add it directly (not recommended for production)")
        return
    
    # Check if input file exists
    if not os.path.exists(INPUT_CSV):
        print(f"ERROR: Input file '{INPUT_CSV}' not found!")
        print(f"Please ensure the CSV file is in the same directory as this script.")
        return
    
    print("=" * 60)
    print("Restaurant Hospitality Group Finder")
    print("=" * 60)
    print(f"Input file: {INPUT_CSV}")
    print(f"Output file: {OUTPUT_CSV}")
    print(f"Request delay: {REQUEST_DELAY} seconds")
    print("=" * 60)
    print()
    
    # Process the restaurants
    process_restaurants(INPUT_CSV, OUTPUT_CSV)


if __name__ == "__main__":
    main()