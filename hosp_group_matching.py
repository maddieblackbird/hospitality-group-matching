#!/usr/bin/env python3
"""
Restaurant Hospitality Group Finder
This script processes a CSV of restaurants and uses Perplexity API to identify 
if each restaurant is part of a larger hospitality group.
"""

import pandas as pd
import requests
import time
import os
from typing import Optional

# Configuration
INPUT_CSV = "signed_restaurants.csv"
OUTPUT_CSV = "restaurants_with_hospitality_groups.csv"
PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")  # Set this environment variable

# Rate limiting (adjust based on your Perplexity plan)
REQUEST_DELAY = 1  # seconds between requests


def search_hospitality_group(restaurant_name: str, location: str = "", domain: str = "") -> str:
    """
    Search Perplexity API to determine if a restaurant is part of a hospitality group.
    
    Args:
        restaurant_name: Name of the restaurant
        location: Geographic location/market (optional)
        domain: Restaurant's domain name (optional)
    
    Returns:
        Name of hospitality group or "Independent" if none found
    """
    if not PERPLEXITY_API_KEY:
        return "ERROR: No API key"
    
    # Construct search query
    query_parts = [f'"{restaurant_name}"']
    if domain:
        query_parts.append(f'domain "{domain}"')
    if location:
        query_parts.append(f'in {location}')
    
    query = (
        f"Is {' '.join(query_parts)} part of a restaurant group or hospitality group? "
        f"If yes, provide ONLY the name of the parent company/restaurant group. "
        f"If it's an independent restaurant with no parent company, respond with 'Independent'. "
        f"Keep your answer to just the group name or 'Independent'."
    )
    
    try:
        response = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={
                "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "sonar",  # Fast and affordable model for quick searches (as of Feb 2025)
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a concise assistant that identifies restaurant ownership. Respond with only the hospitality group name or 'Independent'."
                    },
                    {
                        "role": "user",
                        "content": query
                    }
                ],
                "temperature": 0.1,  # Low temperature for more deterministic results
                "max_tokens": 100
            },
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            answer = result['choices'][0]['message']['content'].strip()
            
            # Clean up the response
            answer = answer.replace('"', '').strip()
            
            # If response is too long or contains explanation, try to extract just the name
            if len(answer) > 100:
                # Look for common patterns
                if "independent" in answer.lower():
                    return "Independent"
                # Try to get first sentence or line
                answer = answer.split('.')[0].split('\n')[0].strip()
            
            return answer if answer else "Unknown"
        else:
            print(f"API Error {response.status_code}: {response.text}")
            return f"ERROR: {response.status_code}"
            
    except Exception as e:
        print(f"Error processing {restaurant_name}: {str(e)}")
        return f"ERROR: {str(e)}"


def process_restaurants(input_file: str, output_file: str):
    """
    Process the restaurant CSV file and add hospitality group information.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
    """
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    # Add new column if it doesn't exist
    if "Hospitality Group" not in df.columns:
        df["Hospitality Group"] = ""
    
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
        
        hospitality_group = search_hospitality_group(restaurant_name, location, domain)
        df.at[idx, "Hospitality Group"] = hospitality_group
        
        print(f"  → Result: {hospitality_group}")
        
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
    if not PERPLEXITY_API_KEY:
        print("ERROR: PERPLEXITY_API_KEY environment variable not set!")
        print("\nPlease set your API key using:")
        print("  export PERPLEXITY_API_KEY='your-api-key-here'")
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