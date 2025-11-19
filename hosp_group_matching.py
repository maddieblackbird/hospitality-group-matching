#!/usr/bin/env python3
"""
Restaurant Hospitality Group Finder
This script processes a CSV of restaurants and uses Perplexity's sonar-pro model
to identify if each restaurant is part of a larger hospitality group.
"""

import pandas as pd
import requests
import time
import os
import json
import re
from typing import Tuple

# Configuration
INPUT_CSV = "signed_restaurants_test.csv"
OUTPUT_CSV = "restaurants_with_hospitality_groups.csv"
PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")
SERPER_API_KEY = os.environ.get("SERPER_API_KEY", "")  # For Google search verification

# Rate limiting
REQUEST_DELAY = 2  # seconds between requests
SERPER_DELAY = 1  # seconds between Serper requests


def search_hospitality_group(restaurant_name: str, location: str = "", domain: str = "") -> Tuple[str, str]:
    """
    Use Perplexity's sonar-pro model to determine if a restaurant is part of a hospitality group.
    
    Args:
        restaurant_name: Name of the restaurant
        location: Geographic location/market (optional)
        domain: Restaurant's domain name (optional)
    
    Returns:
        Tuple of (hospitality_group_name, total_locations)
    """
    if not PERPLEXITY_API_KEY:
        return "ERROR: No API key", ""
    
    # Construct detailed search query
    location_str = f" in {location}" if location else ""
    domain_str = f" (website: {domain})" if domain else ""
    
    query = f"""Research the restaurant "{restaurant_name}"{location_str}{domain_str}.

Determine:
1. Is this restaurant part of a larger hospitality group, restaurant group, or management company?
2. If yes, what is the exact name of the parent company?
3. How many total restaurant locations does this group operate?

Format your response exactly like this:
Group Name: [exact company name, or "Independent" if standalone]
Total Locations: [number, or "1" if independent, or "Unknown" if unclear]

Only provide these two lines. Be thorough in your research."""
    
    try:
        response = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={
                "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "sonar-pro",  # Better model for deeper research
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a restaurant industry researcher. Search thoroughly to identify restaurant ownership and parent companies. Always respond in the exact format requested."
                    },
                    {
                        "role": "user",
                        "content": query
                    }
                ],
                "temperature": 0.2,
                "max_tokens": 300,
                "search_domain_filter": ["perplexity.ai"],  # Use Perplexity's search
                "return_citations": True
            },
            timeout=45
        )
        
        if response.status_code == 200:
            result = response.json()
            answer = result['choices'][0]['message']['content'].strip()
            
            # Parse the structured response
            group_name = "Unknown"
            total_locations = "Unknown"
            
            for line in answer.split('\n'):
                line = line.strip()
                if line.startswith("Group Name:"):
                    group_name = line.replace("Group Name:", "").strip()
                    # Clean up any markdown or extra characters
                    group_name = group_name.replace("**", "").replace("*", "").strip()
                elif line.startswith("Total Locations:"):
                    total_locations = line.replace("Total Locations:", "").strip()
                    total_locations = total_locations.replace("**", "").replace("*", "").strip()
            
            # If we didn't get structured response, try to parse from natural language
            if group_name == "Unknown" and "independent" in answer.lower():
                group_name = "Independent"
                total_locations = "1"
            
            return group_name, total_locations
        else:
            print(f"API Error {response.status_code}: {response.text}")
            return f"ERROR: {response.status_code}", ""
            
    except Exception as e:
        print(f"Error processing {restaurant_name}: {str(e)}")
        return f"ERROR: {str(e)}", ""


def verify_with_serper(restaurant_name: str, location: str = "", domain: str = "") -> Tuple[str, str]:
    """
    Use Serper (Google Search) to verify if a restaurant marked as Independent is actually part of a group.
    
    Args:
        restaurant_name: Name of the restaurant
        location: Geographic location/market (optional)
        domain: Restaurant's domain name (optional)
    
    Returns:
        Tuple of (hospitality_group_name, total_locations) or ("Independent", "1") if verification confirms independence
    """
    if not SERPER_API_KEY:
        return "Independent", "1"  # If no API key, assume Perplexity was correct
    
    # Search for restaurant group ownership info
    location_str = f" {location}" if location else ""
    search_query = f'"{restaurant_name}"{location_str} restaurant group owner parent company hospitality'
    
    try:
        response = requests.post(
            "https://google.serper.dev/search",
            headers={
                "X-API-KEY": SERPER_API_KEY,
                "Content-Type": "application/json"
            },
            json={
                "q": search_query,
                "num": 10  # Get top 10 results
            },
            timeout=30
        )
        
        if response.status_code != 200:
            return "Independent", "1"
            
        result = response.json()
        
        # Collect all relevant text from search results
        search_snippets = []
        
        # Check organic results
        for item in result.get("organic", [])[:8]:  # Look at top 8 results
            title = item.get('title', '')
            snippet = item.get('snippet', '')
            if title or snippet:
                search_snippets.append(f"{title}. {snippet}")
        
        # Check knowledge graph if present
        if "knowledgeGraph" in result:
            kg = result["knowledgeGraph"]
            kg_text = f"{kg.get('title', '')} {kg.get('description', '')}".strip()
            if kg_text:
                search_snippets.append(kg_text)
        
        # If we have search results, use Perplexity to analyze them
        if search_snippets and PERPLEXITY_API_KEY:
            combined_snippets = "\n".join(search_snippets[:5])  # Use top 5 snippets
            
            analysis_prompt = f"""Based on these Google search results about "{restaurant_name}"{location_str}, determine if this restaurant is part of a hospitality/restaurant group:

SEARCH RESULTS:
{combined_snippets}

Respond in this exact format:
Group Name: [exact name of the parent company/restaurant group, or "Independent" if standalone]
Total Locations: [number of total locations the group operates, or "1" if independent, or "Unknown" if unclear]

Be specific with the group name if one is found."""
            
            try:
                analysis_response = requests.post(
                    "https://api.perplexity.ai/chat/completions",
                    headers={
                        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": "sonar-pro",
                        "messages": [
                            {
                                "role": "system",
                                "content": "You are a restaurant industry analyst. Analyze search results to identify restaurant group ownership. Always respond in the exact format requested."
                            },
                            {
                                "role": "user",
                                "content": analysis_prompt
                            }
                        ],
                        "temperature": 0.2,
                        "max_tokens": 250
                    },
                    timeout=30
                )
                
                if analysis_response.status_code == 200:
                    analysis_result = analysis_response.json()
                    answer = analysis_result['choices'][0]['message']['content'].strip()
                    
                    # Parse the structured response
                    group_name = "Independent"
                    total_locations = "1"
                    
                    for line in answer.split('\n'):
                        line = line.strip()
                        if line.startswith("Group Name:"):
                            group_name = line.replace("Group Name:", "").strip()
                            group_name = group_name.replace("**", "").replace("*", "").strip()
                        elif line.startswith("Total Locations:"):
                            total_locations = line.replace("Total Locations:", "").strip()
                            total_locations = total_locations.replace("**", "").replace("*", "").strip()
                    
                    return group_name, total_locations
                    
            except Exception as e:
                print(f"    Error analyzing search results: {str(e)}")
        
        # Fallback: Simple pattern matching if Perplexity analysis fails
        all_text = " ".join(search_snippets).lower()
        
        # Look for indicators of a restaurant group
        group_indicators = [
            "restaurant group", "hospitality group", "restaurant collection",
            "parent company", "owned by", "operates", "portfolio",
            "management company", "dining group", "restaurant family"
        ]
        
        has_group_indicator = any(indicator in all_text for indicator in group_indicators)
        
        if has_group_indicator and restaurant_name.lower() in all_text:
            # Look for specific group names using expanded list
            # Common patterns that indicate group names
            patterns = [
                r'(?:owned by|part of|operates|managed by)\s+([A-Z][A-Za-z\s&]+(?:Group|Hospitality|Restaurant|Management|Collection|Dining|Company|LLC|Inc))',
                r'([A-Z][A-Za-z\s&]+(?:Group|Hospitality|Restaurant|Management|Collection|Dining))\s+(?:owns|operates|manages)',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, " ".join(search_snippets))
                if matches:
                    # Return the first match found
                    group_name = matches[0].strip()
                    return group_name, "Unknown"
            
            # If we found indicators but couldn't extract name
            return "Part of Restaurant Group (verify manually)", "Unknown"
        
        # No evidence found - confirm as Independent
        return "Independent", "1"
        
    except Exception as e:
        print(f"  Serper verification error: {str(e)}")
        return "Independent", "1"  # On error, assume Perplexity was correct


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
    if "Verified" not in df.columns:
        df["Verified"] = ""
    
    total_rows = len(df)
    print(f"Found {total_rows} restaurants to process")
    
    # Process each restaurant
    for idx, row in df.iterrows():
        restaurant_name = row.get("Company name", "")
        location = row.get("Macro Geo (NYC, SF, CHS, DC, LA, NASH, DEN)", "")
        domain = row.get("Company Domain Name", "")
        
        # Skip if already processed and verified
        if (pd.notna(df.at[idx, "Hospitality Group"]) and 
            df.at[idx, "Hospitality Group"] and
            pd.notna(df.at[idx, "Verified"]) and 
            df.at[idx, "Verified"] == "Yes"):
            print(f"[{idx+1}/{total_rows}] Skipping {restaurant_name} (already verified)")
            continue
        
        print(f"[{idx+1}/{total_rows}] Searching for: {restaurant_name}...")
        
        # First pass: Perplexity search
        hospitality_group, total_locations = search_hospitality_group(restaurant_name, location, domain)
        df.at[idx, "Hospitality Group"] = hospitality_group
        df.at[idx, "Total Locations"] = total_locations
        
        print(f"  → Perplexity result: {hospitality_group} ({total_locations} locations)")
        
        # Second pass: If marked as Independent, verify with Serper (Google)
        if hospitality_group == "Independent" and SERPER_API_KEY:
            print(f"  → Verifying with Google Search...")
            time.sleep(SERPER_DELAY)
            
            verified_group, verified_locations = verify_with_serper(restaurant_name, location, domain)
            
            if verified_group != "Independent":
                # Found evidence of a group - update the results
                print(f"  → Google verification found: {verified_group}")
                df.at[idx, "Hospitality Group"] = verified_group
                df.at[idx, "Total Locations"] = verified_locations
                df.at[idx, "Verified"] = "Yes - Group Found"
            else:
                # Confirmed as Independent
                print(f"  → Confirmed Independent")
                df.at[idx, "Verified"] = "Yes - Confirmed Independent"
        elif hospitality_group != "Independent":
            # Part of a group according to Perplexity
            df.at[idx, "Verified"] = "Yes - Group Identified"
        else:
            # No Serper key available
            df.at[idx, "Verified"] = "No - Serper Not Available"
        
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
        verified = len(df[df["Verified"].str.contains("Yes", na=False)])
        
        print(f"Total restaurants: {total}")
        print(f"Independent: {independent} ({independent/total*100:.1f}%)")
        print(f"Part of groups: {groups} ({groups/total*100:.1f}%)")
        print(f"Verified results: {verified} ({verified/total*100:.1f}%)")
        if errors > 0:
            print(f"Errors: {errors}")


def main():
    """Main execution function."""
    # Check for API keys
    if not PERPLEXITY_API_KEY:
        print("ERROR: PERPLEXITY_API_KEY environment variable not set!")
        print("\nPlease set your API key using:")
        print("  export PERPLEXITY_API_KEY='your-api-key-here'")
        return
    
    # Serper is optional but recommended
    if not SERPER_API_KEY:
        print("WARNING: SERPER_API_KEY not set - Independent restaurants won't be verified")
        print("Get a free API key at: https://serper.dev")
        print("Then set it with: export SERPER_API_KEY='your-api-key-here'")
        print("\nContinuing without verification...\n")
        time.sleep(3)
    
    # Check if input file exists
    if not os.path.exists(INPUT_CSV):
        print(f"ERROR: Input file '{INPUT_CSV}' not found!")
        print(f"Please ensure the CSV file is in the same directory as this script.")
        return
    
    print("=" * 70)
    print("Restaurant Hospitality Group Finder")
    print("=" * 70)
    print(f"Input file: {INPUT_CSV}")
    print(f"Output file: {OUTPUT_CSV}")
    print(f"Primary search: Perplexity sonar-pro")
    print(f"Verification: Serper (Google Search) {'✓ Enabled' if SERPER_API_KEY else '✗ Disabled'}")
    print(f"Request delay: {REQUEST_DELAY} seconds")
    print("=" * 70)
    print()
    
    # Process the restaurants
    process_restaurants(INPUT_CSV, OUTPUT_CSV)


if __name__ == "__main__":
    main()