# -*- coding: utf-8 -*-
"""
Download FHFA Production Reports

"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
from pathlib import Path
import time

def download_fha_reports(pdf_path):
    """
    Download all FHA Production Report PDFs from HUD website.
 
    """
    os.makedirs(pdf_path, exist_ok=True)
    
    # URL of the page
    url = "https://www.hud.gov/hud-partners/fha-production-report"
    
    print(f"Fetching page: {url}")
    
    try:
        # Get the page content
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all links that end with .pdf
        pdf_links = soup.find_all('a', href=lambda href: href and href.endswith('.pdf'))
        
        print(f"Found {len(pdf_links)} PDF links")
        
        downloaded = 0
        skipped = 0
        failed = 0
        
        for link in pdf_links:

#            if (downloaded + skipped + failed) > 1: # For test purposes
#                continue

            pdf_url = link['href']
            
            # Convert relative URLs to absolute URLs
            if not pdf_url.startswith('http'):
                pdf_url = urljoin(url, pdf_url)
            
            # Extract filename from URL
            filename = pdf_url.split('/')[-1]
            filepath = os.path.join(pdf_path, filename)
            
            # Skip if file already exists
            if os.path.exists(filepath):
                print(f"Skipping (already exists): {filename}")
                skipped += 1
                continue
            
            try:
                print(f"Downloading: {filename}")
                pdf_response = requests.get(pdf_url, timeout=30)
                pdf_response.raise_for_status()
                
                # Save PDF
                with open(filepath, 'wb') as f:
                    f.write(pdf_response.content)
                
                downloaded += 1
                print(f"  ✓ Saved to: {filepath}")
                
                # Be nice to the server
                time.sleep(0.5)
                
            except Exception as e:
                failed += 1
                print(f"  ✗ Failed to download {filename}: {e}")
        
        # Summary
        print("\n" + "="*50)
        print("Download Summary:")
        print(f"  Downloaded: {downloaded}")
        print(f"  Skipped (already exists): {skipped}")
        print(f"  Failed: {failed}")
        print(f"  Total PDFs found: {len(pdf_links)}")
        print("="*50)
        
    except Exception as e:
        print(f"Error fetching page: {e}")

if __name__ == "__main__":
    download_fha_reports(pdf_path="./pdf/")
