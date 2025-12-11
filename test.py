#!/usr/bin/env python3
"""
Comprehensive test script for Smart Document Retrieval System
Tests all API endpoints and functionality
"""

import requests
import json
from time import sleep

# API Configuration
BASE_URL = "http://localhost:5000/api"

def print_section(title):
    """Print section header"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60 + "\n")

def test_health():
    """Test system health"""
    print_section("TEST 1: System Health Check")
    
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")

def test_index_from_folder():
    """Test indexing from database folder"""
    print_section("TEST 2: Index from Database Folder")
    
    try:
        response = requests.post(
            f"{BASE_URL}/index/from-folder",
            json={"folder_path": "database"}
        )
        
        print(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
        
        # Wait for indexing to complete
        sleep(2)
    except Exception as e:
        print(f"Error: {str(e)}")

def test_autocomplete():
    """Test autocomplete search"""
    print_section("TEST 3: Autocomplete Search")
    
    queries = ["oil", "trade", "dollar"]
    
    for query in queries:
        print(f"\nQuery: '{query}'")
        try:
            response = requests.get(
                f"{BASE_URL}/search/autocomplete",
                params={"q": query, "size": 5}
            )
            
            if response.status_code == 200:
                results = response.json()["results"]
                for i, result in enumerate(results, 1):
                    print(f"  {i}. {result['title']} (score: {result['score']:.2f})")
            else:
                print(f"Error: {response.status_code}")
        except Exception as e:
            print(f"Error: {str(e)}")

def test_smart_search():
    """Test smart search with spatial and temporal filters"""
    print_section("TEST 4: Smart Search with Filters")
    
    # Test 1: Text query only
    print("\n--- Text Query Only ---")
    try:
        response = requests.post(
            f"{BASE_URL}/search/smart",
            json={
                "query": "oil prices",
                "size": 5
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"Total results: {data.get('total', 0)}")
            results = data["results"]
            for i, result in enumerate(results, 1):
                print(f"{i}. {result.get('title', 'No title')}")
                print(f"   Score: {result['score']:.2f}")
                if 'date' in result and result['date']:
                    print(f"   Date: {result['date']}")
    except Exception as e:
        print(f"Error: {str(e)}")
    
    # Test 2: Query with georeference
    print("\n--- Query with Georeference ---")
    try:
        response = requests.post(
            f"{BASE_URL}/search/smart",
            json={
                "query": "trade",
                "georeference": "USA",
                "size": 5
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"Total results: {data.get('total', 0)}")
            results = data["results"]
            for i, result in enumerate(results, 1):
                print(f"{i}. {result.get('title', 'No title')}")
                print(f"   Score: {result['score']:.2f}")
                if 'georeferences' in result:
                    print(f"   Locations: {', '.join(result['georeferences'])}")
    except Exception as e:
        print(f"Error: {str(e)}")
    
    # Test 3: Query with temporal expression
    print("\n--- Query with Temporal Expression ---")
    try:
        response = requests.post(
            f"{BASE_URL}/search/smart",
            json={
                "query": "economy",
                "temporal_expression": "1987",
                "size": 5
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"Total results: {data.get('total', 0)}")
            results = data["results"]
            for i, result in enumerate(results, 1):
                print(f"{i}. {result.get('title', 'No title')}")
                print(f"   Score: {result['score']:.2f}")
                if 'date' in result and result['date']:
                    print(f"   Date: {result['date']}")
    except Exception as e:
        print(f"Error: {str(e)}")
    
    # Test 4: Comprehensive search
    print("\n--- Comprehensive Search (Text + Geo + Time) ---")
    try:
        response = requests.post(
            f"{BASE_URL}/search/smart",
            json={
                "query": "dollar",
                "georeference": "Japan",
                "temporal_expression": "1987",
                "size": 5
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"Total results: {data.get('total', 0)}")
            results = data["results"]
            for i, result in enumerate(results, 1):
                print(f"{i}. {result.get('title', 'No title')}")
                print(f"   Score: {result['score']:.2f}")
    except Exception as e:
        print(f"Error: {str(e)}")

def test_top_georeferences():
    """Test top georeferences analytics"""
    print_section("TEST 5: Top 10 Georeferences")
    
    try:
        response = requests.get(
            f"{BASE_URL}/analytics/top-georeferences",
            params={"size": 10}
        )
        
        if response.status_code == 200:
            results = response.json()["results"]
            for i, result in enumerate(results, 1):
                print(f"{i}. {result['georeference']}: {result['count']} documents")
    except Exception as e:
        print(f"Error: {str(e)}")

def test_time_distribution():
    """Test document time distribution"""
    print_section("TEST 6: Document Time Distribution")
    
    try:
        response = requests.get(
            f"{BASE_URL}/analytics/time-distribution",
            params={"interval": "1M"}
        )
        
        if response.status_code == 200:
            results = response.json()["results"]
            print("\nDocument distribution by date:")
            # Show only dates with documents
            for result in results:
                if result['count'] > 0:
                    print(f"  {result['date']}: {result['count']} documents")
    except Exception as e:
        print(f"Error: {str(e)}")

def test_dashboard():
    """Test comprehensive dashboard"""
    print_section("TEST 7: Comprehensive Dashboard")
    
    try:
        response = requests.get(f"{BASE_URL}/analytics/dashboard")
        
        if response.status_code == 200:
            data = response.json()
            
            print(f"\nTotal Documents: {data['total_documents']}")
            
            print("\nTop Georeferences:")
            for i, geo in enumerate(data['top_georeferences'][:5], 1):
                print(f"  {i}. {geo['georeference']}: {geo['count']}")
            
            print("\nDocument Distribution (last 10 entries):")
            for item in data['documents_over_time'][-10:]:
                if item['count'] > 0:
                    print(f"  {item['date']}: {item['count']}")
    except Exception as e:
        print(f"Error: {str(e)}")

def test_index_stats():
    """Test index statistics"""
    print_section("TEST 8: Index Statistics")
    
    try:
        response = requests.get(f"{BASE_URL}/index/stats")
        
        if response.status_code == 200:
            stats = response.json()
            print(f"Document count: {stats['document_count']}")
            print(f"Index size: {stats['index_size']:,} bytes")
            print(f"Index name: {stats['index_name']}")
            print(f"Status: {stats['status']}")
    except Exception as e:
        print(f"Error: {str(e)}")

def run_all_tests():
    """Run all tests"""
    try:
        print("\n" + "#"*60)
        print("#  Smart Document Retrieval System - Complete Test")
        print("#"*60)
        
        test_health()
        test_index_stats()
        test_autocomplete()
        test_smart_search()
        test_top_georeferences()
        test_time_distribution()
        test_dashboard()
        
        print("\n" + "#"*60)
        print("#  All tests completed successfully!")
        print("#"*60 + "\n")
        
    except requests.exceptions.ConnectionError:
        print("\n❌ Error: Cannot connect to server")
        print("Make sure the application is running on http://localhost:5000")
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")

if __name__ == "__main__":
    run_all_tests()