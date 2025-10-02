#!/usr/bin/env python3
"""
🏙️ Smart Location Naming Utility
===============================
Provides intelligent location naming for consistent city names across collectors
"""

def get_smart_location_name(lat: float, lng: float, provided_name: str = None) -> str:
    """
    Get intelligent location name based on coordinates
    
    Args:
        lat: Latitude
        lng: Longitude  
        provided_name: Optional name provided by user/API
        
    Returns:
        Smart location name (city name if recognized, regional name if not)
    """
    # If name is provided and looks good, use it
    if provided_name and not provided_name.replace('.', '').replace('-', '').replace(' ', '').replace('°', '').replace('N', '').replace('E', '').replace('W', '').replace('S', '').replace(',', '').isdigit():
        return provided_name
    
    # Known major cities with their coordinates and proper names
    major_cities = {
        # Bangladesh
        (23.8103, 90.4125): "Dhaka, Bangladesh",
        (24.8949, 91.8687): "Sylhet, Bangladesh", 
        (22.3569, 91.7832): "Chittagong, Bangladesh",
        (24.3745, 88.6042): "Rajshahi, Bangladesh",
        (25.7439, 89.2752): "Rangpur, Bangladesh",
        (23.4607, 91.1809): "Comilla, Bangladesh",
        
        (28.6139, 77.2090): "New Delhi, India",
        (19.0760, 72.8777): "Mumbai, India",
        (12.9716, 77.5946): "Bangalore, India",
        (13.0827, 80.2707): "Chennai, India",
        (22.5726, 88.3639): "Kolkata, India",
        (17.3850, 78.4867): "Hyderabad, India",
        (18.5204, 73.8567): "Pune, India",
        (26.9124, 75.7873): "Jaipur, India",
        (21.1458, 79.0882): "Nagpur, India",
        
        # Pakistan
        (24.8607, 67.0011): "Karachi, Pakistan",
        (31.5204, 74.3587): "Lahore, Pakistan",
        (33.6844, 73.0479): "Islamabad, Pakistan",
        
        # US major cities
        (40.7128, -74.0060): "New York City, NY",
        (34.0522, -118.2437): "Los Angeles, CA",
        (41.8781, -87.6298): "Chicago, IL",
        (29.7604, -95.3698): "Houston, TX",
        (33.4484, -112.0740): "Phoenix, AZ",
        (39.9526, -75.1652): "Philadelphia, PA",
        (29.4241, -98.4936): "San Antonio, TX",
        (32.7767, -96.7970): "Dallas, TX",
        (37.7749, -122.4194): "San Francisco, CA",
        
        # Canada major cities  
        (43.6532, -79.3832): "Toronto, Canada",
        (45.5017, -73.5673): "Montreal, Canada",
        (49.2827, -123.1207): "Vancouver, Canada",
        (51.0447, -114.0719): "Calgary, Canada",
        
        # UK major cities
        (51.5074, -0.1278): "London, UK",
        (53.4808, -2.2426): "Manchester, UK",
        (55.9533, -3.1883): "Edinburgh, UK",
        
        # Other major cities
        (35.6762, 139.6503): "Tokyo, Japan",
        (37.5665, 126.9780): "Seoul, South Korea",
        (39.9042, 116.4074): "Beijing, China",
        (31.2304, 121.4737): "Shanghai, China",
    }
    
    min_distance = float('inf')
    closest_city = None
    
    for (city_lat, city_lng), city_name in major_cities.items():
        distance = abs(lat - city_lat) + abs(lng - city_lng)  # Manhattan distance
        if distance < min_distance and distance < 0.1:  # Within ~11km
            min_distance = distance
            closest_city = city_name
    
    if closest_city:
        return closest_city
    
    # Regional/country-based fallback based on coordinate ranges
    regional_names = [
        ((20.0, 27.0), (88.0, 93.0), "Bangladesh"),
        ((8.0, 37.0), (68.0, 97.5), "India"), 
        ((24.0, 37.0), (61.0, 75.5), "Pakistan"),
        ((22.0, 31.0), (79.0, 89.0), "Nepal/Bhutan"),
        ((5.9, 9.9), (79.5, 81.9), "Sri Lanka"),
        
        # North America
        ((24.0, 85.0), (-170.0, -50.0), "United States/Canada"),
        ((14.0, 33.0), (-120.0, -86.0), "Mexico/Central America"),
        
        # Europe
        ((35.0, 72.0), (-10.0, 40.0), "Europe"),
        
        # East Asia
        ((18.0, 54.0), (73.0, 135.0), "East Asia"),
        
        # Middle East
        ((12.0, 42.0), (26.0, 63.0), "Middle East"),
        
        # Africa
        ((-35.0, 38.0), (-20.0, 52.0), "Africa"),
        
        # Oceania
        ((-45.0, -10.0), (110.0, 180.0), "Australia/Oceania"),
        
        ((-56.0, 13.0), (-82.0, -34.0), "South America"),
    ]
    
    for (lat_min, lat_max), (lng_min, lng_max), region_name in regional_names:
        if lat_min <= lat <= lat_max and lng_min <= lng <= lng_max:
            return f"{region_name} ({lat:.3f}°N, {abs(lng):.3f}°{'W' if lng < 0 else 'E'})"
    
    # Ultimate fallback: coordinate-based name
    return f"{lat:.3f}°N, {abs(lng):.3f}°{'W' if lng < 0 else 'E'}"