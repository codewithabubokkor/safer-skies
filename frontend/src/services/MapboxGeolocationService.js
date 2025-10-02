import axios from 'axios';
import locationCacheManager from './LocationCacheManager';

// Mapbox API Configuration
const MAPBOX_API_KEY = import.meta.env.VITE_MAPBOX_TOKEN;
const MAPBOX_BASE_URL = 'https://api.mapbox.com/geocoding/v5/mapbox.places';

/**
 * Simple Mapbox Geolocation Service - Cache First, API Last
 */
class MapboxGeolocationService {
    constructor() {
        console.log('🚀 MapboxGeolocationService initialized (Cache First, API Last)');
    }

    /**
     * MAIN LOCATION METHOD - Always check cache first
     */
    async getCurrentLocation() {
        const startTime = performance.now();

        const cachedLocation = locationCacheManager.getCurrentLocation();
        if (cachedLocation) {
            console.log(`✅ USING CACHE - Total time: ⏱️ ${Math.round(performance.now() - startTime)}ms`);
            return { success: true, data: cachedLocation, fromCache: true };
        }

        console.log('🔍 NO CACHE - Getting fresh location...');

        try {
            const position = await this.getGPSLocation();

            const addressData = await this.reverseGeocode(position.coords.latitude, position.coords.longitude);

            const locationData = {
                latitude: position.coords.latitude,
                longitude: position.coords.longitude,
                accuracy: position.coords.accuracy,
                address: addressData.place_name,
                city: this.extractCity(addressData),
                country: this.extractCountry(addressData),
                locationName: this.extractLocationName(addressData), // New: clean location name from GPS
                source: 'gps+mapbox'
            };

            locationCacheManager.saveCurrentLocation(locationData);

            const totalTime = Math.round(performance.now() - startTime);
            console.log(`✅ FRESH LOCATION - Total time: ⏱️ ${totalTime}ms`);

            return { success: true, data: locationData, fromCache: false };

        } catch (error) {
            console.error('❌ Location failed:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Search for location by text - Cache First
     */
    async searchLocation(query) {
        if (!query || query.trim().length === 0) {
            return { success: false, error: 'Query is required' };
        }

        const startTime = performance.now();

        try {
            const response = await this.forwardGeocode(query);

            if (!response || response.length === 0) {
                return { success: false, error: 'No locations found' };
            }

            const totalTime = Math.round(performance.now() - startTime);
            console.log(`🔍 SEARCH COMPLETE - ${response.length} results in ⏱️ ${totalTime}ms`);

            return { success: true, data: response };

        } catch (error) {
            console.error('❌ Search failed:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Select a location from search results and cache it
     */
    async selectLocation(locationData) {
        try {
            const locationToCache = {
                latitude: locationData.latitude,
                longitude: locationData.longitude,
                address: locationData.address,
                city: locationData.city,
                country: locationData.country,
                source: 'search'
            };

            locationCacheManager.saveCurrentLocation(locationToCache);

            console.log('✅ LOCATION SELECTED & CACHED:', locationToCache.address);
            return { success: true, data: locationToCache };

        } catch (error) {
            console.error('❌ Failed to select location:', error);
            return { success: false, error: error.message };
        }
    }

    /**
     * Get GPS coordinates using browser geolocation
     */
    getGPSLocation() {
        return new Promise((resolve, reject) => {
            if (!navigator.geolocation) {
                reject(new Error('Geolocation not supported'));
                return;
            }

            const options = {
                enableHighAccuracy: true,
                timeout: 15000,
                maximumAge: 5 * 60 * 1000 // 5 minutes
            };

            console.log('📍 Getting GPS coordinates...');

            navigator.geolocation.getCurrentPosition(
                (position) => {
                    console.log('✅ GPS coordinates received:', {
                        lat: position.coords.latitude.toFixed(4),
                        lng: position.coords.longitude.toFixed(4),
                        accuracy: `±${Math.round(position.coords.accuracy)}m`
                    });
                    resolve(position);
                },
                (error) => {
                    let errorMessage = 'Location access denied';
                    switch (error.code) {
                        case error.PERMISSION_DENIED:
                            errorMessage = 'Location access denied by user';
                            break;
                        case error.POSITION_UNAVAILABLE:
                            errorMessage = 'Location not available';
                            break;
                        case error.TIMEOUT:
                            errorMessage = 'Location request timed out';
                            break;
                    }
                    console.error('❌ GPS failed:', errorMessage);
                    reject(new Error(errorMessage));
                },
                options
            );
        });
    }

    /**
     * Convert coordinates to address using Mapbox Reverse Geocoding
     */
    async reverseGeocode(latitude, longitude) {
        const apiStartTime = performance.now();

        try {
            const url = `${MAPBOX_BASE_URL}/${longitude},${latitude}.json`;
            const params = {
                access_token: MAPBOX_API_KEY,
                types: 'address,poi,place,locality,neighborhood'
            };

            console.log('🌐 Calling Mapbox reverse geocoding API...');

            const response = await axios.get(url, { params });

            if (!response.data.features || response.data.features.length === 0) {
                throw new Error('No address found for coordinates');
            }

            const apiTime = Math.round(performance.now() - apiStartTime);
            console.log(`✅ Mapbox reverse geocoding - ⏱️ ${apiTime}ms`);

            return response.data.features[0];

        } catch (error) {
            console.error('❌ Reverse geocoding failed:', error.message);
            throw new Error(`Failed to get address: ${error.message}`);
        }
    }

    /**
     * Convert address/query to coordinates using Mapbox Forward Geocoding
     */
    async forwardGeocode(query) {
        const apiStartTime = performance.now();

        try {
            const url = `${MAPBOX_BASE_URL}/${encodeURIComponent(query)}.json`;
            const params = {
                access_token: MAPBOX_API_KEY,
                limit: 5,
                types: 'address,poi,place,locality,neighborhood'
            };

            console.log(`🌐 Searching Mapbox for: "${query}"`);

            const response = await axios.get(url, { params });

            if (!response.data.features || response.data.features.length === 0) {
                return [];
            }

            const apiTime = Math.round(performance.now() - apiStartTime);
            console.log(`✅ Mapbox search - ${response.data.features.length} results in ⏱️ ${apiTime}ms`);

            return response.data.features.map(feature => ({
                latitude: feature.center[1],
                longitude: feature.center[0],
                address: feature.place_name,
                city: this.extractCity(feature),
                country: this.extractCountry(feature),
                locationName: this.extractLocationName(feature), // New: clean location name
                relevance: feature.relevance
            }));

        } catch (error) {
            console.error('❌ Forward geocoding failed:', error.message);
            throw new Error(`Search failed: ${error.message}`);
        }
    }

    /**
     * Extract city from Mapbox response
     */
    extractCity(feature) {
        if (!feature.context) return null;

        const cityContext = feature.context.find(ctx =>
            ctx.id.startsWith('place.') || ctx.id.startsWith('locality.')
        );

        return cityContext ? cityContext.text : null;
    }

    /**
     * Extract proper location name from Mapbox feature (city, state format)
     */
    extractLocationName(feature) {
        if (!feature.context) {
            return this.processPlaceName(feature.place_name);
        }

        let city = null, state = null;

        for (const ctx of feature.context) {
            if (ctx.id.startsWith('place.')) {
                city = ctx.text;
            } else if (ctx.id.startsWith('region.')) {
                state = ctx.text;
            }
        }

        if (city && state) {
            return `${city}, ${state}`;
        } else if (city) {
            return city;
        } else {
            return this.processPlaceName(feature.place_name);
        }
    }

    /**
     * Process place_name string to extract clean location name
     */
    processPlaceName(placeName) {
        if (!placeName) return 'Unknown Location';

        const parts = placeName.split(',').map(part => part.trim());

        if (parts.length >= 3) {
            const firstPart = parts[0];
            if (/^\d+/.test(firstPart)) {
                if (parts[1] === parts[2]) {
                    return parts[1]; // "New York, New York" → "New York"
                } else {
                    return `${parts[1]}, ${parts[2]}`; // "City, State"
                }
            } else {
                if (parts[0] === parts[1]) {
                    return parts[0]; // "New York, New York" → "New York"
                } else {
                    return `${parts[0]}, ${parts[1]}`; // "City, State"
                }
            }
        } else if (parts.length === 2) {
            return `${parts[0]}, ${parts[1]}`;
        } else {
            return parts[0] || 'Unknown Location';
        }
    }

    /**
     * Extract country from Mapbox response
     */
    extractCountry(feature) {
        if (!feature.context) return null;

        const countryContext = feature.context.find(ctx =>
            ctx.id.startsWith('country.')
        );

        return countryContext ? countryContext.text : null;
    }

    /**
     * Check if we have valid cached location
     */
    hasCachedLocation() {
        return locationCacheManager.hasCachedLocation();
    }

    /**
     * Clear all caches
     */
    clearAllCaches() {
        locationCacheManager.clearCache();
        console.log('🧹 All caches cleared');
    }

    /**
     * Get cache status for debugging
     */
    getCacheStatus() {
        return locationCacheManager.getCacheStatus();
    }

    /**
     * Print cache data for inspection
     */
    printCacheData() {
        locationCacheManager.printCacheData();
    }
}

const mapboxGeolocationService = new MapboxGeolocationService();

export default mapboxGeolocationService;
