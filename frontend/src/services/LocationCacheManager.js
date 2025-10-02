/**
 * Location Cache Manager - Simple location caching with strict "Cache First, API Last" policy
 */

class LocationCacheManager {
    constructor() {
        this.CURRENT_LOCATION_KEY = 'naqforecast_current_location';
        this.CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours

        console.log('🗄️ LocationCacheManager initialized (Cache First, API Last)');
    }

    /**
     * STRICT CACHE CHECK - First line of defense
     * Returns cached location if valid, null if not found or expired
     */
    getCurrentLocation() {
        try {
            const startTime = performance.now();
            const cached = localStorage.getItem(this.CURRENT_LOCATION_KEY);

            if (!cached) {
                console.log('❌ NO CACHE DATA - API call required');
                return null;
            }

            const locationData = JSON.parse(cached);

            if (Date.now() - locationData.timestamp > this.CACHE_TTL) {
                const ageHours = Math.round((Date.now() - locationData.timestamp) / (1000 * 60 * 60));
                console.log(`⏰ CACHE EXPIRED (${ageHours}h old) - API call required`);
                this.clearCache();
                return null;
            }

            const cacheTime = Math.round(performance.now() - startTime);
            const ageMinutes = Math.round((Date.now() - locationData.timestamp) / (1000 * 60));

            console.log('✅ CACHE HIT - NO API CALL NEEDED:', {
                address: locationData.address,
                city: locationData.city,
                coordinates: `${locationData.latitude?.toFixed(4)}, ${locationData.longitude?.toFixed(4)}`,
                source: locationData.source,
                age: `${ageMinutes}m old`,
                responseTime: `⏱️ ${cacheTime}ms`
            });

            return locationData;
        } catch (error) {
            console.error('❌ CACHE READ ERROR - API call required:', error);
            return null;
        }
    }

    /**
     * Save current location to cache (after API call)
     */
    saveCurrentLocation(locationData) {
        try {
            const cacheEntry = {
                ...locationData,
                timestamp: Date.now(),
                source: locationData.source || 'api'
            };

            localStorage.setItem(
                this.CURRENT_LOCATION_KEY,
                JSON.stringify(cacheEntry)
            );

            console.log('💾 LOCATION CACHED:', {
                address: cacheEntry.address,
                city: cacheEntry.city,
                coordinates: `${cacheEntry.latitude?.toFixed(4)}, ${cacheEntry.longitude?.toFixed(4)}`,
                source: cacheEntry.source,
                validUntil: new Date(Date.now() + this.CACHE_TTL).toLocaleString()
            });

            return true;
        } catch (error) {
            console.error('❌ CACHE SAVE ERROR:', error);
            return false;
        }
    }

    /**
     * Clear current location cache
     */
    clearCache() {
        try {
            localStorage.removeItem(this.CURRENT_LOCATION_KEY);
            console.log('🗑️ Location cache cleared');
            return true;
        } catch (error) {
            console.error('❌ Failed to clear cache:', error);
            return false;
        }
    }

    /**
     * Check if we have valid cached location
     */
    hasCachedLocation() {
        return this.getCurrentLocation() !== null;
    }

    /**
     * Get cache status for debugging
     */
    getCacheStatus() {
        const cached = localStorage.getItem(this.CURRENT_LOCATION_KEY);
        if (!cached) {
            return { status: 'empty', message: 'No cached location' };
        }

        try {
            const locationData = JSON.parse(cached);
            const age = Date.now() - locationData.timestamp;
            const ageMinutes = Math.round(age / (1000 * 60));
            const isExpired = age > this.CACHE_TTL;

            return {
                status: isExpired ? 'expired' : 'valid',
                age: ageMinutes,
                location: locationData.address,
                message: isExpired ? `Cache expired (${ageMinutes}m old)` : `Cache valid (${ageMinutes}m old)`
            };
        } catch (error) {
            return { status: 'error', message: 'Cache corrupted' };
        }
    }

    /**
     * Print cache data for inspection
     */
    printCacheData() {
        console.group('🗄️ LOCATION CACHE DATA');

        const status = this.getCacheStatus();
        console.log('Cache Status:', status);

        const location = this.getCurrentLocation();
        if (location) {
            console.table({
                'Address': location.address,
                'City': location.city,
                'Country': location.country,
                'Latitude': location.latitude?.toFixed(6),
                'Longitude': location.longitude?.toFixed(6),
                'Source': location.source,
                'Cached At': new Date(location.timestamp).toLocaleString(),
                'Age (minutes)': Math.round((Date.now() - location.timestamp) / (1000 * 60))
            });
        } else {
            console.log('No valid cache data found');
        }

        console.groupEnd();
    }
}

const locationCacheManager = new LocationCacheManager();

export default locationCacheManager;
