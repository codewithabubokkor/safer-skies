/**
 * 📊 Trend API Service
 * Frontend service for interacting with the trend API endpoints
 */

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:5000'; // Deployment ready

class TrendApiService {
    /**
     * Get trend data for a specific location
     */
    async getLocationTrends(locationId, days = 30) {
        try {
            let response;

            if (locationId.includes('_') && locationId.match(/^-?\d+\.?\d*_-?\d+\.?\d*$/)) {
                const [lat, lng] = locationId.split('_').map(Number);
                response = await fetch(
                    `${API_BASE}/api/location/trends?lat=${lat}&lng=${lng}&days=${days}`
                );
            } else {
                response = await fetch(
                    `${API_BASE}/api/trends/${encodeURIComponent(locationId)}`
                );
            }

            if (!response.ok) {
                if (response.status === 404) {
                    return null;
                }
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const result = await response.json();

            if (result.success && (result.trends || result.data)) {
                const trendsData = result.trends || result.data;
                const locationName = result.city || result.location?.city || locationId;
                return this.transformTrendData(trendsData, locationName);
            } else if (result.success && !result.trends && !result.data) {
                return null;
            } else {
                throw new Error(result.error || 'Unknown error from trend API');
            }
        } catch (error) {
            console.error('Failed to fetch trend data:', error);
            if (error.message.includes('404') || error.message.includes('not found')) {
                return null; // Don't throw for missing data
            }
            throw error;
        }
    }

    /**
     * Transform real API trend data to frontend format
     */
    transformTrendData(trends, locationName) {
        if (!trends || trends.length === 0) {
            return null;
        }

        const sortedTrends = [...trends].sort((a, b) => new Date(b.date) - new Date(a.date));

        return {
            location: locationName,
            summary: {
                current_aqi: sortedTrends[0]?.aqi || 0,
                trend_direction: this.calculateTrendDirection(sortedTrends),
                data_points: sortedTrends.length,
                date_range: {
                    start: sortedTrends[sortedTrends.length - 1]?.date,
                    end: sortedTrends[0]?.date
                }
            },
            time_series: {
                dates: sortedTrends.map(t => t.date),
                aqi_values: sortedTrends.map(t => t.aqi),
                dominant_pollutants: sortedTrends.map(t => t.dominant_pollutant),
                data_quality: sortedTrends.map(t => t.data_completeness || 100)
            },
            pollutant_details: sortedTrends.map(t => t.pollutant_details || {}),
            raw_data: sortedTrends // Keep original data for debugging
        };
    }

    /**
     * Calculate trend direction from AQI values
     */
    calculateTrendDirection(trends) {
        if (trends.length < 2) return 'stable';

        const recent = trends.slice(0, Math.min(3, trends.length));
        const older = trends.slice(-Math.min(3, trends.length));

        const recentAvg = recent.reduce((sum, t) => sum + t.aqi, 0) / recent.length;
        const olderAvg = older.reduce((sum, t) => sum + t.aqi, 0) / older.length;

        const difference = recentAvg - olderAvg;

        if (difference > 5) return 'worsening';
        if (difference < -5) return 'improving';
        return 'stable';
    }

    /**
     * Get all locations with trend data available
     */
    async getAllLocationsSummary(days = 7) {
        try {
            const response = await fetch(`${API_BASE}/api/trends/locations`);

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const result = await response.json();

            if (result.success && result.locations) {
                const transformedLocations = result.locations.map((location, index) => ({
                    ...location,
                    name: location.name || location.city, // Ensure name property exists
                    id: location.location_id || location.city, // Keep original id
                    uniqueKey: `${location.city}_${location.coordinates?.lat}_${location.coordinates?.lng}_${index}`, // Unique key for React
                    lat: location.coordinates?.lat, // Flatten coordinates
                    lon: location.coordinates?.lng, // Map lng to lon for compatibility
                    lng: location.coordinates?.lng // Keep lng as well
                }));

                return {
                    status: 'success',
                    locations: transformedLocations,
                    total_locations: result.total_locations,
                    timestamp: result.timestamp
                };
            } else {
                return null;
            }
        } catch (error) {
            console.error('Failed to fetch trends summary:', error);
            return null;
        }
    }

    /**
     * Get trend data using coordinates (for current location)
     */
    async getTrendsByCoordinates(lat, lng, days = 7) {
        try {
            const response = await fetch(
                `${API_BASE}/api/location/trends?lat=${lat}&lng=${lng}&days=${days}`
            );

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const result = await response.json();

            if (result.success && result.data) {
                return this.transformTrendData(result.data, `${lat.toFixed(3)}°, ${lng.toFixed(3)}°`);
            } else {
                return null;
            }
        } catch (error) {
            console.error('Failed to fetch trends by coordinates:', error);
            return null;
        }
    }

    /**
     * Get multiple locations at once (sequential fetch using real endpoints)
     */
    async getBatchTrends(locationIds, days = 30) {
        try {
            const results = {};

            for (const locationId of locationIds) {
                try {
                    const trendData = await this.getLocationTrends(locationId, days);
                    results[locationId] = trendData;
                } catch (error) {
                    console.warn(`Failed to fetch trends for ${locationId}:`, error);
                    results[locationId] = null;
                }

                await new Promise(resolve => setTimeout(resolve, 100));
            }

            return results;
        } catch (error) {
            console.error('Failed to fetch batch trends:', error);
            return null;
        }
    }

    /**
     * Get available locations with trend data
     */
    async getAvailableLocations() {
        try {
            const summary = await this.getAllLocationsSummary();
            return summary?.locations || [];
        } catch (error) {
            console.error('Failed to fetch available locations:', error);
            return [];
        }
    }

    /**
     * Check API health using the main health endpoint
     */
    async checkHealth() {
        try {
            const response = await fetch(`${API_BASE}/api/health`);

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Failed to check API health:', error);
            return { status: 'unhealthy', error: error.message };
        }
    }
}

export default new TrendApiService();