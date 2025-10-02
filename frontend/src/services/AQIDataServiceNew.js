/**
 * AQI Data Service - Wrapper around UnifiedAQIService
 * Now uses shared data to avoid duplicate API calls
 */

import unifiedAQIService from './UnifiedAQIService.js'

class AQIDataService {
    constructor() {
        console.log('🌬️ AQIDataService initialized - Using UnifiedAQIService (no duplicate API calls)')
    }

    /**
     * Get current AQI data for a specific location
     * @param {number} latitude 
     * @param {number} longitude 
     * @param {string} city - optional city name
     * @returns {Promise<Object>} AQI data
     */
    async getCurrentAQI(latitude, longitude, city = null) {
        try {
            const unifiedData = await unifiedAQIService.getLocationDataFast(latitude, longitude, city)

            const aqiData = {
                location: unifiedData.location,
                current_aqi: unifiedData.aqi.value,
                aqi_category: unifiedData.aqi.category,
                dominant_pollutant: unifiedData.aqi.dominant_pollutant,
                pollutants: {}, // Why Today API doesn't have detailed pollutants
                epa_message: unifiedData.aqi.health_message,
                health_recommendations: [],
                last_updated: unifiedData.raw.timestamp,
                data_sources: unifiedData.aqi.data_sources, // Use location-specific sources
                confidence: unifiedData.aqi.confidence,
                timestamp: unifiedData.timestamp
            }

            console.log('✅ AQI data from unified service:', {
                city: aqiData.location.city,
                aqi: aqiData.current_aqi,
                category: aqiData.aqi_category,
                locationName: city
            })

            return aqiData

        } catch (error) {
            console.error('❌ AQI Data Service error:', error.message)
            return this.getFallbackAQIData(latitude, longitude)
        }
    }

    /**
     * Get fallback AQI data when unified service fails
     */
    getFallbackAQIData(latitude, longitude) {
        return {
            location: {
                city: 'Unknown',
                latitude: latitude,
                longitude: longitude
            },
            current_aqi: 50,
            aqi_category: 'Moderate',
            dominant_pollutant: 'PM25',
            pollutants: {},
            epa_message: 'Data temporarily unavailable',
            health_recommendations: [],
            last_updated: new Date().toISOString(),
            data_sources: ['Fallback Data'],
            confidence: 0.5,
            timestamp: new Date().toISOString()
        }
    }

    /**
     * Get AQI category from AQI value
     */
    getAQICategory(aqi) {
        if (aqi <= 50) return 'Good'
        if (aqi <= 100) return 'Moderate'
        if (aqi <= 150) return 'Unhealthy for Sensitive Groups'
        if (aqi <= 200) return 'Unhealthy'
        if (aqi <= 300) return 'Very Unhealthy'
        return 'Hazardous'
    }

    /**
     * Clear cache (delegates to unified service)
     */
    clearCache() {
        unifiedAQIService.clearCache()
    }
}

export default new AQIDataService()