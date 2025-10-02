/**
 * Why Today Service - Wrapper around UnifiedAQIService
 * Now uses shared data to avoid duplicate API calls
 */

import unifiedAQIService from './UnifiedAQIService.js'

class WhyTodayService {
    constructor() {
        console.log('🌟 WhyTodayService initialized - Using UnifiedAQIService (no duplicate API calls)')
    }

    /**
     * Get Why Today explanation by coordinates
     */
    async getExplanationByLocation(lat, lon, locationName = null) {
        try {
            const unifiedData = await unifiedAQIService.getLocationDataFast(lat, lon, locationName)

            console.log('✅ Why Today data from unified service:', {
                city: unifiedData.whyToday.city,
                aqi: unifiedData.whyToday.aqi,
                factors: unifiedData.whyToday.factors.length,
                locationName: locationName
            })

            return unifiedData.whyToday

        } catch (error) {
            console.error('❌ Why Today Service error:', error.message)
            return this.getFallbackExplanation()
        }
    }

    /**
     * Get Why Today explanation by city name
     */
    async getExplanationByCity(cityName) {
        try {
            console.log('🌟 City search not yet implemented with unified service for:', cityName)
            return this.getFallbackExplanation()
        } catch (error) {
            console.error('Error fetching Why Today explanation:', error)
            return this.getFallbackExplanation()
        }
    }

    /**
     * Get fallback explanation when unified service fails
     */
    getFallbackExplanation() {
        return {
            city: 'Unknown',
            aqi: 50,
            aqiStatus: 'MODERATE',
            aqiColor: '#FFFF00',
            primaryPollutant: 'PM25',
            mainSummary: 'Air quality data temporarily unavailable',
            healthAdvice: 'Please check back later for current air quality information',
            factors: [
                {
                    icon: '⚠️',
                    title: 'Data Unavailable',
                    description: 'Unable to load current air quality explanation'
                },
                {
                    icon: '🔄',
                    title: 'Try Again',
                    description: 'Please refresh the page to retry loading data'
                }
            ],
            confidence: 0.5,
            timestamp: new Date().toISOString()
        }
    }

    /**
     * Clear cache (delegates to unified service)
     */
    clearCache() {
        unifiedAQIService.clearCache()
    }
}

export default new WhyTodayService()