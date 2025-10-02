/**
 * Unified AQI Service
 * Single API call shared between AQI Data Service and Why Today Service
 * Uses Why Today API which has all data from comprehensive_aqi_hourly table
 */

class UnifiedAQIService {
    constructor() {
        this.cache = new Map()
        this.cacheTimeout = 10 * 60 * 1000 // 10 minutes cache
        this.pendingRequests = new Map() // Prevent duplicate concurrent requests
    }

    /**
     * Generate cache key for location
     */
    getCacheKey(lat, lon) {
        return `${lat.toFixed(4)}_${lon.toFixed(4)}`
    }

    /**
     * Check if cached data is still valid
     */
    isCacheValid(cacheEntry) {
        return Date.now() - cacheEntry.timestamp < this.cacheTimeout
    }

    /**
     * Get comprehensive data for location with ultra-fast parallel processing
     */
    async getLocationDataFast(lat, lon, locationName = null) {
        const cacheKey = this.getCacheKey(lat, lon)

        const cachedData = this.cache.get(cacheKey)
        if (cachedData && this.isCacheValid(cachedData)) {
            return cachedData.data
        }

        if (this.pendingRequests.has(cacheKey)) {
            return this.pendingRequests.get(cacheKey)
        }

        const requestPromise = this.fetchLocationDataFast(lat, lon, cacheKey, locationName)
        this.pendingRequests.set(cacheKey, requestPromise)

        try {
            const result = await requestPromise
            return result
        } finally {
            this.pendingRequests.delete(cacheKey)
        }
    }

    /**
     * Fetch data from ultra-fast parallel API endpoint
     */
    async fetchLocationDataFast(lat, lon, cacheKey, locationName = null) {
        try {
            let url = `/api/why-today/location?lat=${lat}&lon=${lon}`
            if (locationName) {
                url += `&city_name=${encodeURIComponent(locationName)}`
            }
            const response = await fetch(url)
            const apiData = await response.json()

            if (apiData.success && apiData.data) {
                const unifiedData = {
                    location: {
                        city: apiData.data.city_name || apiData.location?.city || 'Unknown',
                        latitude: lat,
                        longitude: lon
                    },

                    aqi: {
                        value: apiData.data.aqi_value,
                        category: this.getAQICategory(apiData.data.aqi_value),
                        dominant_pollutant: apiData.data.primary_pollutant,
                        health_message: apiData.data.health_context,
                        confidence: apiData.data.confidence_score || 0.95,
                        data_sources: this.getDataSources(lat, lon)
                    },

                    whyToday: {
                        city: apiData.data.city_name || 'Unknown',
                        aqi: apiData.data.aqi_value,
                        aqiStatus: this.getAQIStatus(apiData.data.aqi_value).status,
                        aqiColor: this.getAQIStatus(apiData.data.aqi_value).color,
                        primaryPollutant: apiData.data.primary_pollutant || 'PM25',
                        mainSummary: `${apiData.data.main_explanation || `Air quality index is ${apiData.data.aqi_value}`} (Confidence: ${Math.round((apiData.data.confidence_score || 0.8) * 100)}%)`,
                        healthAdvice: apiData.data.health_context || '',
                        factors: this.formatFactors(apiData.data),
                        confidence: apiData.data.confidence_score || 0.8,
                        timestamp: apiData.data.timestamp || new Date().toISOString()
                    },

                    raw: apiData.data,
                    timestamp: apiData.timestamp,
                    performance: {
                        mode: 'fast_parallel',
                        cached: false
                    }
                }

                this.cache.set(cacheKey, {
                    data: unifiedData,
                    timestamp: Date.now()
                })

                return unifiedData
            }

            throw new Error('API returned unsuccessful response')

        } catch (error) {
            return this.fetchLocationData(lat, lon, cacheKey)
        }
    }

    /**
     * Get comprehensive data for location (used by both AQI and Why Today)
     */
    async getLocationData(lat, lon) {
        const cacheKey = this.getCacheKey(lat, lon)

        const cachedData = this.cache.get(cacheKey)
        if (cachedData && this.isCacheValid(cachedData)) {
            return cachedData.data
        }

        if (this.pendingRequests.has(cacheKey)) {
            return this.pendingRequests.get(cacheKey)
        }

        const requestPromise = this.fetchLocationData(lat, lon, cacheKey)
        this.pendingRequests.set(cacheKey, requestPromise)

        try {
            const result = await requestPromise
            return result
        } finally {
            this.pendingRequests.delete(cacheKey)
        }
    }

    /**
     * Fetch data from Why Today API (has all comprehensive data)
     */
    async fetchLocationData(lat, lon, cacheKey) {
        try {
            const response = await fetch(`/api/why-today/location?lat=${lat}&lon=${lon}`)
            const apiData = await response.json()

            if (apiData.success && apiData.data) {
                const unifiedData = {
                    location: {
                        city: apiData.data.city_name || apiData.location?.city || 'Unknown',
                        latitude: lat,
                        longitude: lon
                    },

                    aqi: {
                        value: apiData.data.aqi_value,
                        category: this.getAQICategory(apiData.data.aqi_value),
                        dominant_pollutant: apiData.data.primary_pollutant,
                        health_message: apiData.data.health_context,
                        confidence: apiData.data.confidence_score || 0.95,
                        data_sources: this.getDataSources(lat, lon)
                    },

                    whyToday: {
                        city: apiData.data.city_name || 'Unknown',
                        aqi: apiData.data.aqi_value,
                        aqiStatus: this.getAQIStatus(apiData.data.aqi_value).status,
                        aqiColor: this.getAQIStatus(apiData.data.aqi_value).color,
                        primaryPollutant: apiData.data.primary_pollutant || 'PM25',
                        mainSummary: `${apiData.data.main_explanation || `Air quality index is ${apiData.data.aqi_value}`} (Confidence: ${Math.round((apiData.data.confidence_score || 0.8) * 100)}%)`,
                        healthAdvice: apiData.data.health_context || '',
                        factors: this.formatFactors(apiData.data),
                        confidence: apiData.data.confidence_score || 0.8,
                        timestamp: apiData.data.timestamp || new Date().toISOString()
                    },

                    raw: apiData.data,
                    timestamp: apiData.timestamp
                }

                this.cache.set(cacheKey, {
                    data: unifiedData,
                    timestamp: Date.now()
                })

                return unifiedData
            }

            throw new Error('API returned unsuccessful response')

        } catch (error) {
            return this.getFallbackData(lat, lon)
        }
    }

    /**
     * Format factors for Why Today display
     */
    formatFactors(data) {
        const factors = []

        factors.push({
            icon: '📊',
            title: `AQI: ${data.aqi_value} (${data.primary_pollutant})`,
            description: data.main_explanation || `Current air quality index is ${data.aqi_value} with ${data.primary_pollutant} as primary pollutant`
        })

        if (data.health_context) {
            factors.push({
                icon: '🏥',
                title: 'Health Context',
                description: data.health_context
            })
        }

        if (data.seasonal_context) {
            factors.push({
                icon: '🍂',
                title: 'Seasonal Context',
                description: data.seasonal_context
            })
        }

        if (data.fire_information) {
            const fireIcon = data.fire_information.includes('No fires') ? '🔥❌' : '🔥'
            factors.push({
                icon: fireIcon,
                title: 'Fire Information',
                description: data.fire_information
            })
        }

        if (data.forecast_insight) {
            factors.push({
                icon: '🔮',
                title: 'Forecast Insight',
                description: data.forecast_insight
            })
        }

        if (data.trend_explanation) {
            factors.push({
                icon: '📈',
                title: 'Trend Analysis',
                description: data.trend_explanation
            })
        }

        return factors.slice(0, 6) // Show up to 6 factors
    }

    /**
     * Get data sources based on location (North America vs Global)
     */
    getDataSources(lat, lon) {
        const isNorthAmerica = (20 <= lat && lat <= 85 && -170 <= lon && lon <= -50)

        if (isNorthAmerica) {
            return ["NASA TEMPO\nNASA GEOS-CF\nNOAA GFS\nAirNow, WAQI"]
        } else {
            return ["NASA GEOS-CF\nNOAA GFS\nOpen-Meteo Air Quality"]
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
     * Get AQI status with color
     */
    getAQIStatus(aqi) {
        if (aqi <= 50) return { status: 'GOOD', color: '#00E400' }
        if (aqi <= 100) return { status: 'MODERATE', color: '#FFFF00' }
        if (aqi <= 150) return { status: 'UNHEALTHY FOR SENSITIVE', color: '#FF7E00' }
        if (aqi <= 200) return { status: 'UNHEALTHY', color: '#FF0000' }
        if (aqi <= 300) return { status: 'VERY UNHEALTHY', color: '#8F3F97' }
        return { status: 'HAZARDOUS', color: '#7E0023' }
    }

    /**
     * Get fallback data when API fails
     */
    getFallbackData(lat, lon) {
        return {
            location: {
                city: 'Unknown',
                latitude: lat,
                longitude: lon
            },
            aqi: {
                value: 50,
                category: 'Moderate',
                dominant_pollutant: 'PM25',
                health_message: 'Data temporarily unavailable',
                confidence: 0.5
            },
            whyToday: {
                city: 'Unknown',
                aqi: 50,
                aqiStatus: 'MODERATE',
                aqiColor: '#FFFF00',
                primaryPollutant: 'PM25',
                mainSummary: 'Air quality data temporarily unavailable',
                healthAdvice: 'Please check back later',
                factors: [{
                    icon: '⚠️',
                    title: 'Data Unavailable',
                    description: 'Unable to load current air quality data'
                }],
                confidence: 0.5,
                timestamp: new Date().toISOString()
            },
            raw: {},
            timestamp: new Date().toISOString()
        }
    }

    /**
     * Clear cache (useful for testing or manual refresh)
     */
    clearCache() {
        this.cache.clear()
    }

    /**
     * Clear cache for specific location and force fresh data
     */
    clearLocationCache(lat, lon) {
        const cacheKey = this.getCacheKey(lat, lon)
        this.cache.delete(cacheKey)
    }
}

const unifiedAQIService = new UnifiedAQIService()

export default unifiedAQIService