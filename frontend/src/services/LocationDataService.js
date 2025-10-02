/**
 * Location Data Service
 * Fetches both forecast and current AQI/why-today data in parallel
 * Optimized for location search results
 */

class LocationDataService {
    constructor() {
        this.cache = new Map()
        this.cacheTimeout = 10 * 60 * 1000 // 10 minutes cache
        this.pendingRequests = new Map()

        console.log('🚀 LocationDataService initialized - Parallel forecast + why-today fetching')
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
     * Get complete location data (forecast + current AQI + why today) in parallel
     */
    async getCompleteLocationData(lat, lon, locationName = null) {
        const cacheKey = this.getCacheKey(lat, lon)

        const cachedData = this.cache.get(cacheKey)
        if (cachedData && this.isCacheValid(cachedData)) {
            console.log('🎯 Using cached complete location data for', locationName || cacheKey)
            return cachedData.data
        }

        if (this.pendingRequests.has(cacheKey)) {
            console.log('⏳ Waiting for pending location data request for', locationName || cacheKey)
            return this.pendingRequests.get(cacheKey)
        }

        const requestPromise = this.fetchCompleteLocationData(lat, lon, cacheKey, locationName)
        this.pendingRequests.set(cacheKey, requestPromise)

        try {
            const result = await requestPromise
            return result
        } finally {
            this.pendingRequests.delete(cacheKey)
        }
    }

    /**
     * Fetch forecast and why-today data in parallel
     */
    async fetchCompleteLocationData(lat, lon, cacheKey, locationName = null) {
        try {
            console.log('🚀 Fetching complete location data in parallel for', locationName || cacheKey)

            const requestBody = {
                lat: lat,
                lng: lon
            }

            if (locationName) {
                requestBody.city_name = locationName
            }

            const [forecastResponse, whyTodayResponse] = await Promise.all([
                fetch('/api/location/forecast', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(requestBody)
                }),
                fetch(`/api/location/why-today?lat=${lat}&lng=${lon}${locationName ? `&city_name=${encodeURIComponent(locationName)}` : ''}`, {
                    method: 'GET',
                    headers: { 'Content-Type': 'application/json' }
                })
            ])

            const [forecastData, whyTodayData] = await Promise.all([
                forecastResponse.json(),
                whyTodayResponse.json()
            ])

            console.log('📊 Parallel API responses received:')
            console.log('  🔮 Forecast:', forecastData.success ? '✅ SUCCESS' : '❌ FAILED')
            console.log('  🌡️ Why Today:', whyTodayData.success ? '✅ SUCCESS' : '❌ FAILED')

            const combinedData = {
                success: true,
                location: {
                    city: locationName || whyTodayData.location?.city || forecastData.location?.city || 'Unknown',
                    lat: lat,
                    lng: lon
                },
                timestamp: new Date().toISOString(),

                forecast: forecastData.success && forecastData.data ? {
                    hourly: forecastData.data.hourly || [],
                    city_name: forecastData.data.city_name || locationName,
                    location_name: forecastData.data.location_name || locationName,
                    daily: this.transformHourlyToDaily(forecastData.data.hourly || [])
                } : null,

                // Current AQI data (for AQI display components)
                currentAQI: whyTodayData.success && whyTodayData.data ? {
                    aqi: whyTodayData.data.aqi_value,
                    category: this.getAQICategory(whyTodayData.data.aqi_value),
                    dominant_pollutant: whyTodayData.data.primary_pollutant,
                    city_name: whyTodayData.data.city_name || locationName,
                    confidence: whyTodayData.data.confidence_score || 0.95
                } : null,

                whyToday: whyTodayData.success && whyTodayData.data ? {
                    explanation: whyTodayData.data.explanation,
                    city: whyTodayData.data.city_name || locationName,
                    aqi: whyTodayData.data.aqi_value,
                    health_context: whyTodayData.data.health_context
                } : null,

                collections_triggered: [
                    ...(forecastData.collections_triggered || []),
                    ...(whyTodayData.collections_triggered || [])
                ]
            }

            this.cache.set(cacheKey, {
                data: combinedData,
                timestamp: Date.now()
            })

            console.log('✅ Complete location data assembled and cached')
            return combinedData

        } catch (error) {
            console.error('❌ Error fetching complete location data:', error)
            return {
                success: false,
                error: error.message,
                location: { city: locationName || 'Unknown', lat, lng }
            }
        }
    }

    /**
     * Transform hourly data to daily format
     */
    transformHourlyToDaily(hourlyData) {
        if (!hourlyData || hourlyData.length === 0) return []

        const dailyData = {}

        hourlyData.forEach(hour => {
            const dayKey = Math.floor(hour.hour / 24)
            if (!dailyData[dayKey]) {
                dailyData[dayKey] = {
                    day: dayKey + 1,
                    hours: [],
                    avgAQI: 0,
                    maxAQI: 0,
                    minAQI: 999
                }
            }

            dailyData[dayKey].hours.push(hour)
            dailyData[dayKey].maxAQI = Math.max(dailyData[dayKey].maxAQI, hour.aqi)
            dailyData[dayKey].minAQI = Math.min(dailyData[dayKey].minAQI, hour.aqi)
        })

        Object.values(dailyData).forEach(day => {
            day.avgAQI = Math.round(day.hours.reduce((sum, h) => sum + h.aqi, 0) / day.hours.length)
        })

        return Object.values(dailyData).slice(0, 5) // Return 5 days
    }

    /**
     * Get AQI category from value
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
     * Clear cache
     */
    clearCache() {
        this.cache.clear()
        console.log('🗑️ LocationDataService cache cleared')
    }

    /**
     * Get cache stats
     */
    getCacheStats() {
        return {
            entries: this.cache.size,
            timeout_minutes: this.cacheTimeout / (60 * 1000)
        }
    }
}

const locationDataService = new LocationDataService()
export default locationDataService