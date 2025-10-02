/**
 * Why Today Service
 * Fetches scientific explanations for air quality conditions
 */

const WHY_TODAY_API_BASE = '/api/why-today'  // Use unified server endpoint

class WhyTodayService {
    constructor() {
        // Cache for storing API responses
        this.cache = new Map()
        this.cacheTimeout = 10 * 60 * 1000 // 10 minutes cache
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
     * Get Why Today explanation by coordinates
     */
    async getExplanationByLocation(lat, lon) {
        try {
            const cacheKey = this.getCacheKey(lat, lon)
            const cachedData = this.cache.get(cacheKey)

            if (cachedData && this.isCacheValid(cachedData)) {
                console.log('🎯 Using cached Why Today data for', cacheKey)
                return cachedData.data
            }

            console.log('🌐 Fetching fresh Why Today data for', cacheKey)
            const response = await fetch(`${WHY_TODAY_API_BASE}/location?lat=${lat}&lon=${lon}`)
            const data = await response.json()

            console.log('🌟 Why Today API response:', data)

            if (data.success && data.data) {
                const formattedData = this.formatEnhancedExplanation(data.data, data.location)

                this.cache.set(cacheKey, {
                    data: formattedData,
                    timestamp: Date.now()
                })

                return formattedData
            }

            return this.getFallbackExplanation()
        } catch (error) {
            console.error('Error fetching Why Today explanation:', error)
            return this.getFallbackExplanation()
        }
    }

    /**
     * Format enhanced API response to frontend format
     */
    formatEnhancedExplanation(explanation, location = {}) {
        const aqi = explanation.aqi_value || 0
        const pollutant = explanation.primary_pollutant || 'PM25'
        const city = location.city || location.name || 'Your Location'

        const aqiStatus = this.getAQIStatus(aqi)

        const mainSummary = `${explanation.main_explanation || `Air quality index is ${aqi}`} (Confidence: ${Math.round((explanation.confidence_score || 0.8) * 100)}%)`

        const factors = []

        factors.push({
            icon: '📊',
            title: `AQI: ${aqi} (${pollutant})`,
            description: explanation.main_explanation || `Current air quality index is ${aqi} with ${pollutant} as primary pollutant`
        })

        if (explanation.health_context) {
            factors.push({
                icon: '🏥',
                title: 'Health Context',
                description: explanation.health_context
            })
        }

        if (explanation.seasonal_context) {
            factors.push({
                icon: '🍂',
                title: 'Seasonal Context',
                description: explanation.seasonal_context
            })
        }

        if (explanation.fire_information) {
            const fireIcon = explanation.fire_information.includes('No fires') ? '🔥❌' : '🔥'
            factors.push({
                icon: fireIcon,
                title: 'Fire Information',
                description: explanation.fire_information
            })
        }

        if (explanation.forecast_insight) {
            factors.push({
                icon: '🔮',
                title: 'Forecast Insight',
                description: explanation.forecast_insight
            })
        }

        if (explanation.trend_explanation) {
            factors.push({
                icon: '📈',
                title: 'Trend Analysis',
                description: explanation.trend_explanation
            })
        }

        if (explanation.meteorological_factors && explanation.meteorological_factors.length > 0) {
            factors.push({
                icon: '⛅️',
                title: 'Weather Impact',
                description: explanation.meteorological_factors[0]
            })
        }

        if (explanation.environmental_factors && explanation.environmental_factors.length > 0) {
            factors.push({
                icon: '�',
                title: 'Environmental Factor',
                description: explanation.environmental_factors[0].description || explanation.environmental_factors[0]
            })
        }

        return {
            city: city,
            aqi: aqi,
            aqiStatus: aqiStatus.status,
            aqiColor: aqiStatus.color,
            primaryPollutant: pollutant,
            mainSummary: mainSummary,
            healthAdvice: explanation.health_context || this.getDirectHealthAdvice(aqi, aqiStatus.status),
            factors: factors.slice(0, 6), // Show more factors now
            confidence: explanation.confidence_score || 0.8,
            timestamp: explanation.timestamp || new Date().toISOString()
        }
    }

    /**
     * Get Why Today explanation by city name
     */
    async getExplanationByCity(cityName) {
        try {
            const cityCacheKey = `city_${cityName.toLowerCase()}`
            const cachedData = this.cache.get(cityCacheKey)

            if (cachedData && this.isCacheValid(cachedData)) {
                console.log('🎯 Using cached Why Today data for city', cityName)
                return cachedData.data
            }

            console.log('🌐 Fetching fresh Why Today data for city', cityName)
            const response = await fetch(`${WHY_TODAY_API_BASE}/city?city=${encodeURIComponent(cityName)}`)
            const data = await response.json()

            if (data.success && data.data) {
                const formattedData = this.formatExplanation(data.data, data.location)

                this.cache.set(cityCacheKey, {
                    data: formattedData,
                    timestamp: Date.now()
                })

                return formattedData
            }

            return this.getFallbackExplanation()
        } catch (error) {
            console.error('Error fetching Why Today explanation:', error)
            return this.getFallbackExplanation()
        }
    }

    /**
     * Clear cache (useful for testing or manual refresh)
     */
    clearCache() {
        this.cache.clear()
        console.log('🧹 Why Today cache cleared')
    }

    /**
     * Format explanation data for frontend consumption
     */
    formatExplanation(data, location = {}) {
        const aqi = data.aqi_value || 0
        const pollutant = data.primary_pollutant || 'PM25'
        const city = location.city || 'Your Location'

        const aqiStatus = this.getAQIStatus(aqi)

        const mainSummary = `${city}: Air is ${aqiStatus.status} today (AQI ${aqi}). ${pollutant} is the main concern.`

        const pollutantDetails = []

        const primaryInfo = this.getPollutantDirectInfo(pollutant, aqi, true)
        pollutantDetails.push(primaryInfo)

        if (data.meteorological_factors) {
            pollutantDetails.push({
                icon: '🌤️',
                title: 'Weather Factor',
                description: data.meteorological_factors[0] || 'Current weather affecting air quality'
            })
        }

        const whyExplanation = this.getWhyExplanation(aqi, pollutant, data)
        pollutantDetails.push(whyExplanation)

        return {
            city: city,
            aqi: aqi,
            aqiStatus: aqiStatus.status,
            aqiColor: aqiStatus.color,
            primaryPollutant: pollutant,
            mainSummary: mainSummary,
            healthAdvice: this.getDirectHealthAdvice(aqi, aqiStatus.status),
            factors: pollutantDetails.slice(0, 3),
            confidence: data.confidence_score || 0.8,
            timestamp: data.timestamp
        }
    }

    /**
     * Get direct AQI status description
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
     * Get direct pollutant information
     */
    getPollutantDirectInfo(pollutant, aqi, isPrimary = false) {
        const pollutantMap = {
            'PM25': {
                icon: '🌫️',
                title: isPrimary ? `PM2.5: ${aqi} AQI (Primary)` : 'PM2.5 Particles',
                description: aqi > 100 ?
                    'Fine particles from combustion, smoke, and industrial sources' :
                    'Fine particle levels are within acceptable range'
            },
            'PM10': {
                icon: '💨',
                title: isPrimary ? `PM10: ${aqi} AQI (Primary)` : 'PM10 Particles',
                description: aqi > 100 ?
                    'Large particles from dust, construction, and road debris' :
                    'Dust particle levels are normal'
            },
            'O3': {
                icon: '☀️',
                title: isPrimary ? `Ozone: ${aqi} AQI (Primary)` : 'Ground Ozone',
                description: aqi > 100 ?
                    'High ozone from sunlight reacting with vehicle and industrial emissions' :
                    'Ozone levels elevated by heat and sunlight but still acceptable'
            },
            'NO2': {
                icon: '🚗',
                title: isPrimary ? `NO2: ${aqi} AQI (Primary)` : 'Nitrogen Dioxide',
                description: aqi > 100 ?
                    'High nitrogen dioxide mainly from vehicle exhaust and power plants' :
                    'Vehicle emissions present but within acceptable limits'
            },
            'SO2': {
                icon: '🏭',
                title: isPrimary ? `SO2: ${aqi} AQI (Primary)` : 'Sulfur Dioxide',
                description: aqi > 100 ?
                    'Elevated sulfur dioxide from industrial facilities and fossil fuel burning' :
                    'Industrial emissions detected but not concerning'
            },
            'CO': {
                icon: '⛽',
                title: isPrimary ? `CO: ${aqi} AQI (Primary)` : 'Carbon Monoxide',
                description: aqi > 100 ?
                    'High carbon monoxide from vehicle exhaust and incomplete combustion' :
                    'Carbon monoxide levels are normal'
            }
        }

        return pollutantMap[pollutant] || pollutantMap['PM25']
    }

    /**
     * Get specific explanation of why air is good/bad today
     */
    getWhyExplanation(aqi, pollutant, data) {
        if (aqi <= 50) {
            return {
                icon: '✅',
                title: 'Why It\'s Good Today',
                description: 'Clean air conditions with low pollutant concentrations. Weather is helping disperse emissions effectively.'
            }
        } else if (aqi <= 100) {
            return {
                icon: '⚠️',
                title: 'Why It\'s Moderate Today',
                description: `${pollutant} levels are elevated but acceptable. ${this.getModerateReason(pollutant)}`
            }
        } else if (aqi <= 150) {
            return {
                icon: '🚨',
                title: 'Why It\'s Unhealthy for Sensitive',
                description: `${pollutant} concentrations are high enough to affect sensitive individuals. ${this.getUnhealthyReason(pollutant)}`
            }
        } else {
            return {
                icon: '💀',
                title: 'Why It\'s Unhealthy Today',
                description: `Dangerous ${pollutant} levels from multiple pollution sources. Immediate action recommended.`
            }
        }
    }

    /**
     * Get direct health advice
     */
    getDirectHealthAdvice(aqi, status) {
        if (aqi <= 50) {
            return 'Perfect day for all outdoor activities! Air quality is excellent.'
        } else if (aqi <= 100) {
            return 'Good day for outdoor activities. Sensitive individuals should monitor symptoms.'
        } else if (aqi <= 150) {
            return 'Sensitive people should reduce outdoor activities. Everyone else can enjoy outdoors with caution.'
        } else if (aqi <= 200) {
            return 'Everyone should limit outdoor activities. Wear masks if going outside.'
        } else {
            return 'STAY INDOORS. Dangerous air quality - avoid all outdoor activities.'
        }
    }

    /**
     * Get reason for moderate air quality
     */
    getModerateReason(pollutant) {
        const reasons = {
            'PM25': 'from vehicle traffic and some industrial activity',
            'PM10': 'due to dust and road particles in the air',
            'O3': 'because heat and sunlight are creating ozone from emissions',
            'NO2': 'from increased vehicle traffic and combustion sources',
            'SO2': 'due to industrial activity and fuel combustion',
            'CO': 'from vehicle exhaust and incomplete burning'
        }
        return reasons[pollutant] || 'from various emission sources'
    }

    /**
     * Get reason for unhealthy air quality
     */
    getUnhealthyReason(pollutant) {
        const reasons = {
            'PM25': 'Heavy traffic, industrial emissions, or wildfire smoke contributing to high particle levels.',
            'PM10': 'Construction, dust storms, or heavy traffic creating excessive large particles.',
            'O3': 'Strong sunlight and heat creating high ozone from vehicle and industrial emissions.',
            'NO2': 'Heavy traffic or power plant emissions causing elevated nitrogen dioxide.',
            'SO2': 'Industrial facilities or coal burning creating dangerous sulfur dioxide levels.',
            'CO': 'Traffic congestion or faulty combustion sources producing excess carbon monoxide.'
        }
        return reasons[pollutant] || 'Multiple pollution sources contributing to poor air quality.'
    }

    /**
     * Fallback explanation when API is unavailable
     */
    getFallbackExplanation() {
        return {
            city: 'Your Location',
            aqi: 50,
            aqiStatus: 'GOOD',
            aqiColor: '#00E400',
            primaryPollutant: 'PM25',
            mainSummary: 'Your Location: Air quality data is loading... (AQI --). Please wait for analysis.',
            healthAdvice: 'Air quality information is loading. Please check back in a moment.',
            factors: [
                {
                    icon: '🌐',
                    title: 'Loading Real Data',
                    description: 'Connecting to NASA TEMPO satellite and EPA monitoring stations'
                },
                {
                    icon: '📡',
                    title: 'Analyzing Air Quality',
                    description: 'Processing current pollutant levels: PM2.5, O3, NO2, CO, SO2'
                },
                {
                    icon: '⏰',
                    title: 'Scientific Explanation Coming',
                    description: 'Generating personalized explanation for your location'
                }
            ],
            confidence: 0.5,
            timestamp: new Date().toISOString()
        }
    }
}

export default new WhyTodayService()
