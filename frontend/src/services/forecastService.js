/**
 * 5-Day Forecast Service
 * Fetches AQI forecast data from the unified API server
 */

const FORECAST_API_BASE = '/api/forecast'  // Use unified server endpoint

class ForecastService {
    /**
     * Get 5-day forecast by coordinates - Updated to use working endpoint
     */
    async getForecastByLocation(lat, lon, locationName = null) {
        try {
            console.log(`🔮 ForecastService: Fetching from /api/location/forecast (POST) for ${locationName || 'coordinates'}`)
            const requestBody = {
                lat: lat,
                lng: lon
            }

            if (locationName) {
                requestBody.city_name = locationName
            }

            const response = await fetch('/api/location/forecast', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(requestBody)
            })
            const data = await response.json()

            console.log('🔮 ForecastService: Raw API response:', data)

            if (data.success && data.data) {
                console.log('✅ ForecastService: Using real 120-hour forecast data')

                const hourlyData = data.data.hourly || []
                console.log(`📊 ForecastService: Received ${hourlyData.length} hourly forecast records`)

                return {
                    success: true,
                    hourly: hourlyData, // The DayPlannerComponent expects this format
                    daily: this.transformHourlyToDaily(hourlyData),
                    location: data.location,
                    timestamp: data.timestamp,
                    city_name: data.data.city_name,
                    summary: {
                        totalHours: hourlyData.length,
                        totalDays: 5,
                        dataQuality: 'excellent',
                        lastUpdated: data.timestamp
                    }
                }
            }

            console.log('⚠️ ForecastService: Trying fallback /5day endpoint')
            const fallbackResponse = await fetch(`${FORECAST_API_BASE}/5day?lat=${lat}&lng=${lon}`)
            const fallbackData = await fallbackResponse.json()

            if (fallbackData.success && fallbackData.forecast) {
                console.log('📊 ForecastService: Using 5-day endpoint data')
                return this.formatNewForecast(fallbackData)
            }

            console.log('❌ ForecastService: All endpoints failed, using fallback')
            return this.getFallbackForecast()
        } catch (error) {
            console.error('❌ ForecastService error:', error)
            return this.getFallbackForecast()
        }
    }

    /**
     * Get 5-day forecast by city name
     */
    async getForecastByCity(city) {
        try {
            console.log(`🔮 ForecastService: Fetching from /api/forecast/city?city=${city}`)
            const response = await fetch(`${FORECAST_API_BASE}/city?city=${encodeURIComponent(city)}`)
            const data = await response.json()

            console.log('🔮 ForecastService: Raw city API response:', data)

            if (data.success && data.data) {
                console.log('✅ ForecastService: Using real city forecast data')
                return {
                    success: true,
                    data: data.data,
                    location: data.location,
                    timestamp: data.timestamp,
                    summary: data.summary
                }
            }

            console.log('❌ ForecastService: City forecast failed, using fallback')
            return this.getFallbackForecast()
        } catch (error) {
            console.error('❌ ForecastService city error:', error)
            return this.getFallbackForecast()
        }
    }

    /**
     * Format new API forecast data for frontend consumption
     */
    formatNewForecast(data) {
        return {
            success: true,
            location: data.location,
            generated_at: data.generated_at,
            days: data.forecast.map(day => ({
                date: day.date,
                day_name: day.day_name,
                aqi: {
                    value: day.aqi,
                    category: day.category,
                    color: this.getAQIColor(day.aqi)
                },
                pollutants: {
                    pm25: day.pm25,
                    pm10: day.pm10,
                    o3: day.o3,
                    no2: day.no2
                },
                weather: day.weather
            }))
        }
    }

    /**
     * Get AQI color based on value
     */
    getAQIColor(aqi) {
        if (aqi <= 50) return '#00E400'      // Good - Green
        if (aqi <= 100) return '#FFFF00'     // Moderate - Yellow
        if (aqi <= 150) return '#FF7E00'     // Unhealthy for Sensitive - Orange
        if (aqi <= 200) return '#FF0000'     // Unhealthy - Red
        if (aqi <= 300) return '#8F3F97'     // Very Unhealthy - Purple
        return '#7E0023'                     // Hazardous - Maroon
    }

    /**
     * Get day recommendation based on max AQI
     */
    getDayRecommendation(maxAqi) {
        if (maxAqi <= 50) {
            return {
                icon: '✅',
                title: 'Perfect Day',
                advice: 'Great for all outdoor activities!'
            }
        } else if (maxAqi <= 100) {
            return {
                icon: '🟡',
                title: 'Good Day',
                advice: 'Enjoy outdoor activities with caution'
            }
        } else if (maxAqi <= 150) {
            return {
                icon: '🟠',
                title: 'Sensitive Alert',
                advice: 'Sensitive individuals should limit outdoor time'
            }
        } else if (maxAqi <= 200) {
            return {
                icon: '🔴',
                title: 'Caution Advised',
                advice: 'Everyone should reduce outdoor activities'
            }
        } else {
            return {
                icon: '🚨',
                title: 'Stay Inside',
                advice: 'Avoid all outdoor activities'
            }
        }
    }

    /**
     * Get hour recommendation based on AQI
     */
    getHourRecommendation(aqi) {
        if (aqi <= 50) return 'Perfect'
        if (aqi <= 100) return 'Good'
        if (aqi <= 150) return 'Caution'
        if (aqi <= 200) return 'Limited'
        return 'Avoid'
    }

    /**
     * Fallback forecast when API is unavailable
     */
    getFallbackForecast() {
        const fallbackDays = []
        for (let i = 0; i < 5; i++) {
            const date = new Date()
            date.setDate(date.getDate() + i)

            fallbackDays.push({
                day: i,
                date: date.toISOString().split('T')[0],
                dateFormatted: date.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' }),
                aqi: {
                    max: 50,
                    min: 30,
                    avg: 40,
                    category: 'Good',
                    color: '#00E400'
                },
                weather: {
                    tempMax: 25,
                    tempMin: 18,
                    tempAvg: 22
                },
                recommendation: {
                    icon: '⏳',
                    title: 'Loading...',
                    advice: 'Forecast data is loading'
                },
                hourlyCount: 24
            })
        }

        return {
            daily: fallbackDays,
            hourly: [],
            summary: {
                totalHours: 0,
                totalDays: 5,
                dataQuality: 'loading',
                lastUpdated: new Date().toISOString()
            }
        }
    }

    getHourRecommendation(aqi) {
        if (aqi <= 50) return "Good for outdoors";
        if (aqi <= 100) return "Limit intensity";
        if (aqi <= 150) return "Sensitive avoid";
        if (aqi <= 200) return "Stay indoors";
        return "Emergency alert";
    }

    transformHourlyToDaily(hourlyData) {
        if (!hourlyData || hourlyData.length === 0) return []

        const dailyMap = new Map()

        hourlyData.forEach(hour => {
            const hourNum = hour.hour || 0
            const dayIndex = Math.floor(hourNum / 24)
            const date = new Date()
            date.setDate(date.getDate() + dayIndex)

            const dateKey = date.toISOString().split('T')[0]

            if (!dailyMap.has(dateKey)) {
                dailyMap.set(dateKey, {
                    day: dayIndex,
                    date: dateKey,
                    dateFormatted: date.toLocaleDateString('en-US', {
                        weekday: 'short',
                        month: 'short',
                        day: 'numeric'
                    }),
                    aqiValues: [],
                    hourlyCount: 0
                })
            }

            const dayData = dailyMap.get(dateKey)
            dayData.aqiValues.push(hour.aqi || 50)
            dayData.hourlyCount++
        })

        return Array.from(dailyMap.values()).map(day => {
            const maxAqi = Math.max(...day.aqiValues)
            const minAqi = Math.min(...day.aqiValues)
            const avgAqi = Math.round(day.aqiValues.reduce((a, b) => a + b, 0) / day.aqiValues.length)

            return {
                ...day,
                aqi: {
                    max: maxAqi,
                    min: minAqi,
                    avg: avgAqi,
                    category: this.getAQICategory(avgAqi),
                    color: this.getAQIColor(avgAqi)
                },
                weather: {
                    tempMax: 25,
                    tempMin: 18,
                    tempAvg: 22
                },
                recommendation: this.getDayRecommendation(maxAqi)
            }
        })
    }

    getAQICategory(aqi) {
        if (aqi <= 50) return 'Good'
        if (aqi <= 100) return 'Moderate'
        if (aqi <= 150) return 'Unhealthy for Sensitive Groups'
        if (aqi <= 200) return 'Unhealthy'
        if (aqi <= 300) return 'Very Unhealthy'
        return 'Hazardous'
    }

    getActivityRecommendation(avgAqi, category) {
        const recommendations = {
            'Good': {
                icon: '🌟',
                title: 'Perfect Day',
                advice: 'Great for all outdoor activities'
            },
            'Moderate': {
                icon: '⚠️',
                title: 'Be Aware',
                advice: 'Sensitive individuals should limit prolonged outdoor exertion'
            },
            'Unhealthy for Sensitive Groups': {
                icon: '🚨',
                title: 'Caution Advised',
                advice: 'Children, elderly, and those with conditions should reduce outdoor activities'
            },
            'Unhealthy': {
                icon: '🚫',
                title: 'Avoid Outdoors',
                advice: 'Everyone should limit outdoor activities'
            },
            'Very Unhealthy': {
                icon: '☢️',
                title: 'Stay Inside',
                advice: 'Avoid all outdoor activities'
            },
            'Hazardous': {
                icon: '💀',
                title: 'Emergency',
                advice: 'Emergency conditions - stay indoors with air purifiers'
            }
        }

        return recommendations[category] || {
            icon: '❓',
            title: 'Unknown',
            advice: 'Check air quality conditions'
        }
    }
}

export default new ForecastService()
