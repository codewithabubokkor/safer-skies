import React, { useState, useEffect } from 'react'
import forecastService from '../services/forecastService'

const DayPlannerComponent = ({ currentLocation }) => {
    const [forecastData, setForecastData] = useState(null)
    const [loading, setLoading] = useState(true)
    const [selectedDay, setSelectedDay] = useState(0)
    const [viewMode, setViewMode] = useState('daily') // 'daily' or 'hourly'

    useEffect(() => {
        if (currentLocation?.lat && currentLocation?.lon) {
            fetchForecast()
        } else if (currentLocation?.city) {
            fetchForecastByCity()
        }
    }, [currentLocation])

    const fetchForecast = async () => {
        if (!currentLocation?.lat || !currentLocation?.lon) return

        setLoading(true)
        try {
            const result = await forecastService.getForecastByLocation(currentLocation.lat, currentLocation.lon)
            setForecastData(result)
        } catch (err) {
            console.error('Error fetching forecast:', err)
            setForecastData(forecastService.getFallbackForecast())
        } finally {
            setLoading(false)
        }
    }

    const fetchForecastByCity = async () => {
        if (!currentLocation?.city) return

        setLoading(true)
        try {
            const result = await forecastService.getForecastByCity(currentLocation.city)
            setForecastData(result)
        } catch (err) {
            console.error('Error fetching city forecast:', err)
            setForecastData(forecastService.getFallbackForecast())
        } finally {
            setLoading(false)
        }
    }

    const getTimezoneOffset = (lat, lon) => {
        if (lat >= 24 && lat <= 25 && lon >= 88 && lon <= 90) {
            return 6
        } else if (lat >= 40 && lat <= 41 && lon >= -75 && lon <= -73) {
            return -4
        } else if (lat >= 34 && lat <= 35 && lon >= -119 && lon <= -117) {
            return -7
        } else {
            return 0
        }
    }

    const getHourlyForecastFromNow = () => {
        if (!forecastData?.hourly) return []

        const locationLat = currentLocation?.lat || currentLocation?.latitude || 40.713
        const locationLon = currentLocation?.lon || currentLocation?.longitude || -74.006

        const now = new Date()
        const utcHour = now.getUTCHours()
        const timezoneOffset = getTimezoneOffset(locationLat, locationLon)
        const currentLocalHour = (utcHour + timezoneOffset + 24) % 24

        let currentHourIndex = -1
        for (let i = 0; i < forecastData.hourly.length; i++) {
            const hourData = forecastData.hourly[i]
            try {
                const forecastTime = new Date(hourData.timestamp)
                const forecastUtcHour = forecastTime.getUTCHours()
                const forecastLocalHour = (forecastUtcHour + timezoneOffset + 24) % 24

                if (forecastLocalHour === currentLocalHour) {
                    currentHourIndex = i
                    break
                }
            } catch (e) {
                console.error('Error parsing forecast timestamp:', e)
            }
        }

        if (currentHourIndex === -1) {
            console.log('🔍 Current hour not found in forecast, starting from beginning')
            return forecastData.hourly
        }

        console.log(`🕐 Starting day planner from current hour: ${currentLocalHour}:00 (index ${currentHourIndex})`)

        return forecastData.hourly.slice(currentHourIndex)
    }

    const getLocalTimeDisplay = (timestamp) => {
        try {
            const locationLat = currentLocation?.lat || currentLocation?.latitude || 40.713
            const locationLon = currentLocation?.lon || currentLocation?.longitude || -74.006

            const forecastTime = new Date(timestamp)
            const utcHour = forecastTime.getUTCHours()
            const timezoneOffset = getTimezoneOffset(locationLat, locationLon)
            const localHour = (utcHour + timezoneOffset + 24) % 24

            const hour12 = localHour === 0 ? 12 : localHour > 12 ? localHour - 12 : localHour
            const ampm = localHour >= 12 ? 'PM' : 'AM'

            return `${hour12}${ampm}`
        } catch (e) {
            console.error('Error formatting time:', e)
            return new Date(timestamp).toLocaleTimeString('en-US', {
                hour: 'numeric',
                hour12: true
            })
        }
    }

    if (loading) {
        return (
            <div className="space-y-3 h-[calc(100%-40px)] overflow-y-auto animate-pulse">
                <div className="flex justify-between mb-2">
                    <div className="h-4 bg-gray-600 rounded w-20"></div>
                    <div className="h-4 bg-gray-600 rounded w-16"></div>
                </div>
                {[1, 2, 3, 4, 5].map(i => (
                    <div key={i} className="p-2 bg-gray-800/30 rounded-lg">
                        <div className="flex justify-between items-center mb-2">
                            <div className="h-3 bg-gray-600 rounded w-20"></div>
                            <div className="h-6 bg-gray-600 rounded w-12"></div>
                        </div>
                        <div className="h-2 bg-gray-700 rounded w-full"></div>
                    </div>
                ))}
            </div>
        )
    }

    if (!forecastData) {
        return (
            <div className="text-center text-red-400 text-sm">
                <span className="text-xl mb-2 block">⚠️</span>
                Unable to load forecast data
            </div>
        )
    }

    return (
        <div className="space-y-3 h-[calc(100%-40px)] overflow-y-auto touch-pan-y" style={{
            WebkitOverflowScrolling: 'touch',
            scrollBehavior: 'smooth'
        }}>
            {}
            <div className="flex justify-between items-center mb-2">
                <div className="flex bg-gray-800/50 rounded-lg p-1">
                    <button
                        onClick={() => setViewMode('daily')}
                        className={`px-3 py-1 rounded text-xs font-medium transition-all ${viewMode === 'daily'
                            ? 'bg-blue-600 text-white'
                            : 'text-gray-300 hover:text-white'
                            }`}
                    >
                        5-Day
                    </button>
                    <button
                        onClick={() => setViewMode('hourly')}
                        className={`px-3 py-1 rounded text-xs font-medium transition-all ${viewMode === 'hourly'
                            ? 'bg-blue-600 text-white'
                            : 'text-gray-300 hover:text-white'
                            }`}
                    >
                        24-Hour
                    </button>
                </div>
                <span className="text-xs text-gray-400">
                    {forecastData.summary.dataQuality === 'excellent' ? '✅' : '⚠️'} NASA Data
                </span>
            </div>

            {}
            {viewMode === 'daily' && (
                <div className="space-y-2">
                    {forecastData.daily.map((day, index) => (
                        <div
                            key={day.day}
                            className={`p-3 rounded-lg border transition-all duration-200 cursor-pointer hover:bg-gray-800/20 ${selectedDay === index
                                ? 'border-blue-500/50 bg-blue-900/20'
                                : 'border-gray-700/50'
                                }`}
                            onClick={() => setSelectedDay(index)}
                        >
                            <div className="flex justify-between items-center mb-2">
                                <div>
                                    <h4 className="text-white font-semibold text-sm">
                                        {day.dateFormatted}
                                    </h4>
                                    <p className="text-gray-400 text-xs">
                                        AQI: {day.aqi.min}-{day.aqi.max} (avg {day.aqi.avg})
                                    </p>
                                </div>
                                <div className="text-right">
                                    <div
                                        className="px-2 py-1 rounded text-xs font-medium text-white"
                                        style={{ backgroundColor: day.aqi.color + '40', border: `1px solid ${day.aqi.color}` }}
                                    >
                                        {day.aqi.category}
                                    </div>
                                    <p className="text-gray-300 text-xs mt-1">
                                        {day.weather.tempMin}°-{day.weather.tempMax}°C
                                    </p>
                                </div>
                            </div>

                            {}
                            <div className="flex items-center space-x-2 mt-2">
                                <span className="text-sm">{day.recommendation.icon}</span>
                                <div>
                                    <span className="text-white text-xs font-medium">
                                        {day.recommendation.title}:
                                    </span>
                                    <span className="text-gray-300 text-xs ml-1">
                                        {day.recommendation.advice}
                                    </span>
                                </div>
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {}
            {viewMode === 'hourly' && (
                <div className="space-y-2">
                    <h4 className="text-white font-medium text-sm mb-2">Next 24 Hours</h4>
                    {getHourlyForecastFromNow().slice(0, 12).map((hour, index) => (
                        <div key={hour.hour} className="flex items-center justify-between p-2 bg-gray-800/20 rounded-lg">
                            <div className="flex items-center space-x-3">
                                <div className="text-xs text-gray-400 w-12">
                                    {getLocalTimeDisplay(hour.timestamp)}
                                </div>
                                <div
                                    className="w-2 h-2 rounded-full"
                                    style={{ backgroundColor: hour.aqi.color }}
                                ></div>
                                <div>
                                    <span className="text-white text-xs font-medium">
                                        AQI {hour.aqi.overall}
                                    </span>
                                    <span className="text-gray-400 text-xs ml-2">
                                        {hour.aqi.category}
                                    </span>
                                </div>
                            </div>

                            <div className="text-right">
                                <div className="text-white text-xs">
                                    {hour.weather.temperature}°C
                                </div>
                                <div className="text-gray-400 text-xs">
                                    {forecastService.getHourRecommendation(hour.aqi.overall)}
                                </div>
                            </div>
                        </div>
                    ))}

                    {getHourlyForecastFromNow().length > 12 && (
                        <button
                            className="w-full text-center text-blue-400 text-xs py-2 hover:text-blue-300 transition-colors"
                            onClick={() => { }}
                        >
                            Show more hours ({getHourlyForecastFromNow().length - 12} remaining)
                        </button>
                    )}
                </div>
            )}

            {}
            <div className="mt-4 pt-2 border-t border-gray-700/50">
                <div className="flex justify-between items-center text-xs text-gray-500">
                    <span>
                        NASA GEOS-CF + Open-Meteo
                    </span>
                    <span>
                        Updated {new Date(forecastData.summary.lastUpdated).toLocaleTimeString()}
                    </span>
                </div>
            </div>
        </div>
    )
}

export default DayPlannerComponent
