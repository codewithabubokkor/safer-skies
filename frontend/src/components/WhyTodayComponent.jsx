import React, { useState, useEffect, useRef } from 'react'
import whyTodayService from '../services/whyTodayServiceNew'

const WhyTodayComponent = ({ currentLocation }) => {
    const [explanation, setExplanation] = useState(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState(null)
    const lastLocationRef = useRef(null)

    useEffect(() => {
        // Prevent unnecessary API calls by checking if location actually changed
        const locationKey = currentLocation?.lat && currentLocation?.lon
            ? `${currentLocation.lat.toFixed(4)}_${currentLocation.lon.toFixed(4)}`
            : currentLocation?.city || ''

        if (!locationKey || locationKey === lastLocationRef.current) {
            return
        }

        lastLocationRef.current = locationKey

        if (currentLocation?.lat && currentLocation?.lon) {
            fetchExplanation()
        } else if (currentLocation?.city) {
            fetchExplanationByCity()
        }
    }, [currentLocation?.lat, currentLocation?.lon, currentLocation?.city])

    const fetchExplanation = async () => {
        if (!currentLocation?.lat || !currentLocation?.lon) return

        setLoading(true)
        setError(null)

        try {
            const result = await whyTodayService.getExplanationByLocation(
                currentLocation.lat,
                currentLocation.lon
            )
            setExplanation(result)
        } catch (err) {
            console.error('Error fetching Why Today explanation:', err)
            setError('Unable to load explanation')
            setExplanation(whyTodayService.getFallbackExplanation())
        } finally {
            setLoading(false)
        }
    }

    const fetchExplanationByCity = async () => {
        if (!currentLocation?.city) return

        setLoading(true)
        setError(null)

        try {
            const result = await whyTodayService.getExplanationByCity(currentLocation.city)
            setExplanation(result)
        } catch (err) {
            console.error('Error fetching Why Today explanation:', err)
            setError('Unable to load explanation')
            setExplanation(whyTodayService.getFallbackExplanation())
        } finally {
            setLoading(false)
        }
    }

    if (loading) {
        return (
            <div className="space-y-3 h-[calc(100%-40px)] overflow-y-auto touch-pan-y animate-pulse">
                <div className="flex items-start space-x-3">
                    <div className="w-6 h-6 bg-gray-600 rounded"></div>
                    <div className="flex-1">
                        <div className="h-4 bg-gray-600 rounded mb-2"></div>
                        <div className="h-3 bg-gray-700 rounded"></div>
                    </div>
                </div>
                <div className="flex items-start space-x-3">
                    <div className="w-6 h-6 bg-gray-600 rounded"></div>
                    <div className="flex-1">
                        <div className="h-4 bg-gray-600 rounded mb-2"></div>
                        <div className="h-3 bg-gray-700 rounded"></div>
                    </div>
                </div>
            </div>
        )
    }

    if (error && !explanation) {
        return (
            <div className="space-y-3 h-[calc(100%-40px)] overflow-y-auto touch-pan-y text-center">
                <div className="text-red-400 text-sm">
                    <span className="text-xl mb-2 block">⚠️</span>
                    {error}
                </div>
            </div>
        )
    }

    if (!explanation) {
        return null
    }

    return (
        <div className="space-y-3 h-[calc(100%-40px)] overflow-y-auto touch-pan-y" style={{
            WebkitOverflowScrolling: 'touch',
            scrollBehavior: 'smooth'
        }}>
            {}
            <div className="mb-4 p-3 rounded-lg border" style={{
                backgroundColor: `${explanation.aqiColor}15`,
                borderColor: `${explanation.aqiColor}50`
            }}>
                <h3 className="text-white font-bold text-sm mb-2" style={{ color: explanation.aqiColor }}>
                    {explanation.aqiStatus} AIR QUALITY
                </h3>
                <p className="text-white text-xs font-medium">
                    {explanation.mainSummary}
                </p>
            </div>

            {}
            {explanation.factors.map((factor, index) => (
                <div key={index} className="flex items-start space-x-3">
                    <span className="text-xl">{factor.icon}</span>
                    <div>
                        <h3 className="text-white font-semibold text-sm">{factor.title}</h3>
                        <p className="text-gray-300 text-xs">{factor.description}</p>
                    </div>
                </div>
            ))}

            {}
            {explanation.healthAdvice && (
                <div className="mt-3 p-3 bg-blue-900/30 rounded-lg border border-blue-700/50">
                    <h4 className="text-blue-300 font-semibold mb-1 text-sm">Health Recommendation</h4>
                    <p className="text-blue-200 text-xs">{explanation.healthAdvice}</p>
                </div>
            )}

            {}
            <div className="mt-2 text-right">
                <span className="text-gray-500 text-xs">
                    Powered by NASA TEMPO • Updated {new Date(explanation.timestamp).toLocaleTimeString()}
                </span>
            </div>
        </div>
    )
}

export default WhyTodayComponent
