import React, { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import EarthBackground from '../components/EarthBackground'
import FullLiquidGlass from '../components/FullLiquidGlass'
import GeolocationSearch from '../components/GeolocationSearch'
import AQIDisplay from '../components/AQIDisplay'
import WhyTodayComponent from '../components/WhyTodayComponent'
import DashboardModal from '../components/DashboardModal'
import DayPlannerComponent from '../components/DayPlannerComponent'
import NotificationHistory from '../components/NotificationHistory'
import { useAQI } from '../hooks/useAQI'
import forecastService from '../services/forecastService'
import whyTodayService from '../services/whyTodayServiceNew'
import '../styles/liquid-glass.css'

const AlertsModal = ({ onClose }) => {
    const [alerts, setAlerts] = useState([])
    const [loading, setLoading] = useState(true)
    const user_id = 'demo_user_123' // In production, get from auth

    useEffect(() => {
        loadAlerts()
    }, [])

    const loadAlerts = async () => {
        try {
            const response = await fetch('/api/alerts/demo')
            const data = await response.json()
            if (data.success) {
                setAlerts(data.scenarios || [])
            }
        } catch (error) {
            console.error('Error loading alerts:', error)
            setAlerts([
                {
                    alert_id: 'demo_1',
                    alert_level: 'unhealthy_sensitive',
                    pollutant: 'PM25',
                    aqi_value: 125,
                    location_city: 'Current Location',
                    epa_message: 'Sensitive groups should reduce outdoor activities',
                    timestamp: new Date().toISOString()
                }
            ])
        }
        setLoading(false)
    }

    const dismissAlert = async (alertId) => {
        try {
            const response = await fetch(`/api/alerts/${alertId}/dismiss`, {
                method: 'POST'
            })
            if (response.ok) {
                setAlerts(alerts.filter(alert => alert.alert_id !== alertId))
            }
        } catch (error) {
            console.error('Error dismissing alert:', error)
            setAlerts(alerts.filter(alert => alert.alert_id !== alertId))
        }
    }

    const getAlertColor = (level) => {
        const colors = {
            'moderate': 'bg-yellow-900/30 border-yellow-700/50',
            'unhealthy_sensitive': 'bg-orange-900/30 border-orange-700/50',
            'unhealthy': 'bg-red-900/30 border-red-700/50',
            'very_unhealthy': 'bg-purple-900/30 border-purple-700/50',
            'hazardous': 'bg-red-900/50 border-red-800/70'
        }
        return colors[level] || 'bg-gray-900/30 border-gray-700/50'
    }

    const getAlertTextColor = (level) => {
        const colors = {
            'moderate': 'text-yellow-300',
            'unhealthy_sensitive': 'text-orange-300',
            'unhealthy': 'text-red-300',
            'very_unhealthy': 'text-purple-300',
            'hazardous': 'text-red-200'
        }
        return colors[level] || 'text-gray-300'
    }

    const formatAlertLevel = (level) => {
        return level.replace('_', ' ').split(' ').map(word =>
            word.charAt(0).toUpperCase() + word.slice(1)
        ).join(' ')
    }

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
            <div className="bg-gray-900 rounded-2xl p-6 max-w-2xl w-full mx-4 border border-gray-700 max-h-[80vh] overflow-y-auto">
                <div className="flex items-center justify-between mb-6">
                    <div className="flex items-center space-x-3">
                        <h3 className="text-white text-xl font-bold">🚨 Air Quality Alerts</h3>
                        <span className="px-3 py-1 bg-blue-500/20 text-blue-300 rounded-full text-sm">
                            {alerts.length} active
                        </span>
                    </div>
                    <div className="flex items-center space-x-2">
                        <Link
                            to="/alerts"
                            className="px-3 py-1 bg-blue-500/20 text-blue-300 hover:bg-blue-500/30 rounded-lg text-sm transition-colors"
                        >
                            View All
                        </Link>
                        <button
                            onClick={onClose}
                            className="text-gray-400 hover:text-white"
                        >
                            ✕
                        </button>
                    </div>
                </div>

                {loading ? (
                    <div className="flex items-center justify-center py-8">
                        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-white"></div>
                        <span className="ml-3 text-gray-300">Loading alerts...</span>
                    </div>
                ) : alerts.length === 0 ? (
                    <div className="text-center py-8">
                        <div className="text-4xl mb-3">🌟</div>
                        <h4 className="text-white font-semibold mb-2">No Active Alerts</h4>
                        <p className="text-gray-400">Air quality is looking good! We'll notify you if conditions change.</p>
                    </div>
                ) : (
                    <div className="space-y-4">
                        {alerts.map((alert) => (
                            <div
                                key={alert.alert_id}
                                className={`p-4 ${getAlertColor(alert.alert_level)} border rounded-lg relative`}
                            >
                                <div className="flex items-start justify-between">
                                    <div className="flex-1">
                                        <div className="flex items-center space-x-2 mb-2">
                                            <span className={`w-2 h-2 ${alert.alert_level === 'hazardous' ? 'bg-red-500' : alert.alert_level === 'unhealthy' ? 'bg-red-400' : alert.alert_level === 'unhealthy_sensitive' ? 'bg-orange-400' : 'bg-yellow-500'} rounded-full`}></span>
                                            <span className={`${getAlertTextColor(alert.alert_level)} font-semibold`}>
                                                {formatAlertLevel(alert.alert_level)}
                                            </span>
                                            <span className="text-gray-300 text-sm">
                                                {alert.pollutant} AQI {alert.aqi_value}
                                            </span>
                                        </div>
                                        <p className={`${getAlertTextColor(alert.alert_level).replace('300', '200')} text-sm mb-2`}>
                                            {alert.location_city}: {alert.epa_message}
                                        </p>
                                        <div className="text-gray-400 text-xs">
                                            {new Date(alert.timestamp).toLocaleString()}
                                        </div>
                                    </div>
                                    <button
                                        onClick={() => dismissAlert(alert.alert_id)}
                                        className="ml-3 px-2 py-1 bg-gray-600/50 text-gray-300 hover:bg-gray-500/50 hover:text-white rounded text-xs transition-colors"
                                    >
                                        Dismiss
                                    </button>
                                </div>
                            </div>
                        ))}
                    </div>
                )}

                {}
                <div className="flex items-center justify-between mt-6 pt-4 border-t border-gray-700">
                    <div className="text-gray-400 text-sm">
                        EPA-compliant health guidance • NASA Space Apps 2025
                    </div>
                    <Link
                        to="/alerts"
                        className="px-4 py-2 bg-gradient-to-r from-blue-500 to-purple-600 text-white rounded-lg hover:from-blue-600 hover:to-purple-700 transition-all text-sm font-medium"
                        onClick={onClose}
                    >
                        Manage Alerts & Preferences
                    </Link>
                </div>
            </div>
        </div>
    )
}

const requestNotificationPermission = async () => {
    if (!("Notification" in window)) {
        console.log("This browser does not support notifications")
        return false
    }

    if (Notification.permission === "granted") {
        return true
    }

    if (Notification.permission !== "denied") {
        const permission = await Notification.requestPermission()
        return permission === "granted"
    }

    return false
}

const showBrowserNotification = async (title, options = {}) => {
    const hasPermission = await requestNotificationPermission()

    if (hasPermission) {
        const notification = new Notification(title, {
            ...options
        })

        setTimeout(() => {
            notification.close()
        }, 10000)

        return notification
    } else {
        console.log("Notification permission denied")
        return null
    }
}

const triggerTestNotification = async (alertData) => {
    setTimeout(async () => {
        console.log('🔔 Triggering real EPA notification after alert setup...')

        try {
            const location = alertData.locations[0]
            const locationName = location.city || 'Selected Location'

            const testLocations = ['New York', 'Rajshahi']
            const apiLocation = testLocations.includes(locationName) ? locationName : testLocations[0]

            const response = await fetch('/api/alerts/test', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ location: apiLocation })
            })

            if (response.ok) {
                const alertApiData = await response.json()
                const alert = alertApiData.alert

                if ("Notification" in window) {
                    if (Notification.permission === "default") {
                        await Notification.requestPermission()
                    }

                    if (Notification.permission === "granted") {
                        const notification = new Notification(`🌫️ Air Quality Alert - ${alert.location.city}`, {
                            body: `Current AQI: ${alert.aqi_value} (${alert.alert_level.charAt(0).toUpperCase() + alert.alert_level.slice(1)})\n${alert.epa_message}`,
                            tag: 'real-epa-alert',
                            requireInteraction: true,
                            silent: false,
                            renotify: true
                        })

                        notification.onclick = () => {
                            window.focus()
                            notification.close()
                        }

                        setTimeout(() => notification.close(), 10000)
                    }
                }

                console.log(`✅ Real EPA notification sent for ${alert.location.city}: AQI ${alert.aqi_value}`)
            } else {
                await showBrowserNotification(
                    `✅ Alert Set Successfully`,
                    {
                        body: `You'll receive notifications for ${locationName} when air quality changes`,
                        tag: 'alert-confirmation'
                    }
                )
            }
        } catch (error) {
            console.error('Error triggering EPA notification:', error)
            await showBrowserNotification(
                `✅ Alert Set Successfully`,
                {
                    body: `Your air quality alerts have been configured successfully`,
                    tag: 'alert-confirmation'
                }
            )
        }
    }, 2000) // 2 seconds delay as requested
}

const AlertSetupModal = ({ onClose, setInAppNotification }) => {
    const [currentStep, setCurrentStep] = useState(0) // Start with overview screen (step 0)
    const [alertData, setAlertData] = useState({
        locations: [{ city: '', lat: null, lng: null }],
        threshold: { value: 100, type: 'category' }, // 'number' or 'category'
        thresholdCategory: 'moderate', // good, moderate, unhealthy_sensitive, unhealthy, very_unhealthy, hazardous
        pollutants: ['all'], // 'all', 'PM25', 'PM10', 'O3', 'NO2', 'SO2', 'CO'
        frequency: 'every_time', // 'every_time', 'once_daily', 'category_change'
        notifications: {
            email: true,
            web_push: true,
            mobile_push: false
        },
        userDetails: {
            name: '',
            email: '',
            timezone: Intl.DateTimeFormat().resolvedOptions().timeZone
        },
        healthConditions: [],
        quietHours: {
            enabled: true,
            start: '22:00',
            end: '07:00'
        },
        alertTypes: {
            immediate: true,
            forecast: true,
            daily_summary: false
        }
    })
    const [loading, setLoading] = useState(false)
    const [savedAlerts, setSavedAlerts] = useState([])
    const [userEmail, setUserEmail] = useState('') // Track user email separately

    const [locationSuggestions, setLocationSuggestions] = useState({})
    const [showSuggestions, setShowSuggestions] = useState({})
    const [searchingLocation, setSearchingLocation] = useState({})

    useEffect(() => {
        loadExistingAlerts()
        loadExistingPreferences()
    }, [])

    const loadExistingPreferences = async () => {
        try {
            console.log('Loading user preferences')
            setUserEmail('')
        } catch (error) {
            console.error('Error loading preferences:', error)
        }
    }

    const loadExistingAlerts = async () => {
        try {
            if (userEmail || alertData.userDetails.email) {
                const email = userEmail || alertData.userDetails.email
                console.log(`🔄 Loading alerts for email: ${email}`)
                const response = await fetch(`/api/alerts/user-by-email/${encodeURIComponent(email)}`)
                const data = await response.json()

                if (data.success && data.alerts) {
                    setSavedAlerts(data.alerts)
                    console.log(`✅ Loaded ${data.alerts.length} existing alerts:`, data.alerts)
                } else {
                    setSavedAlerts([])
                    console.log('❌ No alerts found or failed to load')
                }
            } else {
                setSavedAlerts([])
                console.log('⚠️ No email available for loading alerts')
            }
        } catch (error) {
            console.error('Error loading saved alerts:', error)
            setSavedAlerts([])
        }
    }

    const handleLocationAdd = () => {
        setAlertData(prev => ({
            ...prev,
            locations: [...prev.locations, { city: '', lat: null, lng: null }]
        }))
    }

    const handleLocationUpdate = (index, field, value) => {
        setAlertData(prev => ({
            ...prev,
            locations: prev.locations.map((loc, i) =>
                i === index ? { ...loc, [field]: value } : loc
            )
        }))
    }

    const handleLocationRemove = (index) => {
        if (alertData.locations.length > 1) {
            setAlertData(prev => ({
                ...prev,
                locations: prev.locations.filter((_, i) => i !== index)
            }))
        }
    }

    const searchLocations = async (query, index) => {
        if (query.length < 2) {
            setLocationSuggestions(prev => ({ ...prev, [index]: [] }))
            setShowSuggestions(prev => ({ ...prev, [index]: false }))
            return
        }

        setSearchingLocation(prev => ({ ...prev, [index]: true }))

        try {
            const response = await fetch(
                `https://nominatim.openstreetmap.org/search?format=json&limit=5&q=${encodeURIComponent(query)}&countrycodes=us,ca,gb,au,bd,in,jp,de,fr,es,it,br,mx,ar,cl,co,pe,ve,ec,py,uy,bo,sr,gy,fk`
            )
            const data = await response.json()

            const suggestions = data.map(item => ({
                display_name: item.display_name,
                name: item.name,
                lat: parseFloat(item.lat),
                lng: parseFloat(item.lon),
                type: item.type,
                importance: item.importance
            }))

            setLocationSuggestions(prev => ({ ...prev, [index]: suggestions }))
            setShowSuggestions(prev => ({ ...prev, [index]: true }))

        } catch (error) {
            console.error('Error searching locations:', error)
            setLocationSuggestions(prev => ({ ...prev, [index]: [] }))
        } finally {
            setSearchingLocation(prev => ({ ...prev, [index]: false }))
        }
    }

    const selectLocationSuggestion = (index, suggestion) => {
        handleLocationUpdate(index, 'city', suggestion.display_name)
        handleLocationUpdate(index, 'lat', suggestion.lat)
        handleLocationUpdate(index, 'lng', suggestion.lng)
        setShowSuggestions(prev => ({ ...prev, [index]: false }))
    }

    const detectCurrentLocation = async (index) => {
        setSearchingLocation(prev => ({ ...prev, [index]: true }))

        try {
            const position = await new Promise((resolve, reject) => {
                navigator.geolocation.getCurrentPosition(resolve, reject, {
                    timeout: 10000,
                    enableHighAccuracy: true
                })
            })

            const { latitude, longitude } = position.coords

            const response = await fetch(
                `https://nominatim.openstreetmap.org/reverse?format=json&lat=${latitude}&lon=${longitude}`
            )
            const data = await response.json()

            if (data.display_name) {
                handleLocationUpdate(index, 'city', data.display_name)
                handleLocationUpdate(index, 'lat', latitude)
                handleLocationUpdate(index, 'lng', longitude)
            } else {
                handleLocationUpdate(index, 'city', `${latitude.toFixed(4)}, ${longitude.toFixed(4)}`)
                handleLocationUpdate(index, 'lat', latitude)
                handleLocationUpdate(index, 'lng', longitude)
            }

        } catch (error) {
            console.error('Error detecting location:', error)
            alert('Unable to detect your location. Please check your browser permissions and try again.')
        } finally {
            setSearchingLocation(prev => ({ ...prev, [index]: false }))
        }
    }

    const handlePollutantToggle = (pollutant) => {
        if (pollutant === 'all') {
            setAlertData(prev => ({
                ...prev,
                pollutants: ['all']
            }))
        } else {
            setAlertData(prev => {
                const newPollutants = prev.pollutants.includes('all')
                    ? [pollutant]
                    : prev.pollutants.includes(pollutant)
                        ? prev.pollutants.filter(p => p !== pollutant)
                        : [...prev.pollutants.filter(p => p !== 'all'), pollutant]

                return {
                    ...prev,
                    pollutants: newPollutants.length === 0 ? ['all'] : newPollutants
                }
            })
        }
    }

    const handleHealthConditionToggle = (condition) => {
        setAlertData(prev => ({
            ...prev,
            healthConditions: prev.healthConditions.includes(condition)
                ? prev.healthConditions.filter(c => c !== condition)
                : [...prev.healthConditions, condition]
        }))
    }

    const saveAlert = async () => {
        setLoading(true)
        try {
            // Transform frontend data to match backend expectations
            const backendData = {
                userDetails: alertData.userDetails,
                locations: alertData.locations.map(loc => ({
                    name: loc.city || loc.name,
                    coordinates: [loc.lng, loc.lat], // [lng, lat] format
                    displayName: loc.city || loc.name
                })),
                threshold: {
                    pollutants: alertData.pollutants,
                    aqi: alertData.threshold.value,      // Map value -> aqi
                    alertLevel: alertData.thresholdCategory, // Map thresholdCategory -> alertLevel
                    type: alertData.threshold.type
                },
                notifications: {
                    ...alertData.notifications,
                    frequency: alertData.frequency  // Move frequency into notifications
                },
                healthConditions: alertData.healthConditions,
                quietHours: alertData.quietHours,
                alertTypes: alertData.alertTypes,
                editing_alert_id: alertData.editing_alert_id  // Include editing_alert_id for updates
            }

            console.log('🚀 Sending alert data:', backendData)

            const response = await fetch('/api/alerts/register', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(backendData)
            })

            const result = await response.json()

            if (response.ok && result.success) {
                const action = result.updated ? 'update' : 'registration'
                console.log(`✅ Alert ${action} successful:`, result)

                await loadExistingAlerts()  // Make sure alerts are reloaded

                if (result.updated) {
                    setCurrentStep(0.5) // Show updated alerts list
                } else {
                    setCurrentStep(4) // Success step for new alerts
                }
            } else {
                console.error('❌ Alert operation failed:', result)
                setCurrentStep(4)
            }
        } catch (error) {
            console.error('❌ Error saving alert:', error)
            setCurrentStep(4)
        } finally {
            setLoading(false)
        }
    }

    const editAlert = (alert) => {
        console.log('🔧 Editing alert with data:', alert)

        let parsedPollutants = ['all']
        if (alert.pollutants) {
            try {
                parsedPollutants = typeof alert.pollutants === 'string'
                    ? JSON.parse(alert.pollutants)
                    : alert.pollutants
            } catch (e) {
                console.warn('Failed to parse pollutants:', alert.pollutants)
            }
        }

        setAlertData({
            editing_alert_id: alert.alert_id,
            locations: [{
                city: alert.city || 'Unknown Location',
                name: alert.city || 'Unknown Location',
                lat: alert.latitude,
                lng: alert.longitude,
                coordinates: [alert.longitude, alert.latitude] // [lng, lat] format
            }],
            threshold: {
                value: alert.aqi_threshold || 100,
                aqi: alert.aqi_threshold || 100, // Add aqi field
                type: 'category' // Default to category for AQI
            },
            thresholdCategory: alert.alert_level || 'moderate',
            pollutants: parsedPollutants,
            frequency: 'every_time', // Default frequency
            notifications: {
                email: alert.notification_preferences?.email || true,
                web_push: alert.notification_preferences?.web_push || true,
                mobile_push: false // SMS not implemented yet
            },
            userDetails: {
                name: alertData.userDetails.name || '',
                email: userEmail,
                timezone: alert.notification_preferences?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone
            },
            healthConditions: [], // Not implemented in current schema
            quietHours: {
                enabled: alert.notification_preferences?.quiet_hours_enabled || true,
                start: alert.notification_preferences?.quiet_hours_start || '22:00',
                end: alert.notification_preferences?.quiet_hours_end || '07:00'
            },
            alertTypes: {
                immediate: true, // Default
                forecast: true, // Default
                daily_summary: alert.notification_preferences?.daily_summary_enabled || false
            }
        })
        setCurrentStep(1) // Go to first step for editing
    }

    const deleteAlert = async (alertId) => {
        if (!confirm('Are you sure you want to delete this alert?')) return

        setLoading(true)
        try {
            const response = await fetch(`/api/alerts/delete/${alertId}`, {
                method: 'DELETE'
            })
            const data = await response.json()

            if (data.success) {
                setSavedAlerts(savedAlerts.filter(alert => alert.alert_id !== alertId))
                alert('Alert deleted successfully!')
            } else {
                alert('Failed to delete alert: ' + (data.error || 'Unknown error'))
            }
        } catch (error) {
            console.error('Error deleting alert:', error)
            alert('Error deleting alert: ' + error.message)
        } finally {
            setLoading(false)
        }
    }

    const startNewAlert = () => {
        setAlertData({
            locations: [{ city: '', lat: null, lng: null }],
            threshold: { value: 100, type: 'category' },
            thresholdCategory: 'moderate',
            pollutants: ['all'],
            frequency: 'every_time',
            notifications: {
                email: true,
                web_push: true,
                mobile_push: false
            },
            userDetails: {
                name: '',
                email: userEmail, // Keep the user email
                timezone: Intl.DateTimeFormat().resolvedOptions().timeZone
            },
            healthConditions: [],
            quietHours: {
                enabled: true,
                start: '22:00',
                end: '07:00'
            },
            alertTypes: {
                immediate: true,
                forecast: true,
                daily_summary: false
            }
        })
        setCurrentStep(1) // Start with location/threshold step
    }

    const validateEmail = (email) => {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
        return emailRegex.test(email)
    }

    const proceedWithEmail = async () => {
        if (!userEmail || !validateEmail(userEmail)) {
            alert('Please enter a valid email address to receive alerts.')
            return
        }

        setLoading(true)

        try {
            const response = await fetch(`/api/alerts/user-by-email/${encodeURIComponent(userEmail)}`)
            const data = await response.json()

            if (data.success) {
                setSavedAlerts(data.alerts || [])

                setAlertData(prev => ({
                    ...prev,
                    userDetails: {
                        ...prev.userDetails,
                        email: userEmail,
                        name: data.user_name || prev.userDetails.name
                    }
                }))

                if (data.alerts && data.alerts.length > 0) {
                    setCurrentStep(0.5) // Show existing alerts overview
                } else {
                    setCurrentStep(1) // Go directly to create new alert
                }
            } else {
                setAlertData(prev => ({
                    ...prev,
                    userDetails: { ...prev.userDetails, email: userEmail }
                }))
                setSavedAlerts([])
                setCurrentStep(1)
            }
        } catch (error) {
            console.error('Error fetching user alerts:', error)
            alert('Unable to fetch your existing alerts. You can still create new ones.')
            setAlertData(prev => ({
                ...prev,
                userDetails: { ...prev.userDetails, email: userEmail }
            }))
            setSavedAlerts([])
            setCurrentStep(1)
        } finally {
            setLoading(false)
        }
    }

    const resetForm = () => {
        setCurrentStep(0) // Reset to email entry step
        setAlertData({
            locations: [{ city: '', lat: null, lng: null }],
            threshold: { value: 100, type: 'category' },
            thresholdCategory: 'moderate',
            pollutants: ['all'],
            frequency: 'every_time',
            notifications: {
                email: true,
                web_push: true,
                mobile_push: false
            },
            userDetails: {
                name: '',
                email: '',
                timezone: Intl.DateTimeFormat().resolvedOptions().timeZone
            },
            healthConditions: [],
            quietHours: {
                enabled: true,
                start: '22:00',
                end: '07:00'
            },
            alertTypes: {
                immediate: true,
                forecast: true,
                daily_summary: false
            }
        })
    }

    const categoryThresholds = {
        good: { min: 0, max: 50, color: 'text-green-400' },
        moderate: { min: 51, max: 100, color: 'text-yellow-400' },
        unhealthy_sensitive: { min: 101, max: 150, color: 'text-orange-400' },
        unhealthy: { min: 151, max: 200, color: 'text-red-400' },
        very_unhealthy: { min: 201, max: 300, color: 'text-purple-400' },
        hazardous: { min: 301, max: 500, color: 'text-red-600' }
    }

    const availableHealthConditions = [
        'asthma', 'heart_disease', 'lung_disease', 'elderly',
        'children', 'pregnant', 'outdoor_worker'
    ]

    const pollutantOptions = [
        { value: 'all', label: 'All Pollutants', description: 'Monitor overall AQI' },
        { value: 'PM25', label: 'PM2.5', description: 'Fine particulate matter' },
        { value: 'PM10', label: 'PM10', description: 'Coarse particulate matter' },
        { value: 'O3', label: 'Ozone', description: 'Ground-level ozone' },
        { value: 'NO2', label: 'NO₂', description: 'Nitrogen dioxide' },
        { value: 'SO2', label: 'SO₂', description: 'Sulfur dioxide' },
        { value: 'CO', label: 'CO', description: 'Carbon monoxide' }
    ]

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
            <FullLiquidGlass className="max-w-4xl w-full mx-4 max-h-[90vh]">
                <div className="p-6 max-h-[90vh] overflow-y-auto">
                    {}
                    <div className="flex items-center justify-between mb-6">
                        <div className="flex items-center space-x-3">
                            <h3 className="text-white text-2xl font-bold">🔔 Set Air Quality Alert</h3>
                            {currentStep >= 1 && currentStep <= 4 && (
                                <div className="flex items-center space-x-2">
                                    {[1, 2, 3, 4].map(step => (
                                        <div key={step} className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold transition-all ${currentStep >= step
                                            ? 'bg-blue-500 text-white'
                                            : 'bg-gray-600 text-gray-300'
                                            }`}>
                                            {step === 4 ? '✓' : step}
                                        </div>
                                    ))}
                                </div>
                            )}
                        </div>
                        <button onClick={onClose} className="text-gray-400 hover:text-white text-2xl">✕</button>
                    </div>

                    {}
                    {currentStep === 0 && (
                        <div className="space-y-6">
                            <div className="text-center mb-8">
                                <h4 className="text-xl text-white font-semibold mb-2">📧 Enter Your Email</h4>
                                <p className="text-gray-400">We need your email to send you air quality alerts via AWS</p>
                            </div>

                            <div className="glass-button p-6 rounded-xl">
                                <div className="max-w-md mx-auto">
                                    <label className="block text-white font-semibold mb-4 text-center">
                                        Email Address *
                                    </label>
                                    <input
                                        type="email"
                                        value={userEmail}
                                        onChange={(e) => setUserEmail(e.target.value)}
                                        placeholder="Enter your email address"
                                        className="w-full px-4 py-3 bg-gray-800 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:ring-2 focus:ring-blue-500 focus:border-transparent text-center"
                                        required
                                    />
                                    <p className="text-gray-400 text-sm mt-2 text-center">
                                        This email will be used to send you air quality alerts and store your preferences.
                                    </p>
                                </div>
                            </div>

                            <div className="flex justify-center">
                                <button
                                    onClick={proceedWithEmail}
                                    disabled={!userEmail || !validateEmail(userEmail) || loading}
                                    className="px-8 py-3 bg-gradient-to-r from-blue-500 to-purple-600 text-white rounded-xl hover:from-blue-600 hover:to-purple-700 transition-all font-semibold disabled:opacity-50 disabled:cursor-not-allowed"
                                >
                                    {loading ? '🔄 Loading...' : 'Continue →'}
                                </button>
                            </div>
                        </div>
                    )}

                    {currentStep === 0.5 && (
                        <div className="space-y-6">
                            <div className="text-center mb-8">
                                <h4 className="text-xl text-white font-semibold mb-2">🔔 Your Alert Dashboard</h4>
                                <p className="text-gray-400">Manage your existing alerts or create new ones</p>
                            </div>

                            {}
                            <div className="glass-button p-4 rounded-xl">
                                <div className="flex items-center justify-between">
                                    <div className="flex items-center space-x-3">
                                        <span className="text-xl">👤</span>
                                        <div>
                                            <div className="text-white font-medium">Signed in as:</div>
                                            <div className="text-blue-300">{userEmail}</div>
                                        </div>
                                    </div>
                                    <button
                                        onClick={() => setCurrentStep(0)}
                                        className="text-gray-400 hover:text-white text-sm"
                                    >
                                        Change Email
                                    </button>
                                </div>
                            </div>

                            {}
                            {savedAlerts.length > 0 ? (
                                <div className="glass-button p-6 rounded-xl">
                                    <h5 className="text-white font-semibold mb-4 flex items-center justify-between">
                                        <span>🚨 Your Active Alerts ({savedAlerts.length})</span>
                                    </h5>
                                    <div className="space-y-3 max-h-60 overflow-y-auto">
                                        {savedAlerts.map((alert, index) => (
                                            <div key={alert.alert_id || alert.id || alert.name || index} className="flex items-center justify-between p-4 bg-white/5 rounded-lg">
                                                <div className="flex-1">
                                                    <div className="text-white font-medium">
                                                        📍 {alert.city || alert.location?.city || alert.location?.name || 'Unknown Location'}
                                                    </div>
                                                    <div className="text-gray-400 text-sm">
                                                        AQI ≥ {alert.aqi_threshold} •
                                                        {alert.notification_preferences?.email ? ' 📧' : ''}
                                                        {alert.notification_preferences?.web_push ? ' 🔔' : ''}
                                                        {alert.notification_preferences?.sms ? ' 📱' : ''}
                                                    </div>
                                                    <div className="text-gray-500 text-xs">
                                                        Created: {new Date(alert.created_at).toLocaleDateString()}
                                                    </div>
                                                </div>
                                                <div className="flex items-center space-x-2">
                                                    <span className="px-2 py-1 bg-green-500/20 text-green-300 rounded text-xs">
                                                        {alert.is_active ? 'Active' : 'Inactive'}
                                                    </span>
                                                    <button
                                                        onClick={() => editAlert(alert)}
                                                        className="text-blue-400 hover:text-blue-300 text-sm px-2 py-1 rounded hover:bg-blue-500/20"
                                                        disabled={loading}
                                                    >
                                                        ✏️ Edit
                                                    </button>
                                                    <button
                                                        onClick={() => deleteAlert(alert.alert_id)}
                                                        className="text-red-400 hover:text-red-300 text-sm px-2 py-1 rounded hover:bg-red-500/20"
                                                        disabled={loading}
                                                    >
                                                        🗑️ Delete
                                                    </button>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            ) : (
                                <div className="glass-button p-6 rounded-xl text-center">
                                    <div className="text-6xl mb-4">🌱</div>
                                    <h5 className="text-white font-semibold mb-2">No alerts yet</h5>
                                    <p className="text-gray-400 mb-4">
                                        You haven't set up any air quality alerts yet. Create your first alert to start monitoring air quality in your area.
                                    </p>
                                </div>
                            )}

                            {}
                            <div className="flex justify-center space-x-4">
                                <button
                                    onClick={startNewAlert}
                                    className="px-8 py-3 bg-gradient-to-r from-green-500 to-blue-600 text-white rounded-xl hover:from-green-600 hover:to-blue-700 transition-all font-semibold"
                                >
                                    + Create New Alert
                                </button>
                                <button
                                    onClick={() => setCurrentStep('history')}
                                    className="px-6 py-3 bg-gradient-to-r from-purple-500 to-indigo-600 text-white rounded-xl hover:from-purple-600 hover:to-indigo-700 transition-all font-semibold"
                                >
                                    📧 View History
                                </button>
                                <button
                                    onClick={onClose}
                                    className="px-6 py-3 bg-gray-600 text-white rounded-xl hover:bg-gray-500 transition-all"
                                >
                                    Done
                                </button>
                            </div>
                        </div>
                    )}

                    {currentStep === 1 && (
                        <div className="space-y-6">
                            <div className="text-center mb-8">
                                <h4 className="text-xl text-white font-semibold mb-2">📍 Choose Locations & Thresholds</h4>
                                <p className="text-gray-400">Set up alerts for your important locations</p>
                            </div>

                            {}
                            <div className="glass-button p-6 rounded-xl">
                                <h5 className="text-white font-semibold mb-4 flex items-center">
                                    <span className="text-xl mr-2">🌍</span>
                                    Locations to Monitor
                                </h5>
                                {alertData.locations.map((location, index) => (
                                    <div key={index} className="mb-4 p-4 bg-white/5 rounded-lg">
                                        <div className="flex items-center justify-between mb-3">
                                            <label className="text-gray-300 font-medium">Location {index + 1}</label>
                                            {alertData.locations.length > 1 && (
                                                <button
                                                    onClick={() => handleLocationRemove(index)}
                                                    className="text-red-400 hover:text-red-300 text-sm"
                                                >
                                                    Remove
                                                </button>
                                            )}
                                        </div>

                                        {}
                                        <div className="relative">
                                            <div className="flex space-x-2">
                                                <div className="flex-1 relative">
                                                    <input
                                                        type="text"
                                                        placeholder="Enter city name or search..."
                                                        value={location.city}
                                                        onChange={(e) => {
                                                            handleLocationUpdate(index, 'city', e.target.value)
                                                            searchLocations(e.target.value, index)
                                                        }}
                                                        onFocus={() => {
                                                            if (location.city.length > 1) {
                                                                searchLocations(location.city, index)
                                                            }
                                                        }}
                                                        onBlur={(e) => {
                                                            setTimeout(() => {
                                                                setShowSuggestions(prev => ({ ...prev, [index]: false }))
                                                            }, 200)
                                                        }}
                                                        className="w-full px-4 py-3 bg-gray-800 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                                                    />

                                                    {}
                                                    {searchingLocation[index] && (
                                                        <div className="absolute right-3 top-1/2 transform -translate-y-1/2">
                                                            <div className="animate-spin rounded-full h-5 w-5 border-2 border-blue-500 border-t-transparent"></div>
                                                        </div>
                                                    )}

                                                    {}
                                                    {showSuggestions[index] && locationSuggestions[index]?.length > 0 && (
                                                        <div className="absolute top-full left-0 right-0 z-50 mt-1 bg-gray-800 border border-gray-600 rounded-lg shadow-xl max-h-60 overflow-y-auto">
                                                            {locationSuggestions[index].map((suggestion, sugIndex) => (
                                                                <button
                                                                    key={sugIndex}
                                                                    onClick={() => selectLocationSuggestion(index, suggestion)}
                                                                    className="w-full text-left px-4 py-3 hover:bg-gray-700 transition-colors border-b border-gray-700 last:border-b-0"
                                                                >
                                                                    <div className="text-white font-medium text-sm">
                                                                        {suggestion.name}
                                                                    </div>
                                                                    <div className="text-gray-400 text-xs mt-1">
                                                                        {suggestion.display_name}
                                                                    </div>
                                                                </button>
                                                            ))}
                                                        </div>
                                                    )}
                                                </div>

                                                {}
                                                <button
                                                    onClick={() => detectCurrentLocation(index)}
                                                    disabled={searchingLocation[index]}
                                                    className="px-4 py-3 bg-blue-500/20 text-blue-300 border border-blue-500/30 rounded-lg hover:bg-blue-500/30 transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center space-x-2"
                                                    title="Detect current location"
                                                >
                                                    <span className="text-lg">📍</span>
                                                    <span className="hidden sm:inline text-sm font-medium">Auto</span>
                                                </button>
                                            </div>

                                            {}
                                            {location.lat && location.lng && (
                                                <div className="mt-2 text-xs text-gray-500">
                                                    📍 {location.lat.toFixed(4)}, {location.lng.toFixed(4)}
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                ))}
                                <button
                                    onClick={handleLocationAdd}
                                    className="w-full py-3 border-2 border-dashed border-gray-600 rounded-lg text-gray-400 hover:border-blue-500 hover:text-blue-400 transition-colors"
                                >
                                    + Add Another Location
                                </button>
                            </div>

                            {}
                            <div className="glass-button p-6 rounded-xl">
                                <h5 className="text-white font-semibold mb-4 flex items-center">
                                    <span className="text-xl mr-2">📊</span>
                                    AQI Alert Threshold
                                </h5>

                                <div className="mb-4">
                                    <div className="flex space-x-4 mb-4">
                                        <button
                                            onClick={() => setAlertData(prev => ({ ...prev, threshold: { ...prev.threshold, type: 'category' } }))}
                                            className={`px-4 py-2 rounded-lg transition-all ${alertData.threshold.type === 'category'
                                                ? 'bg-blue-500 text-white'
                                                : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
                                                }`}
                                        >
                                            Category Based
                                        </button>
                                        <button
                                            onClick={() => setAlertData(prev => ({ ...prev, threshold: { ...prev.threshold, type: 'number' } }))}
                                            className={`px-4 py-2 rounded-lg transition-all ${alertData.threshold.type === 'number'
                                                ? 'bg-blue-500 text-white'
                                                : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
                                                }`}
                                        >
                                            Specific Number
                                        </button>
                                    </div>

                                    {alertData.threshold.type === 'category' ? (
                                        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                                            {Object.entries(categoryThresholds).map(([key, { min, max, color }]) => (
                                                <button
                                                    key={key}
                                                    onClick={() => setAlertData(prev => ({ ...prev, thresholdCategory: key }))}
                                                    className={`p-4 rounded-lg border text-left transition-all ${alertData.thresholdCategory === key
                                                        ? 'border-blue-500 bg-blue-500/20'
                                                        : 'border-gray-600 bg-white/5 hover:border-gray-500'
                                                        }`}
                                                >
                                                    <div className={`font-semibold ${color} capitalize`}>
                                                        {key.replace('_', ' ')}
                                                    </div>
                                                    <div className="text-gray-400 text-sm">
                                                        AQI {min}-{max}
                                                    </div>
                                                </button>
                                            ))}
                                        </div>
                                    ) : (
                                        <div>
                                            <label className="block text-gray-300 mb-2">Alert me when AQI exceeds:</label>
                                            <input
                                                type="number"
                                                min="0"
                                                max="500"
                                                value={alertData.threshold.value}
                                                onChange={(e) => setAlertData(prev => ({
                                                    ...prev,
                                                    threshold: { ...prev.threshold, value: parseInt(e.target.value) || 0 }
                                                }))}
                                                className="w-32 px-4 py-2 bg-gray-800 border border-gray-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                                            />
                                            <span className="text-gray-400 ml-2">AQI</span>
                                        </div>
                                    )}
                                </div>
                            </div>

                            <div className="flex justify-end">
                                <button
                                    onClick={() => setCurrentStep(2)}
                                    className="px-8 py-3 bg-gradient-to-r from-blue-500 to-purple-600 text-white rounded-xl hover:from-blue-600 hover:to-purple-700 transition-all font-semibold"
                                >
                                    Next: Choose Pollutants →
                                </button>
                            </div>
                        </div>
                    )}

                    {currentStep === 2 && (
                        <div className="space-y-6">
                            <div className="text-center mb-8">
                                <h4 className="text-xl text-white font-semibold mb-2">🔬 Pollutant Preferences</h4>
                                <p className="text-gray-400">Choose which pollutants to monitor</p>
                            </div>

                            <div className="glass-button p-6 rounded-xl">
                                <h5 className="text-white font-semibold mb-4">Monitor Specific Pollutants</h5>
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    {pollutantOptions.map((pollutant) => (
                                        <label key={pollutant.value} className="flex items-start space-x-3 p-4 bg-white/5 rounded-lg hover:bg-white/10 cursor-pointer transition-colors">
                                            <input
                                                type="checkbox"
                                                checked={alertData.pollutants.includes(pollutant.value)}
                                                onChange={() => handlePollutantToggle(pollutant.value)}
                                                className="w-5 h-5 mt-1 text-blue-500 bg-gray-700 border-gray-600 rounded focus:ring-blue-500"
                                            />
                                            <div>
                                                <div className="text-white font-medium">{pollutant.label}</div>
                                                <div className="text-gray-400 text-sm">{pollutant.description}</div>
                                            </div>
                                        </label>
                                    ))}
                                </div>
                            </div>

                            {}
                            <div className="glass-button p-6 rounded-xl">
                                <h5 className="text-white font-semibold mb-4 flex items-center">
                                    <span className="text-xl mr-2">⏰</span>
                                    Alert Frequency
                                </h5>
                                <div className="space-y-3">
                                    {[
                                        { value: 'every_time', label: 'Every Time Condition is Met', description: 'Immediate alerts when threshold is crossed' },
                                        { value: 'once_daily', label: 'Once Per Day Maximum', description: 'Limit to one alert per day per location' },
                                        { value: 'category_change', label: 'Only When Category Changes', description: 'Alert when AQI moves to a different category' }
                                    ].map((option) => (
                                        <label key={option.value} className="flex items-start space-x-3 p-4 bg-white/5 rounded-lg hover:bg-white/10 cursor-pointer transition-colors">
                                            <input
                                                type="radio"
                                                name="frequency"
                                                value={option.value}
                                                checked={alertData.frequency === option.value}
                                                onChange={(e) => setAlertData(prev => ({ ...prev, frequency: e.target.value }))}
                                                className="w-5 h-5 mt-1 text-blue-500 bg-gray-700 border-gray-600 focus:ring-blue-500"
                                            />
                                            <div>
                                                <div className="text-white font-medium">{option.label}</div>
                                                <div className="text-gray-400 text-sm">{option.description}</div>
                                            </div>
                                        </label>
                                    ))}
                                </div>
                            </div>

                            <div className="flex justify-between">
                                <button
                                    onClick={() => setCurrentStep(1)}
                                    className="px-6 py-3 bg-gray-600 text-white rounded-xl hover:bg-gray-500 transition-all"
                                >
                                    ← Back
                                </button>
                                <button
                                    onClick={() => setCurrentStep(3)}
                                    className="px-8 py-3 bg-gradient-to-r from-blue-500 to-purple-600 text-white rounded-xl hover:from-blue-600 hover:to-purple-700 transition-all font-semibold"
                                >
                                    Next: Notifications →
                                </button>
                            </div>
                        </div>
                    )}

                    {currentStep === 3 && (
                        <div className="space-y-6">
                            <div className="text-center mb-8">
                                <h4 className="text-xl text-white font-semibold mb-2">📱 Notification Preferences</h4>
                                <p className="text-gray-400">How would you like to receive alerts?</p>
                            </div>

                            {}
                            <div className="glass-button p-6 rounded-xl">
                                <h5 className="text-white font-semibold mb-4 flex items-center">
                                    <span className="text-xl mr-2">👤</span>
                                    Contact Information
                                </h5>
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    <div>
                                        <label className="block text-gray-300 mb-2">Full Name</label>
                                        <input
                                            type="text"
                                            value={alertData.userDetails.name}
                                            onChange={(e) => setAlertData(prev => ({
                                                ...prev,
                                                userDetails: { ...prev.userDetails, name: e.target.value }
                                            }))}
                                            className="w-full px-4 py-3 bg-gray-800 border border-gray-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                                            placeholder="Enter your name"
                                        />
                                    </div>
                                    <div>
                                        <label className="block text-gray-300 mb-2">Email Address</label>
                                        <input
                                            type="email"
                                            value={alertData.userDetails.email}
                                            onChange={(e) => setAlertData(prev => ({
                                                ...prev,
                                                userDetails: { ...prev.userDetails, email: e.target.value }
                                            }))}
                                            className="w-full px-4 py-3 bg-gray-800 border border-gray-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                                            placeholder="Enter your email"
                                        />
                                    </div>
                                </div>
                            </div>

                            {}
                            <div className="glass-button p-6 rounded-xl">
                                <h5 className="text-white font-semibold mb-4 flex items-center">
                                    <span className="text-xl mr-2">📬</span>
                                    Notification Methods
                                </h5>
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    {[
                                        { key: 'email', label: '📧 Email Notifications', description: 'Detailed alerts via email' },
                                        { key: 'web_push', label: '🌐 Browser Push', description: 'Browser notifications' },
                                        { key: 'mobile_push', label: '📱 Mobile Push', description: 'Mobile app notifications' }
                                    ].map((method) => (
                                        <label key={method.key} className="flex items-start space-x-3 p-4 bg-white/5 rounded-lg hover:bg-white/10 cursor-pointer transition-colors">
                                            <input
                                                type="checkbox"
                                                checked={alertData.notifications[method.key]}
                                                onChange={(e) => setAlertData(prev => ({
                                                    ...prev,
                                                    notifications: { ...prev.notifications, [method.key]: e.target.checked }
                                                }))}
                                                className="w-5 h-5 mt-1 text-blue-500 bg-gray-700 border-gray-600 rounded focus:ring-blue-500"
                                            />
                                            <div>
                                                <div className="text-white font-medium">{method.label}</div>
                                                <div className="text-gray-400 text-sm">{method.description}</div>
                                            </div>
                                        </label>
                                    ))}
                                </div>
                            </div>

                            {}
                            <div className="glass-button p-6 rounded-xl">
                                <h5 className="text-white font-semibold mb-4 flex items-center">
                                    <span className="text-xl mr-2">🏥</span>
                                    Health Conditions (Optional)
                                </h5>
                                <p className="text-gray-400 text-sm mb-4">
                                    Help us provide personalized health guidance
                                </p>
                                <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
                                    {availableHealthConditions.map((condition) => (
                                        <label key={condition} className="flex items-center space-x-2 p-3 bg-white/5 rounded-lg hover:bg-white/10 cursor-pointer transition-colors">
                                            <input
                                                type="checkbox"
                                                checked={alertData.healthConditions.includes(condition)}
                                                onChange={() => handleHealthConditionToggle(condition)}
                                                className="w-4 h-4 text-blue-500 bg-gray-700 border-gray-600 rounded focus:ring-blue-500"
                                            />
                                            <div className="text-white text-sm">
                                                {condition.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}
                                            </div>
                                        </label>
                                    ))}
                                </div>
                            </div>

                            {}
                            <div className="glass-button p-6 rounded-xl">
                                <h5 className="text-white font-semibold mb-4 flex items-center">
                                    <span className="text-xl mr-2">🌙</span>
                                    Quiet Hours
                                </h5>
                                <div className="mb-4">
                                    <label className="flex items-center space-x-3">
                                        <input
                                            type="checkbox"
                                            checked={alertData.quietHours.enabled}
                                            onChange={(e) => setAlertData(prev => ({
                                                ...prev,
                                                quietHours: { ...prev.quietHours, enabled: e.target.checked }
                                            }))}
                                            className="w-5 h-5 text-blue-500 bg-gray-700 border-gray-600 rounded focus:ring-blue-500"
                                        />
                                        <span className="text-white">Enable quiet hours (emergency alerts only)</span>
                                    </label>
                                </div>

                                {alertData.quietHours.enabled && (
                                    <div className="grid grid-cols-2 gap-4">
                                        <div>
                                            <label className="block text-gray-300 mb-2">Start Time</label>
                                            <input
                                                type="time"
                                                value={alertData.quietHours.start}
                                                onChange={(e) => setAlertData(prev => ({
                                                    ...prev,
                                                    quietHours: { ...prev.quietHours, start: e.target.value }
                                                }))}
                                                className="w-full px-3 py-2 bg-gray-800 border border-gray-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                                            />
                                        </div>
                                        <div>
                                            <label className="block text-gray-300 mb-2">End Time</label>
                                            <input
                                                type="time"
                                                value={alertData.quietHours.end}
                                                onChange={(e) => setAlertData(prev => ({
                                                    ...prev,
                                                    quietHours: { ...prev.quietHours, end: e.target.value }
                                                }))}
                                                className="w-full px-3 py-2 bg-gray-800 border border-gray-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                                            />
                                        </div>
                                    </div>
                                )}
                            </div>

                            {}
                            <div className="glass-button p-6 rounded-xl">
                                <h5 className="text-white font-semibold mb-4 flex items-center">
                                    <span className="text-xl mr-2">⚡</span>
                                    Alert Types
                                </h5>
                                <div className="space-y-3">
                                    {[
                                        { key: 'immediate', label: '🚨 Immediate Alerts', description: 'Real-time alerts when thresholds are crossed' },
                                        { key: 'forecast', label: '🔮 Forecast Warnings', description: '24-hour advance warnings' },
                                        { key: 'daily_summary', label: '📅 Daily Summary', description: 'End-of-day air quality recap' }
                                    ].map((type) => (
                                        <label key={type.key} className="flex items-start space-x-3 p-4 bg-white/5 rounded-lg hover:bg-white/10 cursor-pointer transition-colors">
                                            <input
                                                type="checkbox"
                                                checked={alertData.alertTypes[type.key]}
                                                onChange={(e) => setAlertData(prev => ({
                                                    ...prev,
                                                    alertTypes: { ...prev.alertTypes, [type.key]: e.target.checked }
                                                }))}
                                                className="w-5 h-5 mt-1 text-blue-500 bg-gray-700 border-gray-600 rounded focus:ring-blue-500"
                                            />
                                            <div>
                                                <div className="text-white font-medium">{type.label}</div>
                                                <div className="text-gray-400 text-sm">{type.description}</div>
                                            </div>
                                        </label>
                                    ))}
                                </div>
                            </div>

                            <div className="flex justify-between">
                                <button
                                    onClick={() => setCurrentStep(2)}
                                    className="px-6 py-3 bg-gray-600 text-white rounded-xl hover:bg-gray-500 transition-all"
                                >
                                    ← Back
                                </button>
                                <button
                                    onClick={saveAlert}
                                    disabled={loading}
                                    className="px-8 py-3 bg-gradient-to-r from-green-500 to-blue-600 text-white rounded-xl hover:from-green-600 hover:to-blue-700 transition-all font-semibold disabled:opacity-50"
                                >
                                    {loading ? 'Saving...' : 'Save Alert Settings'}
                                </button>
                            </div>
                        </div>
                    )}

                    {currentStep === 4 && (
                        <div className="text-center space-y-6">
                            <div className="text-6xl mb-4">✅</div>
                            <h4 className="text-2xl text-white font-bold">Alert Settings Saved!</h4>
                            <p className="text-gray-400 mb-6">
                                Your air quality alerts have been configured successfully.
                                You'll receive notifications based on your preferences.
                            </p>

                            {}
                            <div className="glass-button p-6 rounded-xl text-left">
                                <h5 className="text-white font-semibold mb-4">📋 Your Alert Summary</h5>
                                <div className="space-y-3 text-sm">
                                    <div className="flex justify-between">
                                        <span className="text-gray-400">Locations:</span>
                                        <span className="text-white">
                                            {alertData.locations.filter(l => l.city || l.name).map(l => l.city || l.name).join(', ') || `${alertData.locations.filter(l => l.city).length} location(s)`}
                                        </span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-gray-400">Threshold:</span>
                                        <span className="text-white">
                                            {alertData.threshold.type === 'category'
                                                ? alertData.thresholdCategory.replace('_', ' ')
                                                : `AQI > ${alertData.threshold.value}`}
                                        </span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-gray-400">Pollutants:</span>
                                        <span className="text-white">{alertData.pollutants.join(', ')}</span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-gray-400">Notifications:</span>
                                        <span className="text-white">
                                            {Object.entries(alertData.notifications)
                                                .filter(([_, enabled]) => enabled)
                                                .map(([method, _]) => method)
                                                .join(', ')}
                                        </span>
                                    </div>
                                </div>
                            </div>

                            {}
                            {savedAlerts.length > 0 && (
                                <div className="glass-button p-6 rounded-xl">
                                    <h5 className="text-white font-semibold mb-4 flex items-center justify-between">
                                        <span>🔔 Your Active Alerts</span>
                                        <span className="text-sm text-gray-400">({savedAlerts.length})</span>
                                    </h5>
                                    <div className="space-y-3">
                                        {savedAlerts.map((alert) => (
                                            <div key={alert.id || alert.alert_id} className="flex items-center justify-between p-3 bg-white/5 rounded-lg">
                                                <div>
                                                    <div className="text-white font-medium">
                                                        📍 {alert.city || alert.location?.city || alert.location?.name || 'Unknown Location'}
                                                    </div>
                                                    <div className="text-gray-400 text-sm">{alert.threshold} • {alert.notifications}</div>
                                                </div>
                                                <div className="flex items-center space-x-2">
                                                    <span className="px-2 py-1 bg-green-500/20 text-green-300 rounded text-xs">
                                                        {alert.status}
                                                    </span>
                                                    <button className="text-gray-400 hover:text-white text-sm">Edit</button>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}

                            <div className="flex justify-center space-x-4">
                                <button
                                    onClick={async () => {
                                        console.log('🔔 Test Notification button clicked - showing real EPA alert...')

                                        setTimeout(() => {
                                            onClose() // Close the alert modal
                                        }, 500)

                                        if (!("Notification" in window)) {
                                            alert("This browser doesn't support notifications")
                                            return
                                        }

                                        console.log('Current permission:', Notification.permission)

                                        if (Notification.permission === "default") {
                                            console.log('Requesting notification permission...')
                                            const permission = await Notification.requestPermission()
                                            console.log('Permission result:', permission)
                                        }

                                        setTimeout(async () => {
                                            try {
                                                const testLocations = ['New York', 'Rajshahi']
                                                const randomLocation = testLocations[Math.floor(Math.random() * testLocations.length)]

                                                console.log(`🌐 Fetching real EPA alert for ${randomLocation}...`)
                                                const response = await fetch('/api/alerts/test', {
                                                    method: 'POST',
                                                    headers: { 'Content-Type': 'application/json' },
                                                    body: JSON.stringify({ location: randomLocation })
                                                })

                                                if (response.ok) {
                                                    const alertApiData = await response.json()
                                                    const alert = alertApiData.alert
                                                    console.log('📋 EPA Alert Data:', alert)

                                                    if (Notification.permission === "granted") {
                                                        console.log('Creating browser notification...')
                                                        const notification = new Notification(`🌫️ Air Quality Alert - ${alert.location.city}`, {
                                                            body: `Current AQI: ${alert.aqi_value} (${alert.alert_level.charAt(0).toUpperCase() + alert.alert_level.slice(1)})\n${alert.epa_message}`,
                                                            tag: 'real-epa-test',
                                                            requireInteraction: true,
                                                            silent: false,
                                                            renotify: true
                                                        })

                                                        notification.onshow = () => {
                                                            console.log('✅ Notification displayed successfully!')
                                                        }

                                                        notification.onerror = (error) => {
                                                            console.error('❌ Notification error:', error)
                                                        }

                                                        notification.onclick = () => {
                                                            console.log('Notification clicked')
                                                            window.focus()
                                                            notification.close()
                                                        }

                                                        notification.onclose = () => {
                                                            console.log('Notification closed')
                                                        }

                                                        setTimeout(() => notification.close(), 15000)
                                                    } else {
                                                        console.warn('⚠️ Notification permission not granted:', Notification.permission)
                                                    }

                                                    setInAppNotification({
                                                        title: `🌫️ EPA Air Quality Alert - ${alert.location.city}`,
                                                        message: `AQI ${alert.aqi_value} (${alert.alert_level.toUpperCase()}): ${alert.epa_message}`,
                                                        type: alert.alert_level === 'good' ? 'success' : 'warning'
                                                    })

                                                    setTimeout(() => setInAppNotification(null), 10000)

                                                    console.log(`✅ Real EPA test notification shown for ${alert.location.city}: AQI ${alert.aqi_value}`)
                                                } else {
                                                    console.warn('⚠️ API response not ok:', response.status)
                                                    alert('Failed to fetch EPA data. Please try again.')
                                                }
                                            } catch (error) {
                                                console.error('❌ Error with EPA test notification:', error)
                                                alert('Error testing notification. Check console for details.')
                                            }
                                        }, 1000) // Wait 1 second after closing modal
                                    }}
                                    className="px-6 py-3 bg-yellow-500/20 text-yellow-300 rounded-xl hover:bg-yellow-500/30 transition-all"
                                >
                                    🔔 Test & Close Modal
                                </button>
                                <button
                                    onClick={resetForm}
                                    className="px-6 py-3 bg-blue-500/20 text-blue-300 rounded-xl hover:bg-blue-500/30 transition-all"
                                >
                                    Set Another Alert
                                </button>
                                <button
                                    onClick={onClose}
                                    className="px-8 py-3 bg-gradient-to-r from-green-500 to-blue-600 text-white rounded-xl hover:from-green-600 hover:to-blue-700 transition-all font-semibold"
                                >
                                    Done
                                </button>
                            </div>
                        </div>
                    )}

                    {}
                    {currentStep === 'history' && (
                        <div className="space-y-6">
                            <div className="text-center mb-6">
                                <h4 className="text-2xl text-white font-bold mb-2">📧 Notification History</h4>
                                <p className="text-gray-400">
                                    View all past air quality notifications sent to your email
                                </p>
                            </div>

                            {}
                            <div className="glass-button p-4 rounded-xl mb-6">
                                <div className="flex items-center justify-between">
                                    <div className="flex items-center space-x-3">
                                        <span className="text-xl">📧</span>
                                        <div>
                                            <div className="text-white font-medium">Viewing history for:</div>
                                            <div className="text-blue-300">{userEmail}</div>
                                        </div>
                                    </div>
                                    <button
                                        onClick={() => setCurrentStep(0.5)}
                                        className="text-gray-400 hover:text-white text-sm px-3 py-1 rounded hover:bg-white/10"
                                    >
                                        ← Back to Dashboard
                                    </button>
                                </div>
                            </div>

                            {}
                            <div className="glass-button p-6 rounded-xl">
                                <NotificationHistory
                                    userEmail={userEmail}
                                    onClose={() => setCurrentStep(0.5)}
                                />
                            </div>

                            {}
                            <div className="flex justify-center space-x-4">
                                <button
                                    onClick={() => setCurrentStep(0.5)}
                                    className="px-6 py-3 bg-gray-600 text-white rounded-xl hover:bg-gray-500 transition-all"
                                >
                                    ← Back to Dashboard
                                </button>
                                <button
                                    onClick={onClose}
                                    className="px-6 py-3 bg-gradient-to-r from-blue-500 to-purple-600 text-white rounded-xl hover:from-blue-600 hover:to-purple-700 transition-all font-semibold"
                                >
                                    Close
                                </button>
                            </div>
                        </div>
                    )}
                </div>
            </FullLiquidGlass>
        </div>
    )
}

const HowItWorksModal = ({ onClose }) => {
    const [currentStep, setCurrentStep] = useState(0)

    const steps = [
        {
            title: "Data Pipeline Overview",
            icon: "🛰️",
            color: "from-blue-500 to-purple-600"
        },
        {
            title: "NASA TEMPO Satellite",
            icon: "📡",
            color: "from-purple-500 to-pink-600"
        },
        {
            title: "Ground Data Integration",
            icon: "🌐",
            color: "from-green-500 to-teal-600"
        },
        {
            title: "Bias Correction & Fusion",
            icon: "⚖️",
            color: "from-orange-500 to-red-600"
        },
        {
            title: "EPA AQI Calculation",
            icon: "🧮",
            color: "from-teal-500 to-blue-600"
        },
        {
            title: "Weather Forecasting",
            icon: "🌤️",
            color: "from-indigo-500 to-purple-600"
        },
        {
            title: "Health Alerts & UI",
            icon: "💚",
            color: "from-emerald-500 to-green-600"
        }
    ]

    const renderStepContent = () => {
        switch (currentStep) {
            case 0: // Overview
                return (
                    <div className="space-y-4">
                        <div className="bg-gradient-to-r from-blue-500/10 to-purple-600/10 rounded-xl p-4 border border-blue-500/20">
                            <h4 className="text-white font-semibold mb-3">Complete Data Processing Pipeline</h4>
                            <div className="text-sm text-gray-300 space-y-2">
                                <p>🛰️ <strong>TEMPO L2</strong> → 📡 <strong>Regional Extract</strong> → 🌐 <strong>Multi-Source Fusion</strong></p>
                                <p>⚖️ <strong>Bias Correction</strong> → 🧮 <strong>EPA AQI</strong> → � <strong>5-Day Forecast</strong> → �🚨 <strong>Health Alerts</strong> → 💻 <strong>User Interface</strong></p>
                            </div>
                        </div>
                        <div className="grid grid-cols-2 gap-3 text-sm">
                            <div className="bg-gray-800/50 rounded-lg p-3 border border-gray-700/50">
                                <div className="text-blue-400 font-semibold">Data Sources</div>
                                <div className="text-gray-300 text-xs mt-1">NASA TEMPO • EPA AirNow • WAQI • GEOS-CF • GFS</div>
                            </div>
                            <div className="bg-gray-800/50 rounded-lg p-3 border border-gray-700/50">
                                <div className="text-green-400 font-semibold">Processing Speed</div>
                                <div className="text-gray-300 text-xs mt-1">60-70s batch • Parallel collection • AWS Lambda ready</div>
                            </div>
                        </div>
                    </div>
                )

            case 1: // NASA TEMPO
                return (
                    <div className="space-y-4">
                        <div className="bg-gradient-to-r from-purple-500/10 to-pink-600/10 rounded-xl p-4 border border-purple-500/20">
                            <h4 className="text-white font-semibold mb-3">NASA TEMPO Satellite Data Processing</h4>
                            <div className="text-sm text-gray-300 space-y-2">
                                <p><strong>Direct NASA S3 Access:</strong> Bearer token authentication to tempo-tempo bucket</p>
                                <p><strong>Fast Data Retrieval:</strong> Direct bucket access bypasses API delays (seconds vs minutes)</p>
                                <p><strong>Spatial Resolution:</strong> Native 2.2km x 4.4km pixels preserved</p>
                                <p><strong>Pollutants:</strong> NO₂, HCHO with quality flags and cloud fraction</p>
                            </div>
                        </div>

                        <div className="space-y-3">
                            <div className="bg-gray-800/30 rounded-lg p-3 border border-gray-700/50">
                                <div className="text-purple-400 font-semibold text-sm">NASA Quality Filtering</div>
                                <div className="text-xs text-gray-300 mt-1 font-mono">
                                    {"quality_flag == 0 && cloud_fraction < 0.3"}
                                </div>
                                <div className="text-xs text-gray-400 mt-1">
                                    Reference: TEMPO L2-L3 User Guide V1.0 (NASA ASDC)
                                </div>
                            </div>

                            <div className="bg-gray-800/30 rounded-lg p-3 border border-gray-700/50">
                                <div className="text-pink-400 font-semibold text-sm">Processing Formula</div>
                                <div className="text-xs text-gray-300 mt-1 font-mono">
                                    {"column_density_corrected = column_density × quality_weight"}
                                </div>
                                <div className="text-xs text-gray-400 mt-1">
                                    NetCDF4 optimized extraction with regional clipping
                                </div>
                            </div>
                        </div>
                    </div>
                )

            case 2: // Ground Data
                return (
                    <div className="space-y-4">
                        <div className="bg-gradient-to-r from-green-500/10 to-teal-600/10 rounded-xl p-4 border border-green-500/20">
                            <h4 className="text-white font-semibold mb-3">Multi-Source Ground Data Integration</h4>
                        </div>

                        <div className="grid grid-cols-1 gap-3">
                            <div className="bg-gray-800/30 rounded-lg p-3 border border-green-500/30">
                                <div className="flex items-center space-x-2 mb-2">
                                    <span className="text-blue-400">🇺🇸</span>
                                    <div className="text-green-400 font-semibold text-sm">EPA AirNow API</div>
                                </div>
                                <div className="text-xs text-gray-300 space-y-1">
                                    <p><strong>Endpoint:</strong> www.airnow.gov/partners/api-information/</p>
                                    <p><strong>Data:</strong> PM2.5, O₃, NO₂, CO real-time measurements</p>
                                    <p><strong>Coverage:</strong> 2000+ monitoring stations across North America</p>
                                </div>
                            </div>

                            <div className="bg-gray-800/30 rounded-lg p-3 border border-teal-500/30">
                                <div className="flex items-center space-x-2 mb-2">
                                    <span className="text-emerald-400">🌍</span>
                                    <div className="text-teal-400 font-semibold text-sm">World Air Quality Index (WAQI)</div>
                                </div>
                                <div className="text-xs text-gray-300 space-y-1">
                                    <p><strong>Platform:</strong> aqicn.org/data-platform/</p>
                                    <p><strong>Purpose:</strong> International coverage + validation</p>
                                    <p><strong>Integration:</strong> Cross-border air quality continuity</p>
                                </div>
                            </div>
                        </div>
                    </div>
                )

            case 3: // Bias Correction
                return (
                    <div className="space-y-4">
                        <div className="bg-gradient-to-r from-orange-500/10 to-red-600/10 rounded-xl p-4 border border-orange-500/20">
                            <h4 className="text-white font-semibold mb-3">Statistical Fusion & Bias Correction</h4>
                        </div>

                        <div className="space-y-3">
                            <div className="bg-gray-800/30 rounded-lg p-3 border border-orange-500/30">
                                <div className="text-orange-400 font-semibold text-sm mb-2">Weighted Fusion Algorithm</div>
                                <div className="text-xs text-gray-300 font-mono space-y-1">
                                    <p>{"fused_value = Σ(corrected_values[source] × normalized_weights[source])"}</p>
                                    <p>{"AirNow: 0.5, WAQI: 0.3, TEMPO: 0.15, GEOS: 0.05"}</p>
                                </div>
                                <div className="text-xs text-gray-400 mt-2">
                                    4-source weighted average with automatic weight normalization
                                </div>
                            </div>

                            <div className="bg-gray-800/30 rounded-lg p-3 border border-red-500/30">
                                <div className="text-red-400 font-semibold text-sm mb-2">Linear Bias Correction</div>
                                <div className="text-xs text-gray-300 font-mono space-y-1">
                                    <p>{"C_corrected = C_raw × slope + intercept"}</p>
                                    <p>{"NO₂: slope=0.92, HCHO: slope=0.88 (vs ground)"}</p>
                                </div>
                                <div className="text-xs text-gray-400 mt-2">
                                    Pollutant-specific corrections from validation studies
                                </div>
                            </div>

                            <div className="bg-gray-800/30 rounded-lg p-3 border border-yellow-500/30">
                                <div className="text-yellow-400 font-semibold text-sm mb-2">Uncertainty Quantification & ML Enhancement</div>
                                <div className="text-xs text-gray-300 space-y-1">
                                    <p><strong>Confidence Score:</strong> Based on source agreement</p>
                                    <p><strong>Prediction Intervals:</strong> ±15% typical uncertainty</p>
                                    <p><strong>Quality Flags:</strong> High/Medium/Low confidence ratings</p>
                                    <p><strong>Random Forest Model:</strong> Planned for historical data training (enhanced accuracy)</p>
                                </div>
                            </div>
                        </div>
                    </div>
                )

            case 4: // EPA AQI
                return (
                    <div className="space-y-4">
                        <div className="bg-gradient-to-r from-teal-500/10 to-blue-600/10 rounded-xl p-4 border border-teal-500/20">
                            <h4 className="text-white font-semibold mb-3">EPA AQI Calculation (Official Formula)</h4>
                        </div>

                        <div className="space-y-3">
                            <div className="bg-gray-800/30 rounded-lg p-3 border border-teal-500/30">
                                <div className="text-teal-400 font-semibold text-sm mb-2">EPA Linear Interpolation</div>
                                <div className="text-xs text-gray-300 font-mono space-y-1">
                                    <p>{"I = ((I_Hi - I_Lo) / (BP_Hi - BP_Lo)) × (C - BP_Lo) + I_Lo"}</p>
                                </div>
                                <div className="text-xs text-gray-400 mt-2">
                                    Where I = AQI, C = pollutant concentration, BP = breakpoints
                                </div>
                            </div>

                            <div className="bg-gray-800/30 rounded-lg p-3 border border-blue-500/30">
                                <div className="text-blue-400 font-semibold text-sm mb-2">EPA Time Averaging (Strict Compliance)</div>
                                <div className="text-xs text-gray-300 space-y-1">
                                    <p><strong>O₃:</strong> 8-hour rolling average</p>
                                    <p><strong>PM2.5/PM10:</strong> 24-hour average</p>
                                    <p><strong>NO₂/SO₂:</strong> 1-hour maximum</p>
                                    <p><strong>CO:</strong> 8-hour rolling average</p>
                                </div>
                                <div className="text-xs text-gray-400 mt-2">
                                    Reference: EPA Technical Assistance Document (AirNow.gov)
                                </div>
                            </div>

                            <div className="bg-gray-800/30 rounded-lg p-3 border border-purple-500/30">
                                <div className="text-purple-400 font-semibold text-sm mb-2">Health Categories & Colors</div>
                                <div className="text-xs space-y-1">
                                    <p><span className="text-green-400">●</span> Good (0-50): <span className="text-gray-300">Satisfactory air quality</span></p>
                                    <p><span className="text-yellow-400">●</span> Moderate (51-100): <span className="text-gray-300">Acceptable for most people</span></p>
                                    <p><span className="text-orange-400">●</span> Unhealthy for Sensitive (101-150): <span className="text-gray-300">Sensitive groups affected</span></p>
                                </div>
                            </div>
                        </div>
                    </div>
                )

            case 5: // Weather Forecasting
                return (
                    <div className="space-y-4">
                        <div className="bg-gradient-to-r from-indigo-500/10 to-purple-600/10 rounded-xl p-4 border border-indigo-500/20">
                            <h4 className="text-white font-semibold mb-3">Weather Data & Air Quality Forecasting</h4>
                        </div>

                        <div className="grid grid-cols-1 gap-3">
                            <div className="bg-gray-800/30 rounded-lg p-3 border border-indigo-500/30">
                                <div className="flex items-center space-x-2 mb-2">
                                    <span className="text-indigo-400">🌤️</span>
                                    <div className="text-indigo-400 font-semibold text-sm">NASA GEOS-CF (Chemical Forecasting)</div>
                                </div>
                                <div className="text-xs text-gray-300 space-y-1">
                                    <p><strong>Source:</strong> GMAO GEOS Composition Forecast</p>
                                    <p><strong>Resolution:</strong> 25km global, 3D chemical transport</p>
                                    <p><strong>Data:</strong> O₃, NO₂, CO, PM2.5 with 5-day forecasts</p>
                                </div>
                                <div className="text-xs text-gray-400 mt-2">
                                    Reference: Knowland et al., GMAO Technical Documentation
                                </div>
                            </div>

                            <div className="bg-gray-800/30 rounded-lg p-3 border border-purple-500/30">
                                <div className="flex items-center space-x-2 mb-2">
                                    <span className="text-purple-400">🌪️</span>
                                    <div className="text-purple-400 font-semibold text-sm">NOAA GFS (Meteorological Data)</div>
                                </div>
                                <div className="text-xs text-gray-300 space-y-1">
                                    <p><strong>Endpoint:</strong> nomads.ncep.noaa.gov/gribfilter.php</p>
                                    <p><strong>Parameters:</strong> Wind speed/direction, temperature, humidity</p>
                                    <p><strong>Purpose:</strong> Air pollutant transport & dispersion modeling (5-day forecasts)</p>
                                </div>
                            </div>

                            <div className="bg-gray-800/30 rounded-lg p-3 border border-cyan-500/30">
                                <div className="text-cyan-400 font-semibold text-sm mb-2">Forecast Integration & ML Enhancement</div>
                                <div className="text-xs text-gray-300 font-mono space-y-1">
                                    <p>{"AQI_forecast(t) = f(GEOS_CF(t), GFS_wind(t), current_conditions)"}</p>
                                    <p>{"uncertainty(t) = σ × √(t - t₀) + model_error"}</p>
                                    <p className="text-yellow-300">{"+ Random Forest model (future: historical training)"}</p>
                                </div>
                                <div className="text-xs text-gray-400 mt-2">
                                    5-day predictions using GEOS-CF & GFS with ML-enhanced accuracy planned
                                </div>
                            </div>
                        </div>
                    </div>
                )

            case 6: // Health Alerts
                return (
                    <div className="space-y-4">
                        <div className="bg-gradient-to-r from-emerald-500/10 to-green-600/10 rounded-xl p-4 border border-emerald-500/20">
                            <h4 className="text-white font-semibold mb-3">Personalized Health Alerts & User Interface</h4>
                        </div>

                        <div className="space-y-3">
                            <div className="bg-gray-800/30 rounded-lg p-3 border border-emerald-500/30">
                                <div className="text-emerald-400 font-semibold text-sm mb-2">Sensitive Group Targeting</div>
                                <div className="text-xs text-gray-300 space-y-1">
                                    <p><strong>Respiratory Conditions:</strong> Asthma, COPD, bronchitis</p>
                                    <p><strong>Cardiovascular:</strong> Heart disease, high blood pressure</p>
                                    <p><strong>Vulnerable Populations:</strong> Children, elderly (65+), pregnant women</p>
                                    <p><strong>Outdoor Workers:</strong> Construction, delivery, athletics</p>
                                </div>
                            </div>

                            <div className="bg-gray-800/30 rounded-lg p-3 border border-green-500/30">
                                <div className="text-green-400 font-semibold text-sm mb-2">Alert Delivery System</div>
                                <div className="text-xs text-gray-300 space-y-1">
                                    <p><strong>Multi-Channel:</strong> Email, SMS, push notifications, dashboard</p>
                                    <p><strong>Smart Frequency:</strong> Max 4 alerts/day (prevent fatigue)</p>
                                    <p><strong>Custom Thresholds:</strong> User-defined AQI trigger levels</p>
                                    <p><strong>Activity Recommendations:</strong> "Postpone jogging until 3 PM"</p>
                                </div>
                            </div>

                            <div className="bg-gray-800/30 rounded-lg p-3 border border-blue-500/30">
                                <div className="text-blue-400 font-semibold text-sm mb-2">Liquid Glass UI Components</div>
                                <div className="text-xs text-gray-300 space-y-1">
                                    <p><strong>AQI Dial:</strong> Real-time air quality with color coding</p>
                                    <p><strong>Day Planner:</strong> Hourly forecasts for activity planning</p>
                                    <p><strong>"Why Today?" Explainer:</strong> Pollutant breakdown & weather impact</p>
                                    <p><strong>Interactive Maps:</strong> Regional air quality visualization</p>
                                </div>
                            </div>
                        </div>

                        <div className="bg-gradient-to-r from-blue-500/10 to-purple-600/10 rounded-xl p-3 border border-blue-500/20 text-center">
                            <div className="text-white font-semibold text-sm">🌍 From NASA's TEMPO Mission to Personal Health Protection</div>
                            <div className="text-xs text-gray-300 mt-1">
                                Complete pipeline: 10-hour data freshness • Continental coverage • EPA compliance
                            </div>
                        </div>
                    </div>
                )

            default:
                return null
        }
    }

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
            <FullLiquidGlass className="max-w-4xl w-full mx-4 max-h-[90vh] overflow-hidden">
                <div className="p-6 space-y-4">
                    {}
                    <div className="flex items-center justify-between">
                        <div className="flex items-center space-x-3">
                            <div className={`w-10 h-10 rounded-xl bg-gradient-to-r ${steps[currentStep].color} flex items-center justify-center text-xl`}>
                                {steps[currentStep].icon}
                            </div>
                            <div>
                                <h3 className="text-white text-xl font-bold">How Safer Skies Works</h3>
                                <p className="text-gray-400 text-sm">{steps[currentStep].title}</p>
                            </div>
                        </div>
                        <button
                            onClick={onClose}
                            className="text-gray-400 hover:text-white transition-colors"
                        >
                            ✕
                        </button>
                    </div>

                    {}
                    <div className="flex items-center space-x-2">
                        {steps.map((_, index) => (
                            <div
                                key={index}
                                className={`flex-1 h-1 rounded-full transition-all duration-300 ${index <= currentStep
                                    ? 'bg-gradient-to-r from-blue-500 to-purple-600'
                                    : 'bg-gray-700'
                                    }`}
                            />
                        ))}
                    </div>

                    {}
                    <div className="min-h-[400px] overflow-y-auto">
                        {renderStepContent()}
                    </div>

                    {}
                    <div className="flex items-center justify-between pt-4 border-t border-gray-700">
                        <button
                            onClick={() => setCurrentStep(Math.max(0, currentStep - 1))}
                            disabled={currentStep === 0}
                            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${currentStep === 0
                                ? 'bg-gray-800 text-gray-500 cursor-not-allowed'
                                : 'bg-gray-700 text-gray-300 hover:bg-gray-600 hover:text-white'
                                }`}
                        >
                            ← Previous
                        </button>

                        <div className="text-sm text-gray-400">
                            Step {currentStep + 1} of {steps.length}
                        </div>

                        <button
                            onClick={() => setCurrentStep(Math.min(steps.length - 1, currentStep + 1))}
                            disabled={currentStep === steps.length - 1}
                            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${currentStep === steps.length - 1
                                ? 'bg-gray-800 text-gray-500 cursor-not-allowed'
                                : 'bg-gradient-to-r from-blue-500 to-purple-600 text-white hover:from-blue-600 hover:to-purple-700'
                                }`}
                        >
                            Next →
                        </button>
                    </div>

                    {}
                    <div className="flex items-center justify-center space-x-2">
                        {steps.map((step, index) => (
                            <button
                                key={index}
                                onClick={() => setCurrentStep(index)}
                                className={`w-2 h-2 rounded-full transition-all ${index === currentStep
                                    ? 'bg-blue-500 scale-125'
                                    : 'bg-gray-600 hover:bg-gray-500'
                                    }`}
                                title={step.title}
                            />
                        ))}
                    </div>
                </div>
            </FullLiquidGlass>
        </div>
    )
}

const Home = () => {
    const { aqiData, loading: aqiLoading, error: aqiError, fetchAQI } = useAQI()

    const [searchQuery, setSearchQuery] = useState('')
    const [showHowItWorks, setShowHowItWorks] = useState(false)
    const [showMobileSearch, setShowMobileSearch] = useState(false)
    const [showSetAlert, setShowSetAlert] = useState(false)
    const [showDashboard, setShowDashboard] = useState(false)
    const [inAppNotification, setInAppNotification] = useState(null)
    const [selectedDay, setSelectedDay] = useState(0) // 0 = today, 1 = tomorrow, etc.

    const [currentLocation, setCurrentLocation] = useState(null)
    const [locationHistory, setLocationHistory] = useState([])

    const [forecastData, setForecastData] = useState(null)
    const [forecastLoading, setForecastLoading] = useState(true)
    const [forecastError, setForecastError] = useState(null)

    useEffect(() => {
        console.log('🚀 Home.jsx - Loading default AQI data via UnifiedAQIService...');

        fetchAQI(40.713, -74.006, 'New York');
    }, [fetchAQI]);

    const currentAQI = aqiData?.current?.aqi?.overall?.value || aqiData?.current_aqi || 44

    useEffect(() => {
        console.log('🏠 Home.jsx - Real AQI Data:', aqiData);
        console.log('🏠 Home.jsx - Current AQI:', currentAQI);
        console.log('🏠 Home.jsx - AQI path check:');
        console.log('  - aqiData?.current?.aqi?.overall?.value:', aqiData?.current?.aqi?.overall?.value);
        console.log('  - aqiData?.current_aqi:', aqiData?.current_aqi);
        console.log('  - Final currentAQI used by dial:', currentAQI);
        console.log('🏠 Home.jsx - Loading:', aqiLoading);
        console.log('🏠 Home.jsx - Error:', aqiError);

        if (aqiData) {
            console.log('🔍 AQI Data Structure:', {
                location: aqiData.location,
                current_aqi: aqiData.current_aqi,
                category: aqiData.aqi_category,
                dominant: aqiData.dominant_pollutant
            });
            console.log('📡 Data source debug:', {
                data_source: aqiData.data_source,
                data_sources: aqiData.data_sources,
                first_source: aqiData.data_sources?.[0]
            });
        }
    }, [aqiData, currentAQI, aqiLoading, aqiError]);

    useEffect(() => {
        console.log('🔍 FORECAST useEffect triggered with currentLocation:', currentLocation)
        console.log('🔍 Location check - latitude:', currentLocation?.latitude, 'longitude:', currentLocation?.longitude)

        if (!currentLocation?.latitude || !currentLocation?.longitude) {
            console.log('⏳ Waiting for location before loading forecast data...')
            console.log('  - Missing latitude:', !currentLocation?.latitude)
            console.log('  - Missing longitude:', !currentLocation?.longitude)
            return
        }

        console.log('🚨 FORECAST useEffect TRIGGERED - Starting forecast data loading...')
        const loadForecastData = async () => {
            try {
                setForecastLoading(true)
                const locationName = currentLocation.locationName || extractCityName(currentLocation.address || currentLocation.city)
                console.log(`🔮 Loading 5-day forecast data for location: ${locationName || 'coordinates'} (${currentLocation.latitude}, ${currentLocation.longitude})`)
                const data = await forecastService.getForecastByLocation(currentLocation.latitude, currentLocation.longitude, locationName)
                console.log('🔮 Forecast service returned data:', data)
                setForecastData(data)
                setForecastError(null)

                console.log('✅ Forecast data loaded:', data)
                console.log('📊 DAY PLANNER ACCURACY CHECK:')
                console.log('  📅 Timestamp:', data?.timestamp)
                console.log('  🗓️ Daily forecast entries:', data?.daily?.length || 0)
                console.log('  ⏰ Hourly forecast entries (should be 120):', data?.hourly?.length || 0)

                if (data?.daily) {
                    console.log('  📋 Daily forecast breakdown:')
                    data.data.daily.forEach((day, index) => {
                        console.log(`    Day ${index + 1}:`, {
                            aqi: day.aqi?.avg,
                            category: day.aqi?.category,
                            temp_min: day.weather?.temp_min,
                            temp_max: day.weather?.temp_max,
                            dominant_pollutant: day.aqi?.dominant_pollutant
                        })
                    })
                }

                if (data?.hourly) {
                    const todayHours = data.data.hourly.slice(0, 24)
                    console.log('  ⌚ Today hourly forecast sample (first 6 hours):')
                    todayHours.slice(0, 6).forEach((hour, index) => {
                        console.log(`    Hour ${index}:`, {
                            aqi: hour.aqi?.overall,
                            category: hour.aqi?.category,
                            temperature: hour.weather?.temperature,
                            dominant_pollutant: hour.aqi?.dominant_pollutant
                        })
                    })
                }

            } catch (error) {
                console.error('❌ Failed to load forecast data:', error)
                setForecastError(error.message)
            } finally {
                setForecastLoading(false)
            }
        }

        loadForecastData()
    }, [currentLocation?.latitude, currentLocation?.longitude])

    useEffect(() => {
        if (showMobileSearch) {
            document.body.style.overflow = 'hidden';
        } else {
            document.body.style.overflow = 'unset';
        }

        return () => {
            document.body.style.overflow = 'unset';
        };
    }, [showMobileSearch]);

    const extractCityName = (fullAddress) => {
        if (!fullAddress) return 'Unknown Location';

        const parts = fullAddress.split(',').map(part => part.trim());

        if (parts.length >= 3) {
            if (parts[0] === parts[1]) {
                return parts[0];
            }
            return `${parts[0]}, ${parts[1]}`;
        } else if (parts.length === 2) {
            return `${parts[0]}, ${parts[1]}`;
        } else {
            return parts[0] || 'Unknown Location';
        }
    }

    const handleLocationSelect = (location) => {
        setCurrentLocation(location)
        setLocationHistory(prev => {
            const newHistory = [location, ...prev.filter(loc =>
                loc.latitude !== location.latitude || loc.longitude !== location.longitude
            )]
            return newHistory.slice(0, 5) // Keep only last 5 locations
        })

        setSearchQuery(location.address || `${location.latitude}, ${location.longitude}`)
        console.log('🌍 Location selected:', location)

        if (location.latitude && location.longitude) {
            const properCityName = location.locationName || extractCityName(location.address);
            console.log('🌍 Fetching ALL data for selected location:', location.address, '→ Processed:', properCityName);

            fetchAQI(location.latitude, location.longitude, properCityName);

            console.log('✅ All location data loading initiated (unified service) for:', properCityName);
        }
    }


    const currentCityName = aqiData?.location?.city || 'New York, NY'

    const generateDates = () => {
        const dates = []

        let baseDate
        let isTimezoneAware = false

        if (aqiData?.timestamp) {
            baseDate = new Date(aqiData.timestamp)
            isTimezoneAware = true
            console.log('🌍 Using AQI API timezone-aware timestamp:', aqiData.timestamp)
        } else if (aqiData?.current?.last_updated) {
            baseDate = new Date(aqiData.current.last_updated)
            isTimezoneAware = true
            console.log('🌍 Using AQI current timestamp:', aqiData.current.last_updated)
        } else if (forecastData?.timestamp) {
            baseDate = new Date(forecastData.timestamp)
            isTimezoneAware = true
            console.log('🔮 Using forecast API timestamp:', forecastData.timestamp)
        } else {
            baseDate = new Date()
            isTimezoneAware = false
            console.log('⚠️ Using system time fallback')
        }

        for (let i = 0; i < 5; i++) {
            const date = new Date(baseDate)
            date.setDate(date.getDate() + i)

            let dayName, dateString

            if (isTimezoneAware) {
                dayName = i === 0 ? 'Today' : i === 1 ? 'Tomorrow' : date.toLocaleDateString('en-US', { weekday: 'long' })
                dateString = date.toLocaleDateString('en-US', {
                    month: 'short',
                    day: 'numeric'
                })
                console.log(`📅 Day ${i}: ${dayName}, ${dateString} (timezone-aware from API)`)
            } else {
                dayName = i === 0 ? 'Today' : i === 1 ? 'Tomorrow' : date.toLocaleDateString('en-US', { weekday: 'long' })
                dateString = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
                console.log(`📅 Day ${i}: ${dayName}, ${dateString} (fallback system time)`)
            }

            dates.push({
                date: date,
                dayName: dayName,
                dateString: dateString
            })
        }
        return dates
    }

    const dates = generateDates()

    const getHourlyForecast = (dayIndex) => {
        console.log(`⏰ getHourlyForecast called for dayIndex: ${dayIndex}`)
        console.log('🔍 Forecast data structure check:', forecastData)

        if (!forecastData?.hourly) {
            console.log('⚠️ No hourly forecast data available, using fallback mock data')
            console.log('🔍 Expected: forecastData.hourly, Got:', forecastData?.hourly)
            return Array.from({ length: 24 }, (_, i) => ({
                hour: i,
                aqi: Math.floor(Math.random() * 100) + 10 + (dayIndex * 5),
                time: `${i.toString().padStart(2, '0')}:00`
            }))
        }

        console.log('✅ Using real forecast data for hourly forecast')

        let currentHour = 0

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

        const locationLat = currentLocation?.latitude || 40.713 // Default to NYC
        const locationLon = currentLocation?.longitude || -74.006

        const now = new Date()
        const utcHour = now.getUTCHours()
        const timezoneOffset = getTimezoneOffset(locationLat, locationLon)
        currentHour = (utcHour + timezoneOffset + 24) % 24

        console.log('🌍 Location-based time calculation:', {
            location: `${locationLat}, ${locationLon}`,
            utcTime: `${utcHour}:00 UTC`,
            timezoneOffset: `UTC${timezoneOffset >= 0 ? '+' : ''}${timezoneOffset}`,
            localHour: currentHour,
            localTime: `${currentHour}:00 (location time)`
        })

        // For Day 0 (today), start from current hour. For other days, start from hour 0
        let startHour = dayIndex === 0 ? currentHour : 0

        const dayStartIndex = dayIndex * 24
        const dayHours = forecastData.hourly.slice(dayStartIndex, dayStartIndex + 24)

        console.log(`📊 Raw hourly data for day ${dayIndex} (${dayHours.length} hours):`)
        console.log('  Sample hours:', dayHours.slice(0, 3).map((h, i) => ({
            hour: i,
            aqi: h?.aqi, // Direct aqi access
            time: h?.time
        })))

        if (dayIndex === 0) {
            const next24Hours = forecastData.hourly.slice(currentHour, currentHour + 24)

            console.log(`🔄 Today processing: Starting from hour ${currentHour}, showing next 24 hours`)

            const processed = next24Hours.map((hourData, i) => ({
                hour: (currentHour + i) % 24,
                aqi: hourData?.aqi || 0, // Direct aqi value
                time: `${((currentHour + i) % 24).toString().padStart(2, '0')}:00`,
                category: getAQICategory(hourData?.aqi || 0),
                temperature: Math.round(hourData?.temperature || 20),
                dominant_pollutant: 'PM25'
            }))

            console.log(`✅ Processed today's hourly forecast (${processed.length} hours):`)
            console.log('  Sample:', processed.slice(0, 3))
            return processed
        }

        const processed = dayHours.map((hourData, i) => ({
            hour: i,
            aqi: hourData?.aqi || 0, // Direct aqi value
            time: `${i.toString().padStart(2, '0')}:00`,
            category: getAQICategory(hourData?.aqi || 0),
            temperature: Math.round(hourData?.temperature || 20),
            dominant_pollutant: 'PM25'
        }))

        console.log(`✅ Processed day ${dayIndex} hourly forecast (${processed.length} hours):`)
        console.log('  Sample:', processed.slice(0, 3))
        return processed
    }

    const getDailyForecast = () => {
        console.log('🎯 getDailyForecast called - Processing daily forecast data')

        if (!forecastData?.daily) {
            console.log('⚠️ No daily forecast data available, using fallback mock data')
            return [
                { day: 'Today', aqi: currentAQI, icon: '☀️' },
                { day: 'Tomorrow', aqi: 38, icon: '⛅' },
                { day: 'Friday', aqi: 55, icon: '🌫️' },
                { day: 'Saturday', aqi: 28, icon: '🌤️' },
                { day: 'Sunday', aqi: 35, icon: '☀️' }
            ]
        }

        console.log('✅ Using real forecast data for daily forecast')
        const processedDaily = forecastData.daily.map((dailyData, index) => {
            const dayNames = ['Today', 'Tomorrow', 'Day 3', 'Day 4', 'Day 5']
            const icons = ['☀️', '⛅', '🌫️', '🌤️', '☀️'] // Could be made dynamic based on weather/AQI

            const processed = {
                day: dayNames[index] || `Day ${index + 1}`,
                aqi: dailyData.aqi?.avg || 0,
                icon: icons[index] || '🌤️',
                category: dailyData.aqi?.category || 'Good',
                tempMin: Math.round(dailyData.weather?.temp_min || 15),
                tempMax: Math.round(dailyData.weather?.temp_max || 25)
            }

            console.log(`📅 Processed ${processed.day}:`, processed)
            return processed
        })

        console.log('📊 Final daily forecast array:', processedDaily)
        return processedDaily
    }

    const dailyForecast = getDailyForecast()

    const getAQIColor = (aqi) => {
        if (aqi <= 50) return '#2ECC71'
        if (aqi <= 100) return '#F1C40F'
        if (aqi <= 150) return '#E67E22'
        if (aqi <= 200) return '#E74C3C'
        return '#8E44AD'
    }

    const getAQICategory = (aqi) => {
        if (aqi <= 50) return 'Good'
        if (aqi <= 100) return 'Moderate'
        if (aqi <= 150) return 'Unhealthy for Sensitive Groups'
        if (aqi <= 200) return 'Unhealthy'
        return 'Very Unhealthy'
    }

    const getDashArray = (aqi) => {
        const circumference = 2 * Math.PI * 90
        const progress = Math.min(aqi / 300, 1)
        return `${progress * circumference} ${circumference}`
    }

    return (
        <div className="min-h-screen relative overflow-hidden" style={{ background: 'transparent' }}>
            {}
            <EarthBackground />

            {}
            <header className="relative z-40 glass-header pt-6" style={{ pointerEvents: 'auto' }}>
                <div className="max-w-8xl mx-auto px-3 sm:px-4 lg:px-6">
                    <div className="flex items-center justify-between h-24">
                        {}
                        <Link to="/" className="flex items-center space-x-3">
                            <div className="w-10 h-10 bg-gradient-to-r from-blue-500 to-purple-600 rounded-lg flex items-center justify-center glass-button">
                                <span className="text-white font-bold text-base">SS</span>
                            </div>
                            <span className="text-white font-bold text-xl md:text-2xl">Safer Skies</span>
                        </Link>

                        {}
                        <div className="hidden md:flex flex-1 max-w-3xl mx-4 lg:mx-8">
                            <GeolocationSearch
                                onLocationSelect={handleLocationSelect}
                                className="w-full"
                            />
                        </div>

                        {}
                        <div className="flex items-center space-x-3 md:space-x-4 lg:space-x-6">
                            {}
                            <button
                                className="md:hidden p-3 glass-button rounded-lg text-gray-300 hover:text-white transition-all duration-300 hover:scale-105 active:scale-95 touch-manipulation"
                                onClick={() => setShowMobileSearch(true)}
                                aria-label="Open search"
                            >
                                🔍
                            </button>

                            {}
                            <button
                                onClick={() => setShowSetAlert(true)}
                                className="glass-button px-3 md:px-10 py-3 rounded-lg text-gray-300 hover:text-white transition-all duration-300 flex items-center space-x-2 md:space-x-3 md:min-w-[140px] md:justify-center"
                            >
                                <span className="text-xl">🔔</span>
                                <span className="hidden sm:inline text-sm font-medium">Set Alert</span>
                            </button>

                            {}
                            <button
                                onClick={() => setShowHowItWorks(true)}
                                className="glass-button px-3 md:px-10 py-3 rounded-lg text-gray-300 hover:text-white transition-all duration-300 flex items-center space-x-2 md:space-x-3 md:min-w-[160px] md:justify-center"
                            >
                                <span className="text-xl">❓</span>
                                <span className="hidden sm:inline text-sm font-medium">How it works</span>
                            </button>

                            {}
                            <button
                                onClick={() => setShowDashboard(true)}
                                className="glass-button px-3 md:px-10 py-3 rounded-lg text-white bg-gradient-to-r from-blue-500 to-purple-600 hover:from-blue-600 hover:to-purple-700 transition-all duration-300 flex items-center space-x-2 md:space-x-3 md:min-w-[150px] md:justify-center"
                            >
                                <span className="text-xl">📊</span>
                                <span className="hidden sm:inline text-sm font-medium">Dashboard</span>
                            </button>
                        </div>
                    </div>
                </div>
            </header>

            {}
            <main className="relative z-30 max-w-8xl mx-auto px-3 sm:px-4 lg:px-6 py-4 md:py-8" style={{ pointerEvents: 'none' }}>
                {}
                <div className="hidden md:flex gap-4 h-[calc(100vh-140px)]" style={{ pointerEvents: 'none' }}>
                    {}
                    <div className="w-[55%] relative" style={{ pointerEvents: 'none' }}>

                        {}
                        <div className="absolute top-12 left-12 z-20" style={{ pointerEvents: 'none' }}>
                            {}
                            <div className="relative">
                                {}
                                <div className="relative w-96 h-96">
                                    <svg className="w-full h-full transform -rotate-90" viewBox="0 0 200 200">
                                        {}
                                        <circle
                                            cx="100"
                                            cy="100"
                                            r="95"
                                            stroke="rgba(255, 255, 255, 0.1)"
                                            strokeWidth="2"
                                            fill="transparent"
                                        />

                                        {}
                                        <circle
                                            cx="100"
                                            cy="100"
                                            r="85"
                                            stroke="rgba(255, 255, 255, 0.1)"
                                            strokeWidth="12"
                                            fill="transparent"
                                            strokeDasharray="535"
                                            strokeDashoffset="134"
                                        />

                                        {}
                                        <defs>
                                            <linearGradient id="aqiGradient" x1="0%" y1="0%" x2="100%" y2="0%">
                                                <stop offset="0%" stopColor={getAQIColor(currentAQI)} />
                                                <stop offset="100%" stopColor={getAQIColor(currentAQI)} stopOpacity="0.8" />
                                            </linearGradient>
                                        </defs>

                                        <circle
                                            cx="100"
                                            cy="100"
                                            r="85"
                                            stroke="url(#aqiGradient)"
                                            strokeWidth="12"
                                            fill="transparent"
                                            strokeDasharray="535"
                                            strokeDashoffset={535 - (currentAQI / 500) * 401}
                                            strokeLinecap="round"
                                            className="transition-all duration-2000 ease-out drop-shadow-lg"
                                            style={{
                                                filter: `drop-shadow(0 0 20px ${getAQIColor(currentAQI)}40)`,
                                            }}
                                        />

                                        {}
                                        <circle
                                            cx="100"
                                            cy="100"
                                            r="70"
                                            stroke="rgba(255, 255, 255, 0.05)"
                                            strokeWidth="1"
                                            fill="transparent"
                                        />
                                    </svg>

                                    {}
                                    <div className="absolute inset-0 flex flex-col items-center justify-center">
                                        <div className="text-center p-8 rounded-full bg-black/20 backdrop-blur-md border border-white/10">
                                            <div className="text-8xl font-bold text-white mb-2 drop-shadow-lg">
                                                {currentAQI}
                                            </div>
                                            <div
                                                className="text-2xl font-semibold mb-2 drop-shadow-md"
                                                style={{ color: getAQIColor(currentAQI) }}
                                            >
                                                {getAQICategory(currentAQI)}
                                            </div>
                                            <div className="text-white/80 text-lg">{currentCityName}</div>
                                            <div className="text-white/60 text-xs mt-2 leading-tight">
                                                {aqiLoading ? 'Loading...' : (() => {
                                                    const dataSource = aqiData?.data_sources?.[0] || aqiData?.data_source || 'NASA TEMPO'
                                                    const sources = dataSource.includes('\n') ? dataSource.split('\n') : dataSource.split(' + ')
                                                    return sources.map((source, idx) => (
                                                        <div key={idx}>
                                                            {idx === 0 ? `Live ${source}` : source}
                                                        </div>
                                                    ))
                                                })()}
                                            </div>
                                        </div>
                                    </div>                                    {}
                                    <div className="absolute inset-0">
                                        {}
                                        <div className="absolute top-4 left-1/2 transform -translate-x-1/2 text-green-400 text-xs font-semibold">
                                            50
                                        </div>
                                        {}
                                        <div className="absolute top-8 right-8 text-yellow-400 text-xs font-semibold">
                                            100
                                        </div>
                                        {}
                                        <div className="absolute bottom-8 right-8 text-orange-400 text-xs font-semibold">
                                            150
                                        </div>
                                        {}
                                        <div className="absolute bottom-4 left-1/2 transform -translate-x-1/2 text-red-400 text-xs font-semibold">
                                            200
                                        </div>
                                        {}
                                        <div className="absolute bottom-8 left-8 text-purple-400 text-xs font-semibold">
                                            300
                                        </div>
                                        {}
                                        <div className="absolute top-8 left-8 text-red-600 text-xs font-semibold">
                                            500
                                        </div>
                                    </div>
                                </div>

                                {}
                                <div className="absolute -bottom-16 left-1/2 transform -translate-x-1/2 text-center">
                                    <div className="flex items-center justify-center space-x-2 text-white/80">
                                        <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse"></div>
                                        <span className="text-sm">Live Data</span>
                                    </div>
                                    <div className="text-white/60 text-xs mt-1">Updated: Just now</div>
                                </div>
                            </div>
                        </div>
                    </div>

                    {}
                    <div className="w-[45%] flex flex-col gap-4 relative z-50" style={{ pointerEvents: 'auto' }}>
                        {}
                        <div className="h-1/2 relative z-60">
                            <FullLiquidGlass className="h-full">
                                <div className="w-full h-full p-3" style={{ pointerEvents: 'auto' }}>
                                    <div className="flex items-center justify-between mb-2">
                                        <div>
                                            <h2 className="text-white text-lg font-bold">Day Planner</h2>
                                            <p className="text-gray-400 text-xs">{dates[selectedDay].dayName}, {dates[selectedDay].dateString} • <span className="text-gray-500 opacity-75">Source: NASA GEOS-CF + Open-Meteo + GFS</span></p>
                                        </div>
                                        <div className="flex items-center space-x-2">
                                            <button
                                                onClick={() => setSelectedDay(Math.max(0, selectedDay - 1))}
                                                disabled={selectedDay === 0}
                                                className={`p-1.5 rounded-lg transition-all duration-300 ${selectedDay === 0 ? 'text-gray-600 cursor-not-allowed' : 'text-gray-300 hover:text-white glass-button'}`}
                                            >
                                                ←
                                            </button>
                                            <span className="text-white text-xs px-2">
                                                Day {selectedDay + 1}/5
                                            </span>
                                            <button
                                                onClick={() => setSelectedDay(Math.min(4, selectedDay + 1))}
                                                disabled={selectedDay === 4}
                                                className={`p-1.5 rounded-lg transition-all duration-300 ${selectedDay === 4 ? 'text-gray-600 cursor-not-allowed' : 'text-gray-300 hover:text-white glass-button'}`}
                                            >
                                                →
                                            </button>
                                        </div>
                                    </div>

                                    <div className="h-[calc(100%-50px)] overflow-y-auto touch-pan-y" style={{
                                        WebkitOverflowScrolling: 'touch',
                                        scrollBehavior: 'smooth'
                                    }}>
                                        {}
                                        <div className="grid grid-cols-5 gap-1.5">
                                            {getHourlyForecast(selectedDay).map((hour) => (
                                                <div key={hour.hour} className="glass-button rounded-lg p-1.5 text-center hover:bg-white/10 transition-all duration-300 touch-manipulation">
                                                    <div className="text-gray-300 text-xs mb-1">{hour.time}</div>
                                                    <div
                                                        className="w-3 h-3 rounded-full mx-auto mb-1"
                                                        style={{ backgroundColor: getAQIColor(hour.aqi) }}
                                                    ></div>
                                                    <div className="text-white text-xs font-semibold">{hour.aqi}</div>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                </div>
                            </FullLiquidGlass>
                        </div>

                        {}
                        <div className="h-1/2 relative z-60">
                            <FullLiquidGlass className="h-full">
                                <div className="w-full h-full p-3" style={{ pointerEvents: 'auto' }}>
                                    <h2 className="text-white text-lg font-bold mb-2 flex items-center">
                                        <span className="mr-2">🌟</span>
                                        Why Today
                                    </h2>

                                    <WhyTodayComponent
                                        currentLocation={currentLocation ? {
                                            lat: currentLocation.latitude,
                                            lon: currentLocation.longitude,
                                            city: currentLocation.city || currentLocation.address
                                        } : null}
                                    />
                                </div>
                            </FullLiquidGlass>
                        </div>
                    </div>
                </div>

                {}
                <div className="md:hidden space-y-6" style={{ pointerEvents: 'none' }}>
                    {}
                    <div className="w-full flex flex-col items-center justify-center py-8" style={{ pointerEvents: 'none' }}>
                        {}
                        <div className="relative" style={{ pointerEvents: 'none' }}>
                            {}
                            <div className="relative w-96 h-96">
                                <svg className="w-full h-full transform -rotate-90" viewBox="0 0 200 200">
                                    {}
                                    <circle
                                        cx="100"
                                        cy="100"
                                        r="95"
                                        stroke="rgba(255, 255, 255, 0.1)"
                                        strokeWidth="2"
                                        fill="transparent"
                                    />

                                    {}
                                    <circle
                                        cx="100"
                                        cy="100"
                                        r="85"
                                        stroke="rgba(255, 255, 255, 0.1)"
                                        strokeWidth="12"
                                        fill="transparent"
                                        strokeDasharray="535"
                                        strokeDashoffset="134"
                                    />

                                    {}
                                    <defs>
                                        <linearGradient id="aqiGradientMobile" x1="0%" y1="0%" x2="100%" y2="0%">
                                            <stop offset="0%" stopColor={getAQIColor(currentAQI)} />
                                            <stop offset="100%" stopColor={getAQIColor(currentAQI)} stopOpacity="0.8" />
                                        </linearGradient>
                                    </defs>

                                    <circle
                                        cx="100"
                                        cy="100"
                                        r="85"
                                        stroke="url(#aqiGradientMobile)"
                                        strokeWidth="12"
                                        fill="transparent"
                                        strokeDasharray="535"
                                        strokeDashoffset={535 - (currentAQI / 500) * 401}
                                        strokeLinecap="round"
                                        className="transition-all duration-2000 ease-out drop-shadow-lg"
                                        style={{
                                            filter: `drop-shadow(0 0 20px ${getAQIColor(currentAQI)}40)`,
                                        }}
                                    />

                                    {}
                                    <circle
                                        cx="100"
                                        cy="100"
                                        r="70"
                                        stroke="rgba(255, 255, 255, 0.05)"
                                        strokeWidth="1"
                                        fill="transparent"
                                    />
                                </svg>

                                {}
                                <div className="absolute inset-0 flex flex-col items-center justify-center">
                                    <div className="text-center p-8 rounded-full bg-black/20 backdrop-blur-md border border-white/10">
                                        <div className="text-8xl font-bold text-white mb-2 drop-shadow-lg">
                                            {currentAQI}
                                        </div>
                                        <div
                                            className="text-2xl font-semibold mb-2 drop-shadow-md"
                                            style={{ color: getAQIColor(currentAQI) }}
                                        >
                                            {getAQICategory(currentAQI)}
                                        </div>
                                        <div className="text-white/80 text-lg">{currentCityName}</div>
                                        <div className="text-white/60 text-xs mt-2 leading-tight">
                                            {aqiLoading ? 'Loading...' : (() => {
                                                const dataSource = aqiData?.data_sources?.[0] || aqiData?.data_source || 'NASA TEMPO'
                                                const sources = dataSource.includes('\n') ? dataSource.split('\n') : dataSource.split(' + ')
                                                return sources.map((source, idx) => (
                                                    <div key={idx}>
                                                        {idx === 0 ? `Live ${source}` : source}
                                                    </div>
                                                ))
                                            })()}
                                        </div>
                                    </div>
                                </div>

                                {}
                                <div className="absolute inset-0">
                                    {}
                                    <div className="absolute top-4 left-1/2 transform -translate-x-1/2 text-green-400 text-xs font-semibold">
                                        50
                                    </div>
                                    {}
                                    <div className="absolute top-8 right-8 text-yellow-400 text-xs font-semibold">
                                        100
                                    </div>
                                    {}
                                    <div className="absolute bottom-8 right-8 text-orange-400 text-xs font-semibold">
                                        150
                                    </div>
                                    {}
                                    <div className="absolute bottom-4 left-1/2 transform -translate-x-1/2 text-red-400 text-xs font-semibold">
                                        200
                                    </div>
                                    {}
                                    <div className="absolute bottom-8 left-8 text-purple-400 text-xs font-semibold">
                                        300
                                    </div>
                                    {}
                                    <div className="absolute top-8 left-8 text-red-600 text-xs font-semibold">
                                        500
                                    </div>
                                </div>
                            </div>

                            {}
                            <div className="absolute -bottom-16 left-1/2 transform -translate-x-1/2 text-center">
                                <div className="flex items-center justify-center space-x-2 text-white/80">
                                    <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse"></div>
                                    <span className="text-sm">Live Data</span>
                                </div>
                                <div className="text-white/60 text-xs mt-1">Updated: Just now</div>
                            </div>
                        </div>
                    </div>

                    {}
                    <div className="relative z-50">
                        <FullLiquidGlass>
                            <div className="w-full p-4" style={{ pointerEvents: 'auto' }}>
                                <div className="flex items-center justify-between mb-4">
                                    <div>
                                        <h2 className="text-white text-xl font-bold">Day Planner</h2>
                                        <p className="text-gray-400 text-sm">{dates[selectedDay].dayName}, {dates[selectedDay].dateString} • <span className="text-gray-500 opacity-75">Source: NASA GEOS-CF + Open-Meteo + GFS</span></p>
                                    </div>
                                    <div className="flex items-center space-x-2">
                                        <button
                                            onClick={() => setSelectedDay(Math.max(0, selectedDay - 1))}
                                            disabled={selectedDay === 0}
                                            className={`p-2 rounded-lg transition-all duration-300 touch-manipulation ${selectedDay === 0 ? 'text-gray-600 cursor-not-allowed' : 'text-gray-300 hover:text-white glass-button'}`}
                                        >
                                            ←
                                        </button>
                                        <span className="text-white text-sm px-3">
                                            Day {selectedDay + 1}/5
                                        </span>
                                        <button
                                            onClick={() => setSelectedDay(Math.min(4, selectedDay + 1))}
                                            disabled={selectedDay === 4}
                                            className={`p-2 rounded-lg transition-all duration-300 touch-manipulation ${selectedDay === 4 ? 'text-gray-600 cursor-not-allowed' : 'text-gray-300 hover:text-white glass-button'}`}
                                        >
                                            →
                                        </button>
                                    </div>
                                </div>

                                {}
                                <div
                                    className="grid grid-cols-4 gap-2 max-h-96 overflow-y-auto touch-pan-y"
                                    style={{
                                        WebkitOverflowScrolling: 'touch',
                                        scrollBehavior: 'smooth'
                                    }}
                                >
                                    {getHourlyForecast(selectedDay).map((hour) => (
                                        <div key={hour.hour} className="glass-button rounded-lg p-2 text-center hover:bg-white/10 transition-all duration-300 touch-manipulation">
                                            <div className="text-gray-300 text-xs mb-1">{hour.time}</div>
                                            <div
                                                className="w-4 h-4 rounded-full mx-auto mb-1"
                                                style={{ backgroundColor: getAQIColor(hour.aqi) }}
                                            ></div>
                                            <div className="text-white text-xs font-semibold">{hour.aqi}</div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </FullLiquidGlass>
                    </div>

                    {}
                    <div className="relative z-50">
                        <FullLiquidGlass>
                            <div className="w-full p-4" style={{ pointerEvents: 'auto' }}>
                                <h2 className="text-white text-xl font-bold mb-4 flex items-center">
                                    <span className="mr-2">🌟</span>
                                    Why Today
                                </h2>

                                <WhyTodayComponent
                                    currentLocation={currentLocation ? {
                                        lat: currentLocation.latitude,
                                        lon: currentLocation.longitude,
                                        city: currentLocation.city || currentLocation.address
                                    } : null}
                                />
                            </div>
                        </FullLiquidGlass>
                    </div>
                </div>
            </main>

            {}
            {showMobileSearch && (
                <div
                    className="fixed inset-0 z-[9999] flex items-start justify-center bg-black/60 backdrop-blur-sm pt-4 md:pt-20 p-4"
                    onTouchStart={(e) => {
                        if (e.target === e.currentTarget) {
                            e.preventDefault();
                        }
                    }}
                    onClick={(e) => {
                        if (e.target === e.currentTarget && e.detail > 0) {
                            setShowMobileSearch(false);
                        }
                    }}
                    style={{ touchAction: 'none' }}
                >
                    <div
                        className="bg-gray-900/95 backdrop-blur-xl rounded-2xl p-4 md:p-6 w-full max-w-md border border-gray-700/50 shadow-2xl animate-fade-in-up relative z-[10000]"
                        onClick={(e) => e.stopPropagation()}
                        onTouchStart={(e) => e.stopPropagation()}
                    >
                        <div className="flex items-center justify-between mb-4">
                            <h3 className="text-white text-lg md:text-xl font-bold flex items-center">
                                <span className="mr-2">🔍</span>
                                Search Location
                            </h3>
                            <button
                                onClick={() => setShowMobileSearch(false)}
                                onTouchStart={(e) => {
                                    e.preventDefault();
                                    setShowMobileSearch(false);
                                }}
                                className="text-gray-400 hover:text-white text-2xl p-2 hover:bg-gray-800/50 rounded-lg transition-all duration-200 touch-manipulation"
                                aria-label="Close search"
                                style={{ userSelect: 'none' }}
                            >
                                ✕
                            </button>
                        </div>
                        <div className="w-full">
                            <GeolocationSearch
                                key={`mobile-search-${showMobileSearch}`} // Force reset on modal open
                                skipAutoInit={true} // Don't auto-initialize for mobile modal
                                onLocationSelect={(location) => {
                                    handleLocationSelect(location);
                                    setShowMobileSearch(false);
                                }}
                                className="w-full"
                            />
                        </div>
                    </div>
                </div>
            )}

            {}
            {showSetAlert && (
                <AlertSetupModal
                    onClose={() => setShowSetAlert(false)}
                    setInAppNotification={setInAppNotification}
                />
            )}

            {}
            {showDashboard && (
                <DashboardModal
                    isOpen={showDashboard}
                    onClose={() => setShowDashboard(false)}
                    currentLocation={currentLocation ? {
                        lat: currentLocation.latitude,
                        lon: currentLocation.longitude,
                        name: currentLocation.city || currentLocation.address || 'Current Location'
                    } : null}
                />
            )}

            {}
            {showHowItWorks && <HowItWorksModal onClose={() => setShowHowItWorks(false)} />}

            {}
            <svg style={{ display: 'none' }}>
                <filter
                    id="glass-distortion"
                    x="0%"
                    y="0%"
                    width="100%"
                    height="100%"
                    filterUnits="objectBoundingBox"
                >
                    <feTurbulence
                        type="fractalNoise"
                        baseFrequency="0.01 0.01"
                        numOctaves="1"
                        seed="5"
                        result="turbulence"
                    />
                    <feComponentTransfer in="turbulence" result="mapped">
                        <feFuncR type="gamma" amplitude="1" exponent="10" offset="0.5" />
                        <feFuncG type="gamma" amplitude="0" exponent="1" offset="0" />
                        <feFuncB type="gamma" amplitude="0" exponent="1" offset="0.5" />
                    </feComponentTransfer>
                    <feGaussianBlur in="turbulence" stdDeviation="3" result="softMap" />
                    <feSpecularLighting
                        in="softMap"
                        surfaceScale="5"
                        specularConstant="1"
                        specularExponent="100"
                        lightingColor="white"
                        result="specLight"
                    >
                        <fePointLight x="-200" y="-200" z="300" />
                    </feSpecularLighting>
                    <feComposite
                        in="specLight"
                        operator="arithmetic"
                        k1="0"
                        k2="1"
                        k3="1"
                        k4="0"
                        result="litImage"
                    />
                    <feDisplacementMap
                        in="SourceGraphic"
                        in2="softMap"
                        scale="150"
                        xChannelSelector="R"
                        yChannelSelector="G"
                    />
                </filter>
            </svg>

            {}
            {inAppNotification && (
                <div className="fixed top-4 right-4 z-[9999] max-w-sm">
                    <div className="bg-gray-900/95 backdrop-blur-xl border border-gray-700/50 rounded-xl p-4 shadow-2xl animate-slide-in-right">
                        <div className="flex items-start space-x-3">
                            <div className="flex-shrink-0 text-2xl">
                                {inAppNotification.type === 'success' ? '✅' : '🔔'}
                            </div>
                            <div className="flex-1">
                                <h4 className="text-white font-semibold text-sm mb-1">
                                    {inAppNotification.title}
                                </h4>
                                <p className="text-gray-300 text-xs leading-relaxed">
                                    {inAppNotification.message}
                                </p>
                            </div>
                            <button
                                onClick={() => setInAppNotification(null)}
                                className="text-gray-400 hover:text-white text-sm"
                            >
                                ✕
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    )
}

export default Home
