import React, { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'

const AlertsPage = () => {
    const [alerts, setAlerts] = useState([])
    const [preferences, setPreferences] = useState({})
    const [statistics, setStatistics] = useState({})
    const [activeTab, setActiveTab] = useState('alerts')
    const [loading, setLoading] = useState(true)
    const [user_id] = useState('demo_user_123') // In production, get from auth

    const ALERTS_API_BASE = process.env.REACT_APP_ALERTS_API_URL || 'http://localhost:5003'

    useEffect(() => {
        loadAlerts()
        loadPreferences()
        loadStatistics()
    }, [])

    const loadAlerts = async () => {
        try {
            const response = await fetch(`${ALERTS_API_BASE}/api/alerts/user/${user_id}`)
            const data = await response.json()
            if (data.success) {
                setAlerts(data.alerts)
            }
        } catch (error) {
            console.error('Error loading alerts:', error)
        }
    }

    const loadPreferences = async () => {
        try {
            const response = await fetch(`${ALERTS_API_BASE}/api/user/${user_id}/preferences`)
            const data = await response.json()
            if (data.success) {
                setPreferences(data.preferences)
            }
        } catch (error) {
            console.error('Error loading preferences:', error)
        }
        setLoading(false)
    }

    const loadStatistics = async () => {
        try {
            const response = await fetch(`${ALERTS_API_BASE}/api/alerts/statistics`)
            const data = await response.json()
            if (data.success) {
                setStatistics(data.statistics)
            }
        } catch (error) {
            console.error('Error loading statistics:', error)
        }
    }

    const dismissAlert = async (alertId) => {
        try {
            const response = await fetch(`${ALERTS_API_BASE}/api/alerts/${alertId}/dismiss`, {
                method: 'POST'
            })
            const data = await response.json()
            if (data.success) {
                setAlerts(alerts.filter(alert => alert.alert_id !== alertId))
            }
        } catch (error) {
            console.error('Error dismissing alert:', error)
        }
    }

    const updatePreferences = async (newPreferences) => {
        try {
            const response = await fetch(`${ALERTS_API_BASE}/api/user/${user_id}/preferences`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(newPreferences)
            })
            const data = await response.json()
            if (data.success) {
                setPreferences(newPreferences)
            }
        } catch (error) {
            console.error('Error updating preferences:', error)
        }
    }

    const createDemoAlert = async () => {
        try {
            const response = await fetch(`${ALERTS_API_BASE}/api/test/create-demo-alert`, {
                method: 'POST'
            })
            const data = await response.json()
            if (data.success) {
                loadAlerts() // Reload alerts
                loadStatistics() // Reload stats
            }
        } catch (error) {
            console.error('Error creating demo alert:', error)
        }
    }

    const getAlertColor = (level) => {
        const colors = {
            'good': 'border-green-500 bg-green-900/20',
            'moderate': 'border-yellow-500 bg-yellow-900/20',
            'unhealthy_sensitive': 'border-orange-500 bg-orange-900/20',
            'unhealthy': 'border-red-500 bg-red-900/20',
            'very_unhealthy': 'border-purple-500 bg-purple-900/20',
            'hazardous': 'border-red-800 bg-red-900/40'
        }
        return colors[level] || 'border-gray-500 bg-gray-900/20'
    }

    const getAlertIcon = (pollutant, level) => {
        const pollutantIcons = {
            'PM25': '🌫️',
            'PM10': '💨',
            'O3': '☁️',
            'NO2': '🚗',
            'SO2': '🏭',
            'CO': '⚠️'
        }

        if (level === 'hazardous' || level === 'very_unhealthy') {
            return '🚨'
        }

        return pollutantIcons[pollutant] || '🌍'
    }

    const formatAlertLevel = (level) => {
        return level.replace('_', ' ').split(' ').map(word =>
            word.charAt(0).toUpperCase() + word.slice(1)
        ).join(' ')
    }

    const formatTimestamp = (timestamp) => {
        return new Date(timestamp).toLocaleString()
    }

    if (loading) {
        return (
            <div className="min-h-screen bg-gradient-to-br from-blue-900 via-purple-900 to-indigo-900 flex items-center justify-center">
                <div className="text-white text-center">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-white mx-auto mb-4"></div>
                    <p>Loading alerts...</p>
                </div>
            </div>
        )
    }

    return (
        <div className="min-h-screen bg-gradient-to-br from-blue-900 via-purple-900 to-indigo-900">
            {}
            <div className="bg-black/20 backdrop-blur-md border-b border-white/10">
                <div className="container mx-auto px-6 py-4">
                    <div className="flex items-center justify-between">
                        <div className="flex items-center space-x-4">
                            <h1 className="text-2xl font-bold text-white">🚨 Air Quality Alerts</h1>
                            <div className="hidden sm:flex items-center space-x-2">
                                <span className="px-3 py-1 bg-blue-500/20 text-blue-300 rounded-full text-sm">
                                    {alerts.length} Active
                                </span>
                            </div>
                        </div>
                        <button
                            onClick={() => window.history.back()}
                            className="text-gray-300 hover:text-white transition-colors"
                        >
                            ← Back to Dashboard
                        </button>
                    </div>
                </div>
            </div>

            {}
            <div className="container mx-auto px-6 py-4">
                <div className="flex space-x-1 bg-black/20 backdrop-blur-md rounded-xl p-1 w-fit">
                    {[
                        { id: 'alerts', label: '🚨 Active Alerts', count: alerts.length },
                        { id: 'preferences', label: '⚙️ Preferences' },
                        { id: 'statistics', label: '📊 Statistics' }
                    ].map((tab) => (
                        <button
                            key={tab.id}
                            onClick={() => setActiveTab(tab.id)}
                            className={`px-4 py-2 rounded-lg transition-all ${activeTab === tab.id
                                ? 'bg-blue-500 text-white shadow-lg'
                                : 'text-gray-300 hover:text-white hover:bg-white/10'
                                }`}
                        >
                            {tab.label} {tab.count !== undefined && (
                                <span className="ml-2 px-2 py-0.5 bg-white/20 rounded-full text-xs">
                                    {tab.count}
                                </span>
                            )}
                        </button>
                    ))}
                </div>
            </div>

            {}
            <div className="container mx-auto px-6 pb-8">
                <AnimatePresence mode="wait">
                    {}
                    {activeTab === 'alerts' && (
                        <motion.div
                            key="alerts"
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            exit={{ opacity: 0, y: -20 }}
                            className="space-y-4"
                        >
                            {}
                            <div className="bg-black/20 backdrop-blur-md rounded-xl p-4 border border-white/10">
                                <div className="flex items-center justify-between">
                                    <div>
                                        <h3 className="text-white font-semibold">Testing & Demo</h3>
                                        <p className="text-gray-300 text-sm">Create demo alerts to test the notification system</p>
                                    </div>
                                    <button
                                        onClick={createDemoAlert}
                                        className="px-4 py-2 bg-gradient-to-r from-blue-500 to-purple-600 text-white rounded-lg hover:from-blue-600 hover:to-purple-700 transition-all"
                                    >
                                        Create Demo Alert
                                    </button>
                                </div>
                            </div>

                            {}
                            {alerts.length === 0 ? (
                                <div className="bg-black/20 backdrop-blur-md rounded-xl p-8 border border-white/10 text-center">
                                    <div className="text-6xl mb-4">🌟</div>
                                    <h3 className="text-xl font-bold text-white mb-2">No Active Alerts</h3>
                                    <p className="text-gray-300">Air quality is looking good! We'll notify you if conditions change.</p>
                                </div>
                            ) : (
                                <div className="space-y-4">
                                    {alerts.map((alert) => (
                                        <motion.div
                                            key={alert.alert_id}
                                            initial={{ opacity: 0, scale: 0.95 }}
                                            animate={{ opacity: 1, scale: 1 }}
                                            className={`bg-black/20 backdrop-blur-md rounded-xl p-6 border ${getAlertColor(alert.alert_level)}`}
                                        >
                                            <div className="flex items-start justify-between">
                                                <div className="flex items-start space-x-4 flex-1">
                                                    <div className="text-3xl">
                                                        {getAlertIcon(alert.pollutant, alert.alert_level)}
                                                    </div>
                                                    <div className="flex-1">
                                                        <div className="flex items-center space-x-2 mb-2">
                                                            <h3 className="text-white font-bold text-lg">
                                                                {formatAlertLevel(alert.alert_level)} Air Quality
                                                            </h3>
                                                            <span className="px-2 py-1 bg-white/20 text-white rounded-full text-xs">
                                                                {alert.location_city}
                                                            </span>
                                                        </div>

                                                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                                                            <div>
                                                                <span className="text-gray-300 text-sm">Pollutant</span>
                                                                <div className="text-white font-semibold">{alert.pollutant}</div>
                                                            </div>
                                                            <div>
                                                                <span className="text-gray-300 text-sm">AQI Value</span>
                                                                <div className="text-white font-semibold text-xl">{alert.aqi_value}</div>
                                                            </div>
                                                            <div>
                                                                <span className="text-gray-300 text-sm">Time</span>
                                                                <div className="text-white font-semibold text-sm">
                                                                    {formatTimestamp(alert.timestamp)}
                                                                </div>
                                                            </div>
                                                        </div>

                                                        <div className="bg-white/10 rounded-lg p-4 mb-4">
                                                            <h4 className="text-white font-semibold mb-2">🏥 EPA Health Guidance</h4>
                                                            <p className="text-gray-200 text-sm leading-relaxed">
                                                                {alert.epa_message}
                                                            </p>
                                                        </div>

                                                        <div className="text-gray-400 text-xs">
                                                            Expires: {formatTimestamp(alert.expires_at)} • Alert ID: {alert.alert_id.slice(0, 8)}...
                                                        </div>
                                                    </div>
                                                </div>

                                                <button
                                                    onClick={() => dismissAlert(alert.alert_id)}
                                                    className="ml-4 px-3 py-1 bg-gray-600/50 text-gray-300 hover:bg-gray-500/50 hover:text-white rounded-lg transition-colors text-sm"
                                                >
                                                    Dismiss
                                                </button>
                                            </div>
                                        </motion.div>
                                    ))}
                                </div>
                            )}
                        </motion.div>
                    )}

                    {}
                    {activeTab === 'preferences' && (
                        <motion.div
                            key="preferences"
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            exit={{ opacity: 0, y: -20 }}
                            className="space-y-6"
                        >
                            <NotificationPreferences
                                preferences={preferences}
                                onUpdate={updatePreferences}
                            />
                        </motion.div>
                    )}

                    {}
                    {activeTab === 'statistics' && (
                        <motion.div
                            key="statistics"
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            exit={{ opacity: 0, y: -20 }}
                            className="space-y-6"
                        >
                            <AlertStatistics statistics={statistics} />
                        </motion.div>
                    )}
                </AnimatePresence>
            </div>
        </div>
    )
}

const NotificationPreferences = ({ preferences, onUpdate }) => {
    const [localPrefs, setLocalPrefs] = useState(preferences)
    const [healthConditions, setHealthConditions] = useState(
        preferences.health_conditions || []
    )

    useEffect(() => {
        setLocalPrefs(preferences)
        setHealthConditions(preferences.health_conditions || [])
    }, [preferences])

    const handlePreferenceChange = (key, value) => {
        const updated = { ...localPrefs, [key]: value }
        setLocalPrefs(updated)
    }

    const handleHealthConditionToggle = (condition) => {
        const updated = healthConditions.some(c => c.condition === condition)
            ? healthConditions.filter(c => c.condition !== condition)
            : [...healthConditions, { condition, severity: 'moderate' }]

        setHealthConditions(updated)
    }

    const savePreferences = () => {
        const updated = {
            ...localPrefs,
            health_conditions: healthConditions
        }
        onUpdate(updated)
    }

    const availableConditions = [
        'asthma', 'heart_disease', 'lung_disease', 'elderly',
        'children', 'pregnant', 'outdoor_worker'
    ]

    return (
        <div className="space-y-6">
            {}
            <div className="bg-black/20 backdrop-blur-md rounded-xl p-6 border border-white/10">
                <h3 className="text-xl font-bold text-white mb-4">📬 Notification Channels</h3>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {[
                        { key: 'email', label: '📧 Email Notifications', description: 'Detailed alerts via email' },
                        { key: 'push', label: '📱 Push Notifications', description: 'Mobile app notifications' },
                        { key: 'web_push', label: '🌐 Browser Notifications', description: 'Browser push notifications' },
                        { key: 'sms', label: '📱 SMS Alerts', description: 'Text message alerts (emergency only)' }
                    ].map((channel) => (
                        <label key={channel.key} className="flex items-center space-x-3 p-3 rounded-lg hover:bg-white/5 cursor-pointer">
                            <input
                                type="checkbox"
                                checked={localPrefs[channel.key] || false}
                                onChange={(e) => handlePreferenceChange(channel.key, e.target.checked)}
                                className="w-4 h-4 text-blue-500 bg-gray-700 border-gray-600 rounded focus:ring-blue-500"
                            />
                            <div>
                                <div className="text-white font-medium">{channel.label}</div>
                                <div className="text-gray-400 text-sm">{channel.description}</div>
                            </div>
                        </label>
                    ))}
                </div>
            </div>

            {}
            <div className="bg-black/20 backdrop-blur-md rounded-xl p-6 border border-white/10">
                <h3 className="text-xl font-bold text-white mb-4">⚠️ Alert Types</h3>
                <div className="space-y-3">
                    {[
                        { key: 'daily_summary', label: '📅 Daily Summary', description: 'Daily air quality recap' },
                        { key: 'forecast_warnings', label: '🔮 Forecast Warnings', description: '24-hour advance warnings' }
                    ].map((type) => (
                        <label key={type.key} className="flex items-center space-x-3 p-3 rounded-lg hover:bg-white/5 cursor-pointer">
                            <input
                                type="checkbox"
                                checked={localPrefs[type.key] || false}
                                onChange={(e) => handlePreferenceChange(type.key, e.target.checked)}
                                className="w-4 h-4 text-blue-500 bg-gray-700 border-gray-600 rounded focus:ring-blue-500"
                            />
                            <div>
                                <div className="text-white font-medium">{type.label}</div>
                                <div className="text-gray-400 text-sm">{type.description}</div>
                            </div>
                        </label>
                    ))}
                </div>
            </div>

            {}
            <div className="bg-black/20 backdrop-blur-md rounded-xl p-6 border border-white/10">
                <h3 className="text-xl font-bold text-white mb-4">🏥 Health Conditions</h3>
                <p className="text-gray-300 text-sm mb-4">
                    Help us provide personalized health guidance by selecting any relevant conditions.
                </p>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    {availableConditions.map((condition) => (
                        <label key={condition} className="flex items-center space-x-3 p-3 rounded-lg hover:bg-white/5 cursor-pointer">
                            <input
                                type="checkbox"
                                checked={healthConditions.some(c => c.condition === condition)}
                                onChange={() => handleHealthConditionToggle(condition)}
                                className="w-4 h-4 text-blue-500 bg-gray-700 border-gray-600 rounded focus:ring-blue-500"
                            />
                            <div className="text-white font-medium">
                                {condition.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}
                            </div>
                        </label>
                    ))}
                </div>
            </div>

            {}
            <div className="bg-black/20 backdrop-blur-md rounded-xl p-6 border border-white/10">
                <h3 className="text-xl font-bold text-white mb-4">🌙 Quiet Hours</h3>
                <p className="text-gray-300 text-sm mb-4">
                    No notifications during these hours except for emergency alerts (hazardous AQI).
                </p>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                        <label className="block text-white font-medium mb-2">Start Time</label>
                        <input
                            type="time"
                            value={localPrefs.quiet_hours_start || '22:00'}
                            onChange={(e) => handlePreferenceChange('quiet_hours_start', e.target.value)}
                            className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                        />
                    </div>
                    <div>
                        <label className="block text-white font-medium mb-2">End Time</label>
                        <input
                            type="time"
                            value={localPrefs.quiet_hours_end || '07:00'}
                            onChange={(e) => handlePreferenceChange('quiet_hours_end', e.target.value)}
                            className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                        />
                    </div>
                </div>
            </div>

            {}
            <div className="flex justify-center">
                <button
                    onClick={savePreferences}
                    className="px-8 py-3 bg-gradient-to-r from-green-500 to-blue-600 text-white rounded-xl hover:from-green-600 hover:to-blue-700 transition-all font-semibold"
                >
                    Save Preferences
                </button>
            </div>
        </div>
    )
}

const AlertStatistics = ({ statistics }) => {
    const alertLevels = statistics.alert_levels || {}
    const notifications = statistics.notifications || {}
    const recentAlerts = statistics.recent_alerts || []

    return (
        <div className="space-y-6">
            {}
            <div className="bg-black/20 backdrop-blur-md rounded-xl p-6 border border-white/10">
                <h3 className="text-xl font-bold text-white mb-4">📊 Active Alerts by Level</h3>
                <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                    {Object.entries(alertLevels).map(([level, count]) => (
                        <div key={level} className="text-center p-4 bg-white/5 rounded-lg">
                            <div className="text-2xl font-bold text-white">{count}</div>
                            <div className="text-gray-300 text-sm capitalize">
                                {level.replace('_', ' ')}
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            {}
            <div className="bg-black/20 backdrop-blur-md rounded-xl p-6 border border-white/10">
                <h3 className="text-xl font-bold text-white mb-4">📈 Notification Delivery (24h)</h3>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    {Object.entries(notifications).map(([type, count]) => (
                        <div key={type} className="text-center p-4 bg-white/5 rounded-lg">
                            <div className="text-2xl font-bold text-white">{count}</div>
                            <div className="text-gray-300 text-sm capitalize">
                                {type.replace('_', ' ')}
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            {}
            <div className="bg-black/20 backdrop-blur-md rounded-xl p-6 border border-white/10">
                <h3 className="text-xl font-bold text-white mb-4">🕐 Recent Alert Activity</h3>
                {recentAlerts.length === 0 ? (
                    <p className="text-gray-400 text-center py-4">No recent activity</p>
                ) : (
                    <div className="space-y-3">
                        {recentAlerts.map((alert, index) => (
                            <div key={index} className="flex items-center justify-between p-3 bg-white/5 rounded-lg">
                                <div className="flex items-center space-x-3">
                                    <div className="text-xl">🌍</div>
                                    <div>
                                        <div className="text-white font-medium">
                                            {alert.location_city} - AQI {alert.aqi_value}
                                        </div>
                                        <div className="text-gray-400 text-sm">
                                            {alert.alert_level.replace('_', ' ')}
                                        </div>
                                    </div>
                                </div>
                                <div className="text-gray-400 text-sm">
                                    {new Date(alert.timestamp).toLocaleString()}
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </div>

            {}
            <div className="bg-black/20 backdrop-blur-md rounded-xl p-6 border border-white/10">
                <h3 className="text-xl font-bold text-white mb-4">⚙️ System Status</h3>
                <div className="space-y-3">
                    {statistics.system_status && Object.entries(statistics.system_status).map(([system, status]) => (
                        system !== 'last_update' && (
                            <div key={system} className="flex items-center justify-between p-3 bg-white/5 rounded-lg">
                                <div className="text-white capitalize">
                                    {system.replace('_', ' ')}
                                </div>
                                <div className={`px-3 py-1 rounded-full text-sm ${status === 'operational'
                                    ? 'bg-green-500/20 text-green-300'
                                    : 'bg-red-500/20 text-red-300'
                                    }`}>
                                    {status}
                                </div>
                            </div>
                        )
                    ))}
                </div>
            </div>
        </div>
    )
}

export default AlertsPage
