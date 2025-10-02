import React, { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'

const NotificationHistory = ({ userEmail, onClose }) => {
    const [history, setHistory] = useState([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState(null)
    const [totalNotifications, setTotalNotifications] = useState(0)

    // API base URL - should match your backend
    const API_BASE = 'http://localhost:5003'

    useEffect(() => {
        if (userEmail) {
            loadNotificationHistory()
        }
    }, [userEmail])

    const loadNotificationHistory = async () => {
        setLoading(true)
        setError(null)

        try {
            const response = await fetch(`${API_BASE}/api/alerts/history/${encodeURIComponent(userEmail)}`)
            const data = await response.json()

            if (data.success) {
                setHistory(data.recent_notifications || [])
                setTotalNotifications(data.total_notifications || 0)
            } else {
                setError(data.error || 'Failed to load notification history')
            }
        } catch (error) {
            console.error('Error loading notification history:', error)
            setError('Unable to connect to notification service')
        } finally {
            setLoading(false)
        }
    }

    const formatTimestamp = (timestamp) => {
        const date = new Date(timestamp)
        const now = new Date()
        const diffInHours = (now - date) / (1000 * 60 * 60)

        if (diffInHours < 1) {
            const diffInMinutes = Math.floor((now - date) / (1000 * 60))
            return `${diffInMinutes} minute${diffInMinutes !== 1 ? 's' : ''} ago`
        } else if (diffInHours < 24) {
            const hours = Math.floor(diffInHours)
            return `${hours} hour${hours !== 1 ? 's' : ''} ago`
        } else if (diffInHours < 168) { // Less than a week
            const days = Math.floor(diffInHours / 24)
            return `${days} day${days !== 1 ? 's' : ''} ago`
        } else {
            return date.toLocaleDateString()
        }
    }

    const getNotificationIcon = (type, status) => {
        if (status === 'failed') return '❌'

        switch (type) {
            case 'email': return '📧'
            case 'push': return '📱'
            case 'web_push': return '🌐'
            case 'sms': return '💬'
            default: return '🔔'
        }
    }

    const getStatusColor = (status) => {
        switch (status) {
            case 'sent': return 'text-green-400 bg-green-900/20'
            case 'failed': return 'text-red-400 bg-red-900/20'
            case 'pending': return 'text-yellow-400 bg-yellow-900/20'
            default: return 'text-gray-400 bg-gray-900/20'
        }
    }

    const getAQIColor = (aqiValue) => {
        if (aqiValue <= 50) return 'text-green-400'
        if (aqiValue <= 100) return 'text-yellow-400'
        if (aqiValue <= 150) return 'text-orange-400'
        if (aqiValue <= 200) return 'text-red-400'
        if (aqiValue <= 300) return 'text-purple-400'
        return 'text-red-600'
    }

    if (loading) {
        return (
            <div className="flex items-center justify-center py-12">
                <div className="text-center">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-400 mx-auto mb-4"></div>
                    <p className="text-gray-300">Loading notification history...</p>
                </div>
            </div>
        )
    }

    if (error) {
        return (
            <div className="text-center py-12">
                <div className="text-red-400 text-4xl mb-4">⚠️</div>
                <h3 className="text-xl font-bold text-white mb-2">Unable to Load History</h3>
                <p className="text-gray-300 mb-4">{error}</p>
                <button
                    onClick={loadNotificationHistory}
                    className="px-4 py-2 bg-blue-500 hover:bg-blue-600 text-white rounded-lg transition-colors"
                >
                    Try Again
                </button>
            </div>
        )
    }

    return (
        <div className="space-y-6">
            {}
            <div className="flex items-center justify-between">
                <div>
                    <h2 className="text-2xl font-bold text-white">📧 Notification History</h2>
                    <p className="text-gray-300">
                        {userEmail} • {totalNotifications} total notification{totalNotifications !== 1 ? 's' : ''}
                    </p>
                </div>
                <button
                    onClick={onClose}
                    className="text-gray-400 hover:text-white transition-colors text-2xl"
                >
                    ✕
                </button>
            </div>

            {}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="bg-black/20 backdrop-blur-md rounded-xl p-4 border border-white/10">
                    <div className="text-2xl font-bold text-white">{totalNotifications}</div>
                    <div className="text-gray-300 text-sm">Total Notifications</div>
                </div>
                <div className="bg-black/20 backdrop-blur-md rounded-xl p-4 border border-white/10">
                    <div className="text-2xl font-bold text-green-400">
                        {history.filter(n => n.status === 'sent').length}
                    </div>
                    <div className="text-gray-300 text-sm">Successfully Sent</div>
                </div>
                <div className="bg-black/20 backdrop-blur-md rounded-xl p-4 border border-white/10">
                    <div className="text-2xl font-bold text-red-400">
                        {history.filter(n => n.status === 'failed').length}
                    </div>
                    <div className="text-gray-300 text-sm">Failed</div>
                </div>
            </div>

            {}
            {history.length === 0 ? (
                <div className="bg-black/20 backdrop-blur-md rounded-xl p-8 border border-white/10 text-center">
                    <div className="text-6xl mb-4">📬</div>
                    <h3 className="text-xl font-bold text-white mb-2">No Notifications Yet</h3>
                    <p className="text-gray-300">
                        You haven't received any air quality alerts yet.
                        Notifications will appear here when air quality exceeds your thresholds.
                    </p>
                </div>
            ) : (
                <div className="space-y-3">
                    <h3 className="text-lg font-semibold text-white">Recent Notifications</h3>
                    {history.map((notification, index) => (
                        <motion.div
                            key={notification.notification_id || index}
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: index * 0.1 }}
                            className="bg-black/20 backdrop-blur-md rounded-xl p-4 border border-white/10"
                        >
                            <div className="flex items-start justify-between">
                                <div className="flex items-start space-x-3 flex-1">
                                    <div className="text-2xl">
                                        {getNotificationIcon(notification.notification_type, notification.status)}
                                    </div>
                                    <div className="flex-1">
                                        <div className="flex items-center space-x-2 mb-2">
                                            <h4 className="text-white font-semibold">
                                                {notification.location_name || 'Unknown Location'}
                                            </h4>
                                            <span className={`px-2 py-1 rounded-full text-xs ${getStatusColor(notification.status)}`}>
                                                {notification.status}
                                            </span>
                                        </div>

                                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-3">
                                            <div>
                                                <span className="text-gray-400 text-sm">Type</span>
                                                <div className="text-white">
                                                    {notification.notification_type.replace('_', ' ')}
                                                </div>
                                            </div>
                                            <div>
                                                <span className="text-gray-400 text-sm">Alert Threshold</span>
                                                <div className={`font-semibold ${getAQIColor(notification.alert_threshold)}`}>
                                                    AQI {notification.alert_threshold}
                                                </div>
                                            </div>
                                            <div>
                                                <span className="text-gray-400 text-sm">Alert Level</span>
                                                <div className="text-white capitalize">
                                                    {notification.alert_level?.replace('_', ' ') || 'N/A'}
                                                </div>
                                            </div>
                                        </div>

                                        {notification.alert_message && (
                                            <div className="bg-white/10 rounded-lg p-3 mb-3">
                                                <p className="text-gray-200 text-sm">
                                                    {notification.alert_message}
                                                </p>
                                            </div>
                                        )}

                                        {notification.error_message && (
                                            <div className="bg-red-900/20 border border-red-500/20 rounded-lg p-3 mb-3">
                                                <div className="flex items-center space-x-2">
                                                    <span className="text-red-400">⚠️</span>
                                                    <span className="text-red-300 text-sm font-medium">Error:</span>
                                                </div>
                                                <p className="text-red-200 text-sm mt-1">
                                                    {notification.error_message}
                                                </p>
                                            </div>
                                        )}

                                        <div className="flex items-center justify-between text-gray-400 text-sm">
                                            <div className="flex items-center space-x-4">
                                                {notification.latitude && notification.longitude && (
                                                    <span>
                                                        📍 {parseFloat(notification.latitude).toFixed(4)}, {parseFloat(notification.longitude).toFixed(4)}
                                                    </span>
                                                )}
                                            </div>
                                            <div>
                                                🕒 {formatTimestamp(notification.sent_at)}
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </motion.div>
                    ))}
                </div>
            )}

            {}
            <div className="flex justify-center pt-4">
                <button
                    onClick={loadNotificationHistory}
                    className="px-6 py-2 bg-blue-500 hover:bg-blue-600 text-white rounded-lg transition-colors flex items-center space-x-2"
                >
                    <span>🔄</span>
                    <span>Refresh History</span>
                </button>
            </div>
        </div>
    )
}

export default NotificationHistory