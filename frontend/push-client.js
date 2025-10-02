/**
 * Web Push Notification Client
 * Handles push notification subscription and management for NAQ Forecast frontend
 */

class PushNotificationManager {
    constructor(options = {}) {
        this.serverUrl = options.serverUrl || 'http://localhost:5010';
        this.swPath = options.swPath || '/sw.js';
        this.registration = null;
        this.subscription = null;
        this.userId = options.userId || 'demo-user';

        this.init();
    }

    async init() {
        try {
            if (!('serviceWorker' in navigator)) {
                console.warn('⚠️ Service Workers not supported');
                return false;
            }

            if (!('PushManager' in window)) {
                console.warn('⚠️ Push notifications not supported');
                return false;
            }

            // Register service worker
            await this.registerServiceWorker();

            await this.setupPushNotifications();

            console.log('✅ Push notification system initialized');
            return true;

        } catch (error) {
            console.error('❌ Error initializing push notifications:', error);
            return false;
        }
    }

    async registerServiceWorker() {
        try {
            this.registration = await navigator.serviceWorker.register(this.swPath);

            console.log('🔧 Service Worker registered:', this.registration);

            await navigator.serviceWorker.ready;

            return this.registration;

        } catch (error) {
            console.error('❌ Service Worker registration failed:', error);
            throw error;
        }
    }

    async setupPushNotifications() {
        try {
            let permission = Notification.permission;

            if (permission === 'default') {
                permission = await Notification.requestPermission();
            }

            if (permission !== 'granted') {
                console.warn('⚠️ Push notification permission denied');
                return false;
            }

            console.log('✅ Push notification permission granted');

            await this.subscribeToPush();

            return true;

        } catch (error) {
            console.error('❌ Error setting up push notifications:', error);
            return false;
        }
    }

    async subscribeToPush() {
        try {
            this.subscription = await this.registration.pushManager.subscribe({
                userVisibleOnly: true,
                applicationServerKey: this.urlBase64ToUint8Array(
                    'BEl62iUYgUivxIkv69yViEuiBIa40HI80YaEq3j4KHOqKcFuOyLUVrXKzMQk4E6vFMMq_7KnYjfRzPsYZlHtbwj' // Demo VAPID key
                )
            });

            console.log('🔔 Push subscription created:', this.subscription);

            await this.sendSubscriptionToServer();

            return this.subscription;

        } catch (error) {
            console.error('❌ Error subscribing to push:', error);
            throw error;
        }
    }

    async sendSubscriptionToServer() {
        try {
            const response = await fetch(`${this.serverUrl}/api/push/subscribe`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    user_id: this.userId,
                    subscription: {
                        endpoint: this.subscription.endpoint,
                        keys: {
                            p256dh: this.arrayBufferToBase64(this.subscription.getKey('p256dh')),
                            auth: this.arrayBufferToBase64(this.subscription.getKey('auth'))
                        }
                    }
                })
            });

            const result = await response.json();

            if (result.success) {
                console.log('✅ Subscription sent to server successfully');
                return true;
            } else {
                console.error('❌ Failed to send subscription to server:', result.error);
                return false;
            }

        } catch (error) {
            console.error('❌ Error sending subscription to server:', error);
            return false;
        }
    }

    async sendTestNotification() {
        try {
            const response = await fetch(`${this.serverUrl}/api/push/test/${this.userId}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            const result = await response.json();

            if (result.success) {
                console.log('✅ Test notification sent successfully');
                return true;
            } else {
                console.error('❌ Failed to send test notification:', result.error);
                return false;
            }

        } catch (error) {
            console.error('❌ Error sending test notification:', error);
            return false;
        }
    }

    async sendAirQualityAlert(alertData) {
        try {
            const notification = {
                title: `🌫️ Air Quality Alert - ${alertData.location.city}`,
                body: `AQI: ${alertData.aqi} (${alertData.level}). ${alertData.message}`,
                icon: '/icon-192x192.png',
                badge: '/badge-72x72.png',
                data: {
                    alert_id: alertData.alert_id,
                    location: alertData.location,
                    aqi: alertData.aqi,
                    level: alertData.level,
                    pollutants: alertData.pollutants,
                    timestamp: new Date().toISOString(),
                    url: '/#alerts'
                },
                urgent: alertData.level === 'hazardous' || alertData.level === 'very_unhealthy'
            };

            const response = await fetch(`${this.serverUrl}/api/push/send`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    user_id: this.userId,
                    notification: notification
                })
            });

            const result = await response.json();

            if (result.success) {
                console.log('✅ Air quality alert sent successfully');
                return true;
            } else {
                console.error('❌ Failed to send air quality alert:', result.error);
                return false;
            }

        } catch (error) {
            console.error('❌ Error sending air quality alert:', error);
            return false;
        }
    }

    async getPendingNotifications() {
        try {
            const response = await fetch(`${this.serverUrl}/api/push/pending/${this.userId}`);
            const result = await response.json();

            if (result.success) {
                console.log(`📬 ${result.pending_count} pending notifications found`);
                return result.notifications;
            } else {
                console.error('❌ Failed to get pending notifications:', result.error);
                return [];
            }

        } catch (error) {
            console.error('❌ Error getting pending notifications:', error);
            return [];
        }
    }

    async getSystemStatus() {
        try {
            const response = await fetch(`${this.serverUrl}/api/push/status`);
            const result = await response.json();

            return result;

        } catch (error) {
            console.error('❌ Error getting system status:', error);
            return { success: false, error: error.message };
        }
    }

    // Utility functions
    urlBase64ToUint8Array(base64String) {
        const padding = '='.repeat((4 - base64String.length % 4) % 4);
        const base64 = (base64String + padding)
            .replace(/-/g, '+')
            .replace(/_/g, '/');

        const rawData = window.atob(base64);
        const outputArray = new Uint8Array(rawData.length);

        for (let i = 0; i < rawData.length; ++i) {
            outputArray[i] = rawData.charCodeAt(i);
        }
        return outputArray;
    }

    arrayBufferToBase64(buffer) {
        const bytes = new Uint8Array(buffer);
        let binary = '';
        for (let i = 0; i < bytes.byteLength; i++) {
            binary += String.fromCharCode(bytes[i]);
        }
        return window.btoa(binary);
    }
}

let pushManager = null;

document.addEventListener('DOMContentLoaded', async () => {
    // Get user ID from registration or use demo
    const userId = localStorage.getItem('naq_user_id') || 'demo-user';

    pushManager = new PushNotificationManager({
        userId: userId,
        serverUrl: 'http://localhost:5001'
    });

    window.pushManager = pushManager;

    addPushNotificationControls();
});

function addPushNotificationControls() {
    const testButton = document.createElement('button');
    testButton.innerText = '🧪 Test Push Notification';
    testButton.style.cssText = `
        position: fixed;
        bottom: 20px;
        right: 20px;
        z-index: 10000;
        background: #007bff;
        color: white;
        border: none;
        padding: 10px 15px;
        border-radius: 5px;
        cursor: pointer;
        font-size: 14px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.2);
    `;

    testButton.onclick = async () => {
        if (pushManager) {
            const success = await pushManager.sendTestNotification();
            if (success) {
                alert('✅ Test notification sent! Check your notifications.');
            } else {
                alert('❌ Failed to send test notification. Check console for details.');
            }
        }
    };

    document.body.appendChild(testButton);

    console.log('🔔 Push notification test button added to page');
}

if (typeof module !== 'undefined' && module.exports) {
    module.exports = PushNotificationManager;
}

console.log('🚀 Push Notification Client loaded and ready!');