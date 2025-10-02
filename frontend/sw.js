/**
 * Service Worker for Web Push Notifications
 * Handles background push notifications for NAQ Forecast
 */

// Cache version
const CACHE_VERSION = 'naq-sw-v1';
const NOTIFICATION_CACHE = 'naq-notifications';

// Install service worker
self.addEventListener('install', event => {
    console.log('🔧 Service Worker installing...');

    event.waitUntil(
        caches.open(CACHE_VERSION)
            .then(cache => {
                console.log('✅ Service Worker cache opened');
                return cache.addAll([
                    '/',
                    '/icon-192x192.png',
                    '/badge-72x72.png'
                ]);
            })
            .catch(err => console.log('⚠️ Cache setup error:', err))
    );

    self.skipWaiting();
});

self.addEventListener('activate', event => {
    console.log('🚀 Service Worker activating...');

    event.waitUntil(
        caches.keys().then(cacheNames => {
            return Promise.all(
                cacheNames.map(cacheName => {
                    if (cacheName !== CACHE_VERSION) {
                        console.log('🗑️ Deleting old cache:', cacheName);
                        return caches.delete(cacheName);
                    }
                })
            );
        })
    );

    self.clients.claim();
});

self.addEventListener('push', event => {
    console.log('🔔 Push notification received:', event);

    let notificationData = {
        title: '🌫️ Air Quality Alert',
        body: 'Air quality has changed in your area',
        icon: '/icon-192x192.png',
        badge: '/badge-72x72.png',
        data: {
            timestamp: new Date().toISOString()
        }
    };

    if (event.data) {
        try {
            const pushData = event.data.json();
            notificationData = {
                ...notificationData,
                ...pushData
            };
        } catch (e) {
            console.log('⚠️ Error parsing push data:', e);
            notificationData.body = event.data.text() || notificationData.body;
        }
    }

    const options = {
        body: notificationData.body,
        icon: notificationData.icon || '/icon-192x192.png',
        badge: notificationData.badge || '/badge-72x72.png',
        image: notificationData.image,
        data: notificationData.data || {},
        actions: [
            {
                action: 'view',
                title: 'View Details',
                icon: '/view-icon.png'
            },
            {
                action: 'dismiss',
                title: 'Dismiss'
            }
        ],
        requireInteraction: notificationData.data?.urgent || false,
        silent: false,
        tag: `aqi-alert-${notificationData.data?.alert_id || 'general'}`,
        renotify: true,
        vibrate: notificationData.data?.urgent ? [200, 100, 200] : [100],
        timestamp: Date.now()
    };

    const alertLevel = notificationData.data?.alert_level;
    if (alertLevel) {
        switch (alertLevel) {
            case 'good':
                options.badge = '/badge-good.png';
                break;
            case 'moderate':
                options.badge = '/badge-moderate.png';
                break;
            case 'unhealthy_sensitive':
                options.badge = '/badge-unhealthy-sensitive.png';
                break;
            case 'unhealthy':
                options.badge = '/badge-unhealthy.png';
                options.requireInteraction = true;
                break;
            case 'very_unhealthy':
            case 'hazardous':
                options.badge = '/badge-hazardous.png';
                options.requireInteraction = true;
                options.vibrate = [200, 100, 200, 100, 200];
                break;
        }
    }

    event.waitUntil(
        self.registration.showNotification(notificationData.title, options)
            .then(() => {
                console.log('✅ Notification shown successfully');

                return caches.open(NOTIFICATION_CACHE)
                    .then(cache => {
                        const notificationRecord = {
                            id: Date.now(),
                            title: notificationData.title,
                            body: notificationData.body,
                            data: notificationData.data,
                            timestamp: new Date().toISOString()
                        };

                        return cache.put(
                            `/notifications/${notificationRecord.id}`,
                            new Response(JSON.stringify(notificationRecord))
                        );
                    });
            })
            .catch(err => console.error('❌ Error showing notification:', err))
    );
});

self.addEventListener('notificationclick', event => {
    console.log('🖱️ Notification clicked:', event);

    event.notification.close();

    const action = event.action;
    const data = event.notification.data || {};

    if (action === 'view') {
        const url = data.url || '/';

        event.waitUntil(
            clients.matchAll({ type: 'window', includeUncontrolled: true })
                .then(clients => {
                    for (const client of clients) {
                        if (client.url.includes(self.location.origin) && 'focus' in client) {
                            client.postMessage({
                                type: 'NOTIFICATION_CLICKED',
                                data: data
                            });
                            return client.focus();
                        }
                    }

                    if (clients.openWindow) {
                        return clients.openWindow(url);
                    }
                })
        );
    } else if (action === 'dismiss') {
        console.log('📝 Notification dismissed');
    } else {
        event.waitUntil(
            clients.matchAll({ type: 'window' })
                .then(clients => {
                    if (clients.length > 0) {
                        clients[0].postMessage({
                            type: 'NOTIFICATION_CLICKED',
                            data: data
                        });
                        return clients[0].focus();
                    } else if (clients.openWindow) {
                        return clients.openWindow('/');
                    }
                })
        );
    }
});

// Handle background sync for offline notifications
self.addEventListener('sync', event => {
    console.log('🔄 Background sync triggered:', event.tag);

    if (event.tag === 'sync-notifications') {
        event.waitUntil(
            syncPendingNotifications()
        );
    }
});

async function syncPendingNotifications() {
    try {
        const response = await fetch('/api/push/pending/current-user');

        if (response.ok) {
            const data = await response.json();

            if (data.success && data.notifications) {
                for (const notification of data.notifications) {
                    await self.registration.showNotification(
                        notification.title,
                        notification
                    );
                }
            }
        }
    } catch (error) {
        console.error('❌ Error syncing notifications:', error);
    }
}

self.addEventListener('message', event => {
    console.log('💬 Message received:', event.data);

    if (event.data && event.data.type === 'SKIP_WAITING') {
        self.skipWaiting();
    }

    if (event.data && event.data.type === 'TEST_NOTIFICATION') {
        const testOptions = {
            body: '🧪 This is a test push notification!',
            icon: '/icon-192x192.png',
            badge: '/badge-72x72.png',
            tag: 'test-notification',
            data: {
                test: true,
                timestamp: new Date().toISOString()
            }
        };

        self.registration.showNotification('🧪 Test Notification', testOptions);
    }
});

console.log('🎯 NAQ Forecast Service Worker loaded and ready for push notifications!');