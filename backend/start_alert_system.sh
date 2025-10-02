#!/bin/bash
# start_alert_system.sh - Complete alert system startup script

echo "🛡️ Starting NAQForecast Alert System"
echo "📧 Gmail SMTP + Registration API"
echo "=" * 50

# Start alert registration API in background
echo "🔧 Starting Alert Registration API (Port 5003)..."
cd /app
python3 apis/alert_registration_api.py &
API_PID=$!
echo "✅ Alert Registration API started (PID: $API_PID)"

# Wait a moment for API to start
sleep 5

# Start continuous alert monitoring (foreground)
echo "📡 Starting Continuous Alert Monitor..."
python3 notifications/safer_skies_auto_alerts.py

# If alert monitor exits, cleanup
echo "🛑 Alert monitor stopped, cleaning up..."
kill $API_PID 2>/dev/null