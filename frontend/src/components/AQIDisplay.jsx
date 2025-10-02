import React from 'react';

/**
 * AQI Display Component - Shows real-time AQI data from NAQForecast pipeline
 */
const AQIDisplay = ({ aqiData, loading, error, onRefresh, className = '' }) => {
    if (loading) {
        return (
            <div className={`glass-panel p-4 ${className}`}>
                <div className="flex items-center justify-center">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-white"></div>
                    <span className="ml-3 text-white">Loading AQI data...</span>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className={`glass-panel p-4 ${className}`}>
                <div className="text-red-400 text-center">
                    <div className="text-lg mb-2">⚠️ Error Loading AQI</div>
                    <div className="text-sm text-gray-300 mb-3">{error}</div>
                    {onRefresh && (
                        <button
                            onClick={onRefresh}
                            className="px-4 py-2 bg-red-500/20 hover:bg-red-500/30 rounded-lg text-white transition-colors"
                        >
                            Try Again
                        </button>
                    )}
                </div>
            </div>
        );
    }

    if (!aqiData) {
        return (
            <div className={`glass-panel p-4 ${className}`}>
                <div className="text-center text-gray-400">
                    <div className="text-lg mb-2">📍 Select a location</div>
                    <div className="text-sm">Choose a location to see real-time air quality data</div>
                </div>
            </div>
        );
    }

    const getAQIColor = (aqi) => {
        if (aqi <= 50) return '#00E400'; // Green
        if (aqi <= 100) return '#FFFF00'; // Yellow
        if (aqi <= 150) return '#FF7E00'; // Orange
        if (aqi <= 200) return '#FF0000'; // Red
        if (aqi <= 300) return '#8F3F97'; // Purple
        return '#7E0023'; // Maroon
    };

    const getAQIGradient = (aqi) => {
        const color = getAQIColor(aqi);
        return `linear-gradient(135deg, ${color}22, ${color}44)`;
    };

    const formatTimestamp = (timestamp) => {
        const date = new Date(timestamp);
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    };

    const getPollutantName = (code) => {
        const names = {
            'PM25': 'PM₂.₅',
            'PM10': 'PM₁₀',
            'O3': 'Ozone',
            'NO2': 'NO₂',
            'SO2': 'SO₂',
            'CO': 'CO'
        };
        return names[code] || code;
    };

    return (
        <div className={`glass-panel p-4 ${className}`} style={{ pointerEvents: 'auto' }}>
            {}
            <div className="flex items-center justify-between mb-4">
                <div>
                    <h3 className="text-white text-lg font-bold">🌬️ Air Quality</h3>
                    <div className="text-gray-300 text-sm flex items-center">
                        <span>{aqiData.location.city}</span>
                        {aqiData.data_sources?.includes('NAQForecast Pipeline') && (
                            <span className="ml-2 px-2 py-0.5 bg-blue-500/20 text-blue-300 rounded text-xs">
                                Live Data
                            </span>
                        )}
                    </div>
                </div>
                {onRefresh && (
                    <button
                        onClick={onRefresh}
                        className="p-2 glass-button hover:bg-white/10 rounded-lg transition-colors"
                        title="Refresh AQI data"
                    >
                        🔄
                    </button>
                )}
            </div>

            {}
            <div
                className="rounded-xl p-4 mb-4 border"
                style={{
                    background: getAQIGradient(aqiData.current_aqi),
                    borderColor: getAQIColor(aqiData.current_aqi) + '66'
                }}
            >
                <div className="text-center">
                    <div className="text-white text-3xl font-bold mb-1">
                        {aqiData.current_aqi}
                    </div>
                    <div className="text-white/90 text-sm font-medium mb-2">
                        {aqiData.aqi_category}
                    </div>
                    {aqiData.dominant_pollutant && (
                        <div className="text-white/80 text-xs">
                            Primary: {getPollutantName(aqiData.dominant_pollutant)}
                        </div>
                    )}
                </div>
            </div>

            {}
            {aqiData.epa_message && (
                <div className="bg-gray-800/50 rounded-lg p-3 mb-4">
                    <div className="text-gray-300 text-sm">
                        <div className="font-medium mb-1">EPA Advisory:</div>
                        <div>{aqiData.epa_message}</div>
                    </div>
                </div>
            )}

            {}
            <div className="space-y-2 mb-4">
                <div className="text-white text-sm font-medium mb-2">Pollutant Levels</div>
                {Object.entries(aqiData.pollutants || {}).map(([pollutant, value]) => (
                    <div key={pollutant} className="flex items-center justify-between">
                        <span className="text-gray-300 text-sm">
                            {getPollutantName(pollutant.toUpperCase())}
                        </span>
                        <div className="flex items-center">
                            <div
                                className="w-2 h-2 rounded-full mr-2"
                                style={{ backgroundColor: getAQIColor(value) }}
                            ></div>
                            <span className="text-white text-sm font-medium w-8 text-right">
                                {value}
                            </span>
                        </div>
                    </div>
                ))}
            </div>

            {}
            {aqiData.health_recommendations && aqiData.health_recommendations.length > 0 && (
                <div className="bg-blue-900/20 rounded-lg p-3 mb-3">
                    <div className="text-blue-300 text-sm font-medium mb-2">💡 Health Tips</div>
                    <div className="space-y-1">
                        {aqiData.health_recommendations.slice(0, 2).map((tip, index) => (
                            <div key={index} className="text-gray-300 text-xs">
                                • {tip}
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {}
            <div className="flex items-center justify-between text-xs text-gray-400 border-t border-gray-700 pt-3">
                <div className="flex items-center space-x-3">
                    <span>Updated: {formatTimestamp(aqiData.last_updated)}</span>
                    {aqiData.confidence && (
                        <span>
                            Confidence: {Math.round(aqiData.confidence * 100)}%
                        </span>
                    )}
                </div>
                {aqiData.data_sources && (
                    <div className="text-right">
                        <div className="text-xs">
                            {aqiData.data_sources.slice(0, 2).join(', ')}
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
};

export default AQIDisplay;
