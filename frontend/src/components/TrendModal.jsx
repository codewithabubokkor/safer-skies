import React, { useState } from 'react';
import { useTrendData } from '../hooks/useTrendData';
import TrendChart from './TrendChart';

/**
 * 📊 Trend Modal Component
 * Full-screen modal for detailed trend visualization
 */
const TrendModal = ({ locationId, locationName, onClose }) => {
    const [selectedDays, setSelectedDays] = useState(30);
    const { trendData, loading, error, refetch } = useTrendData(locationId, selectedDays);

    const getTrendColor = (direction) => {
        switch (direction) {
            case 'improving': return 'text-green-400';
            case 'worsening': return 'text-red-400';
            default: return 'text-yellow-400';
        }
    };

    const getTrendIcon = (direction) => {
        switch (direction) {
            case 'improving': return '📈';
            case 'worsening': return '📉';
            default: return '➡️';
        }
    };

    const formatTrendDirection = (direction) => {
        return direction.charAt(0).toUpperCase() + direction.slice(1);
    };

    const handleBackdropClick = (e) => {
        if (e.target === e.currentTarget) {
            onClose();
        }
    };

    React.useEffect(() => {
        const handleEscape = (e) => {
            if (e.key === 'Escape') {
                onClose();
            }
        };

        document.addEventListener('keydown', handleEscape);
        return () => document.removeEventListener('keydown', handleEscape);
    }, [onClose]);

    return (
        <div
            className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4"
            onClick={handleBackdropClick}
        >
            <div className="bg-white/10 backdrop-blur-lg rounded-xl max-w-5xl w-full max-h-[90vh] overflow-hidden border border-white/20 shadow-2xl">

                {}
                <div className="flex justify-between items-center p-6 border-b border-white/20">
                    <div>
                        <h2 className="text-2xl font-bold text-white flex items-center">
                            📊 {locationName}
                        </h2>
                        <p className="text-gray-300">Air Quality Trends - NASA Recommended Visualization</p>
                    </div>

                    <div className="flex items-center space-x-4">
                        {}
                        <div className="flex items-center space-x-2">
                            <span className="text-sm text-gray-300">Period:</span>
                            <select
                                value={selectedDays}
                                onChange={(e) => setSelectedDays(Number(e.target.value))}
                                className="bg-white/10 border border-white/30 rounded-lg px-3 py-1.5 text-white text-sm focus:outline-none focus:ring-2 focus:ring-blue-400 focus:border-transparent"
                            >
                                <option value={7} className="bg-gray-800">7 Days</option>
                                <option value={14} className="bg-gray-800">14 Days</option>
                                <option value={30} className="bg-gray-800">30 Days</option>
                            </select>
                        </div>

                        {}
                        <button
                            onClick={refetch}
                            className="p-2 hover:bg-white/10 rounded-lg transition-colors text-gray-300 hover:text-white"
                            title="Refresh data"
                        >
                            🔄
                        </button>

                        {}
                        <button
                            onClick={onClose}
                            className="text-gray-400 hover:text-white transition-colors text-xl p-1"
                            title="Close (Esc)"
                        >
                            ✕
                        </button>
                    </div>
                </div>

                {}
                <div className="p-6 overflow-y-auto" style={{ maxHeight: 'calc(90vh - 100px)' }}>

                    {loading && (
                        <div className="flex items-center justify-center py-12">
                            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-400"></div>
                            <span className="ml-2 text-white">Loading trend data...</span>
                        </div>
                    )}

                    {error && (
                        <div className="text-center py-12">
                            <div className="text-6xl mb-4">⚠️</div>
                            <p className="text-red-400 text-lg mb-2">Failed to load trend data</p>
                            <p className="text-gray-400 mb-4">{error}</p>
                            <button
                                onClick={refetch}
                                className="px-4 py-2 bg-blue-500/20 hover:bg-blue-500/30 border border-blue-400/30 rounded-lg text-blue-200 transition-colors"
                            >
                                Try Again
                            </button>
                        </div>
                    )}

                    {trendData && !loading && (
                        <div className="space-y-6">

                            {}
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                                <div className="bg-white/5 backdrop-blur-sm rounded-lg p-4 text-center border border-white/10">
                                    <div className="text-2xl font-bold text-white">
                                        {trendData.summary?.avg_aqi || 0}
                                    </div>
                                    <div className="text-sm text-gray-300">Average AQI</div>
                                </div>
                                <div className="bg-white/5 backdrop-blur-sm rounded-lg p-4 text-center border border-white/10">
                                    <div className="text-2xl font-bold text-white">
                                        {trendData.summary?.max_aqi || 0}
                                    </div>
                                    <div className="text-sm text-gray-300">Maximum AQI</div>
                                </div>
                                <div className="bg-white/5 backdrop-blur-sm rounded-lg p-4 text-center border border-white/10">
                                    <div className="text-2xl font-bold text-white">
                                        {trendData.summary?.total_days || 0}
                                    </div>
                                    <div className="text-sm text-gray-300">Days Analyzed</div>
                                </div>
                                <div className="bg-white/5 backdrop-blur-sm rounded-lg p-4 text-center border border-white/10">
                                    <div className={`text-2xl font-bold ${getTrendColor(trendData.summary?.trend_direction)}`}>
                                        {getTrendIcon(trendData.summary?.trend_direction)}
                                    </div>
                                    <div className="text-sm text-gray-300">
                                        {formatTrendDirection(trendData.summary?.trend_direction || 'stable')}
                                    </div>
                                </div>
                            </div>

                            {}
                            <div className="bg-white/5 backdrop-blur-sm rounded-lg p-6 border border-white/10">
                                <div className="flex items-center justify-between mb-4">
                                    <h3 className="text-lg font-semibold text-white">
                                        AQI Trend Over Time
                                    </h3>
                                    <div className="text-sm text-gray-400">
                                        EPA-compliant daily averages
                                    </div>
                                </div>
                                <TrendChart data={trendData} height={400} />
                            </div>

                            {}
                            {trendData.pollutant_trends && Object.keys(trendData.pollutant_trends).length > 0 && (
                                <div className="bg-white/5 backdrop-blur-sm rounded-lg p-6 border border-white/10">
                                    <h3 className="text-lg font-semibold text-white mb-4">
                                        Individual Pollutant Trends
                                    </h3>
                                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                        {Object.entries(trendData.pollutant_trends).map(([pollutant, data]) => {
                                            if (!data || data.length === 0) return null;

                                            const latest = data[data.length - 1];
                                            const previous = data[data.length - 2];
                                            const change = previous ? ((latest.value - previous.value) / previous.value * 100) : 0;

                                            return (
                                                <div key={pollutant} className="bg-white/5 rounded-lg p-4 border border-white/10">
                                                    <div className="flex items-center justify-between mb-2">
                                                        <h4 className="text-white font-medium">
                                                            {pollutant === 'PM25' ? 'PM₂.₅' :
                                                                pollutant === 'PM10' ? 'PM₁₀' :
                                                                    pollutant === 'O3' ? 'Ozone' :
                                                                        pollutant === 'NO2' ? 'NO₂' :
                                                                            pollutant === 'SO2' ? 'SO₂' : pollutant}
                                                        </h4>
                                                        <span className={`text-sm ${change > 0 ? 'text-red-400' : change < 0 ? 'text-green-400' : 'text-gray-400'}`}>
                                                            {change > 0 ? '↑' : change < 0 ? '↓' : '→'} {Math.abs(change).toFixed(1)}%
                                                        </span>
                                                    </div>
                                                    <div className="text-2xl font-bold text-white mb-1">
                                                        {latest?.value?.toFixed(1) || 'N/A'}
                                                    </div>
                                                    <div className="text-xs text-gray-400">
                                                        Method: {latest?.method?.replace('_', ' ') || 'N/A'}
                                                    </div>
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>
                            )}

                            {}
                            <div className="bg-blue-500/10 border border-blue-400/20 rounded-lg p-4">
                                <div className="flex items-start space-x-3">
                                    <div className="text-blue-400 text-xl">ℹ️</div>
                                    <div>
                                        <h4 className="text-blue-200 font-medium mb-1">EPA Compliance</h4>
                                        <p className="text-blue-100/80 text-sm">
                                            This visualization uses EPA-compliant time averaging methods:
                                            8-hour rolling averages for O₃ and CO, 24-hour averages for PM₂.₅ and PM₁₀,
                                            and 1-hour maximum values for NO₂ and SO₂, as recommended by NASA for air quality trend analysis.
                                        </p>
                                    </div>
                                </div>
                            </div>

                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};

export default TrendModal;