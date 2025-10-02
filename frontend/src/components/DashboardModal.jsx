import React, { useState, useEffect } from 'react';
import { X, TrendingUp, MapPin, Clock, AlertCircle, Search, Navigation, BarChart3 } from 'lucide-react';
import { useTrendData } from '../hooks/useTrendData';
import { useCurrentAQI } from '../hooks/useCurrentAQI';
import TrendChart from './TrendChart';
import { getAQIColor, getAQIStatus } from '../utils/aqiUtils';
import trendService from '../services/trendService';

const DashboardModal = ({ isOpen, onClose, currentLocation }) => {
    const [availableLocations, setAvailableLocations] = useState([]);
    const [selectedLocationId, setSelectedLocationId] = useState('40.7128_-74.006'); // Default to New York
    const [selectedLocation, setSelectedLocation] = useState(null);
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedDays, setSelectedDays] = useState(30); // Default to 30 days

    const { trendData, isLoading, error } = useTrendData(selectedLocationId, selectedDays);
    const { currentData, isLoading: currentLoading } = useCurrentAQI(selectedLocation);

    useEffect(() => {
        const loadLocations = async () => {
            try {
                const locations = await trendService.getAvailableLocations();
                setAvailableLocations(locations || []);

                if (currentLocation) {
                    const locationId = `${currentLocation.lat.toFixed(3)}_${currentLocation.lon.toFixed(3)}`;
                    const exactMatch = locations.find(loc => loc.id === locationId);
                    if (exactMatch) {
                        setSelectedLocationId(locationId);
                        setSelectedLocation(exactMatch);
                    } else {
                        const nyLocation = locations.find(loc => loc.id === '40.7128_-74.006');
                        if (nyLocation) {
                            setSelectedLocation(nyLocation);
                        }
                    }
                } else {
                    const nyLocation = locations.find(loc => loc.id === '40.7128_-74.006');
                    if (nyLocation) {
                        setSelectedLocation(nyLocation);
                    }
                }
            } catch (err) {
                console.error('Failed to load available locations:', err);
            }
        };

        if (isOpen) {
            loadLocations();
        }
    }, [isOpen, currentLocation]);

    const handleLocationSelect = (location) => {
        setSelectedLocationId(location.id);
        setSelectedLocation(location);
        setSearchTerm('');
    };

    const handleAutoDetectLocation = () => {
        if (navigator.geolocation) {
            navigator.geolocation.getCurrentPosition(
                (position) => {
                    const { latitude, longitude } = position.coords;
                    const closest = availableLocations.find(loc =>
                        Math.abs((loc.lat || 0) - latitude) < 0.1 && Math.abs((loc.lon || 0) - longitude) < 0.1
                    );

                    if (closest) {
                        handleLocationSelect(closest);
                    } else {
                        const locationId = `${latitude.toFixed(3)}_${longitude.toFixed(3)}`;
                        setSelectedLocationId(locationId);
                        setSelectedLocation({
                            id: locationId,
                            name: 'Current Location',
                            lat: latitude,
                            lon: longitude
                        });
                    }
                },
                (error) => {
                    console.error('Geolocation error:', error);
                    alert('Unable to detect your location. Please select a location manually.');
                }
            );
        } else {
            alert('Geolocation is not supported by this browser.');
        }
    };

    const filteredLocations = availableLocations.filter(location =>
        (location.name || location.city || '').toLowerCase().includes(searchTerm.toLowerCase())
    );

    if (!isOpen) return null;

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-2">
            <div className="full-liquid-glass max-w-4xl w-full mx-4 max-h-[90vh] flex flex-col">
                <div className="p-3 sm:p-4 flex-shrink-0">
                    {}
                    <div className="flex items-center justify-between">
                        <div className="flex items-center space-x-3">
                            <TrendingUp className="h-6 w-6 text-cyan-400" />
                            <h2 className="text-xl font-bold text-white">
                                Air Quality Trends Dashboard
                            </h2>
                        </div>
                        <button
                            onClick={onClose}
                            className="glass-button p-2 hover:bg-white/20"
                        >
                            <X className="h-5 w-5 text-white" />
                        </button>
                    </div>
                </div>

                <div className="overflow-y-auto flex-1 px-3 sm:px-4 pb-3 sm:pb-4 space-y-6">

                    {}
                    <div className="space-y-4">
                        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                            <h3 className="text-lg lg:text-xl font-semibold text-white flex items-center space-x-2">
                                <Search className="h-5 w-5 lg:h-6 lg:w-6 text-cyan-400" />
                                <span>Location Search</span>
                            </h3>
                            <button
                                onClick={handleAutoDetectLocation}
                                className="px-4 py-2 lg:px-6 lg:py-3 text-sm lg:text-base bg-white/10 hover:bg-white/20 rounded-lg flex items-center space-x-2 transition-all border border-white/20"
                            >
                                <Navigation className="h-4 w-4 lg:h-5 lg:w-5" />
                                <span>Auto-Detect Location</span>
                            </button>
                        </div>

                        <div className="relative">
                            <input
                                type="text"
                                placeholder="Search locations..."
                                value={searchTerm}
                                onChange={(e) => setSearchTerm(e.target.value)}
                                className="w-full px-4 py-3 lg:px-6 lg:py-4 bg-white/10 border border-white/20 rounded-lg lg:rounded-xl text-white placeholder-gray-400 focus:outline-none focus:border-cyan-400 focus:ring-2 focus:ring-cyan-400/20 transition-all text-base lg:text-lg"
                            />
                            <Search className="absolute right-4 lg:right-6 top-3.5 lg:top-4.5 h-5 w-5 lg:h-6 lg:w-6 text-gray-400" />
                        </div>

                        {}
                        {searchTerm && (
                            <div className="max-h-40 lg:max-h-48 overflow-y-auto space-y-2">
                                {filteredLocations.map((location) => (
                                    <button
                                        key={location.uniqueKey || `${location.id}_${location.lat}_${location.lon}`}
                                        onClick={() => handleLocationSelect(location)}
                                        className="w-full text-left px-4 py-3 lg:px-6 lg:py-4 rounded-lg lg:rounded-xl bg-white/5 hover:bg-white/10 text-white text-sm lg:text-base transition-all"
                                    >
                                        <div className="flex items-center justify-between">
                                            <span>{location.name || location.city}</span>
                                            <span className="text-xs text-gray-400">
                                                {location.lat?.toFixed(3) || '?'}, {location.lon?.toFixed(3) || '?'}
                                            </span>
                                        </div>
                                    </button>
                                ))}
                            </div>
                        )}
                    </div>

                    {}
                    {selectedLocation && (
                        <div className="border-b border-white/10 pb-4">
                            <div className="flex items-center justify-between">
                                <div className="flex items-center space-x-3 lg:space-x-4">
                                    <MapPin className="h-5 w-5 lg:h-6 lg:w-6 text-cyan-400" />
                                    <div>
                                        <h3 className="font-semibold text-white text-lg lg:text-xl">
                                            {selectedLocation.name || selectedLocation.city}
                                        </h3>
                                        <p className="text-gray-300 text-sm lg:text-base">
                                            {selectedLocation.lat?.toFixed(3) || '?'}, {selectedLocation.lon?.toFixed(3) || '?'}
                                        </p>
                                    </div>
                                </div>
                                {currentData && (
                                    <div className="text-right">
                                        <div className={`text-3xl lg:text-4xl font-bold ${getAQIColor(currentData.aqi)}`}>
                                            {currentData.aqi}
                                        </div>
                                        <div className="text-sm lg:text-base text-gray-300">
                                            {getAQIStatus(currentData.aqi)}
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    )}

                    {}
                    {selectedLocation && (
                        <div className="border-b border-white/10 pb-4">
                            <div className="flex items-center justify-between mb-4">
                                <h3 className="text-lg font-semibold text-white flex items-center space-x-2">
                                    <Clock className="h-5 w-5 text-cyan-400" />
                                    <span>Time Range</span>
                                </h3>
                            </div>
                            <div className="flex space-x-3">
                                {[7, 30].map((days) => (
                                    <button
                                        key={days}
                                        onClick={() => setSelectedDays(days)}
                                        className={`px-4 py-2 rounded-lg text-sm font-medium transition-all border ${selectedDays === days
                                            ? 'bg-cyan-500 text-white shadow-lg border-cyan-400'
                                            : 'bg-white/10 text-gray-300 hover:bg-white/20 border-white/20'
                                            }`}
                                    >
                                        {days} Days
                                    </button>
                                ))}
                            </div>
                        </div>
                    )}

                    {}
                    {isLoading && (
                        <div className="text-center py-8">
                            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-cyan-400 mx-auto mb-4"></div>
                            <p className="text-white">Loading trend data for {selectedLocation?.name}...</p>
                        </div>
                    )}

                    {}
                    {error && (
                        <div className="border border-red-500/30 p-4 rounded-lg">
                            <div className="flex items-center space-x-3 text-red-400">
                                <AlertCircle className="h-5 w-5" />
                                <div>
                                    <h3 className="font-semibold">Unable to Load Trends</h3>
                                    <p className="text-sm opacity-80">{error}</p>
                                </div>
                            </div>
                        </div>
                    )}

                    {}
                    {!isLoading && !error && trendData && (
                        <div className="border-b border-white/10 pb-6">
                            <div className="flex flex-col sm:flex-row sm:items-center justify-between mb-4 gap-3">
                                <div>
                                    <h3 className="text-lg lg:text-2xl font-semibold text-white">
                                        {selectedDays}-Day AQI Trends
                                    </h3>
                                    <p className="text-sm lg:text-base text-gray-300">
                                        {selectedLocation?.name} - Historical Air Quality Analysis
                                    </p>
                                </div>
                                <div className="flex items-center space-x-2 text-sm lg:text-base text-gray-400">
                                    <Clock className="h-4 w-4 lg:h-5 lg:w-5" />
                                    <span>Last {selectedDays} Days</span>
                                </div>
                            </div>
                            <div className="h-64 sm:h-80 lg:h-96 xl:h-[28rem]">
                                <TrendChart
                                    data={trendData}
                                    city={selectedLocation?.name}
                                />
                            </div>
                        </div>
                    )}

                    {}
                    {!isLoading && !error && !trendData && (
                        <div className="text-center py-8 border-b border-white/10 pb-6">
                            <div className="space-y-4">
                                <div className="flex justify-center">
                                    <BarChart3 className="h-12 w-12 text-gray-400" />
                                </div>
                                <div>
                                    <h3 className="text-lg font-semibold text-white mb-2">
                                        Trend Data Not Available
                                    </h3>
                                    <p className="text-gray-300 max-w-md mx-auto mb-4">
                                        We collect trend data for locations where users have set up alerts or frequently search.
                                        <span className="font-semibold text-cyan-400"> {selectedLocation?.name}</span> doesn't have enough data points yet.
                                    </p>
                                    <div className="space-y-3">
                                        <div className="glass-button p-3 bg-blue-500/20 border border-blue-500/30 rounded-lg">
                                            <p className="text-blue-200 text-sm">
                                                📊 <span className="font-semibold">Current AQI & Forecast</span> - Available on homepage for any location
                                            </p>
                                        </div>
                                        <div className="glass-button p-3 bg-cyan-500/20 border border-cyan-500/30 rounded-lg">
                                            <p className="text-cyan-200 text-sm">
                                                � <span className="font-semibold">Historical Trends</span> - Try New York City for sample data
                                            </p>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    )}

                    {}
                    <div className="glass-button p-3 sm:p-4">
                        <div className="flex flex-col sm:flex-row sm:items-center justify-between mb-4 gap-3">
                            <h3 className="text-lg lg:text-xl font-semibold text-white flex items-center space-x-2">
                                <MapPin className="h-5 w-5 lg:h-6 lg:w-6 text-cyan-400" />
                                <span>Available Locations</span>
                            </h3>
                            <div className="text-sm lg:text-base text-gray-400 bg-white/5 px-3 py-1 rounded-full">
                                {availableLocations.length} locations with trend data
                            </div>
                        </div>

                        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
                            {availableLocations.map((location) => (
                                <button
                                    key={location.uniqueKey || `${location.id}_${location.lat}_${location.lon}`}
                                    onClick={() => handleLocationSelect(location)}
                                    className={`p-3 sm:p-4 rounded-lg text-left transition-all ${selectedLocationId === location.id
                                        ? 'bg-cyan-500/20 border border-cyan-400/50 ring-2 ring-cyan-400/20'
                                        : 'bg-white/5 hover:bg-white/10 border border-transparent hover:border-white/20'
                                        }`}
                                >
                                    <div className="flex items-center justify-between">
                                        <div>
                                            <div className="font-medium text-white text-sm lg:text-base">
                                                {location.name || location.city}
                                            </div>
                                            <div className="text-xs lg:text-sm text-gray-400 mt-1">
                                                {location.lat?.toFixed(3) || '?'}, {location.lon?.toFixed(3) || '?'}
                                            </div>
                                        </div>
                                        {location.has_data && (
                                            <div className="w-2 h-2 lg:w-3 lg:h-3 bg-green-400 rounded-full"></div>
                                        )}
                                    </div>
                                </button>
                            ))}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default DashboardModal;