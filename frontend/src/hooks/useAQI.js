import { useState, useEffect, useCallback } from 'react';
import aqiService from '../services/AQIDataServiceNew';

/**
 * Custom hook for managing AQI data state
 * Integrates with your NAQForecast pipeline data
 */
export const useAQI = () => {
    const [aqiData, setAqiData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [lastUpdated, setLastUpdated] = useState(null);

    /**
     * Fetch AQI data for a specific location
     */
    const fetchAQI = useCallback(async (latitude, longitude, city = null) => {
        if (!latitude || !longitude) {
            setError('Invalid coordinates provided');
            return;
        }

        setLoading(true);
        setError(null);

        try {
            const data = await aqiService.getCurrentAQI(latitude, longitude, city);

            setAqiData(data);
            setLastUpdated(new Date());
            setError(null);

        } catch (err) {
            console.error('❌ Error fetching AQI:', err);
            setError(err.message || 'Failed to fetch AQI data');
            setAqiData(null);
        } finally {
            setLoading(false);
        }
    }, []);

    /**
     * Refresh current AQI data
     */
    const refreshAQI = useCallback(() => {
        if (aqiData?.location) {
            fetchAQI(
                aqiData.location.latitude,
                aqiData.location.longitude,
                aqiData.location.city
            );
        }
    }, [aqiData, fetchAQI]);

    /**
     * Auto-refresh every 5 minutes
     */
    useEffect(() => {
        if (!aqiData) return;

        const interval = setInterval(() => {
            refreshAQI();
        }, 5 * 60 * 1000); // 5 minutes

        return () => clearInterval(interval);
    }, [aqiData, refreshAQI]);

    return {
        aqiData,
        loading,
        error,
        lastUpdated,
        fetchAQI,
        refreshAQI,

        getAQIColor: (aqi) => aqiService.getAQIColor(aqi),
        getAQICategory: (aqi) => aqiService.getAQICategory(aqi)
    };
};
