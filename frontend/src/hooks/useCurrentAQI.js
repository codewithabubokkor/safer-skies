import { useState, useEffect } from 'react';
import aqiService from '../services/AQIDataServiceNew';

/**
 * Hook for fetching current AQI data
 */
export const useCurrentAQI = (latitude, longitude, locationName = null, enabled = true) => {
    const [data, setData] = useState(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!enabled || !latitude || !longitude) {
            return;
        }

        const fetchData = async () => {
            setIsLoading(true);
            setError(null);

            try {
                const result = await aqiService.getCurrentAQI(latitude, longitude, locationName);

                setData({
                    aqi: result.current_aqi,
                    category: result.aqi_category,
                    pollutants: result.pollutants || {},
                    location: result.location
                });
            } catch (err) {
                console.error('Error fetching current AQI:', err);
                setError(err.message || 'Failed to fetch AQI data');
                setData(null);
            } finally {
                setIsLoading(false);
            }
        };

        fetchData();
    }, [latitude, longitude, locationName, enabled]);

    return { data, isLoading, error };
};

export default useCurrentAQI;