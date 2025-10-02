import { useState, useEffect } from 'react';
import trendService from '../services/trendService';

/**
 * 📊 Custom hook for trend data management
 * Handles loading, caching, and error states for trend data
 */
export const useTrendData = (locationId, days = 30) => {
    const [trendData, setTrendData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!locationId) {
            setTrendData(null);
            setLoading(false);
            return;
        }

        const fetchTrends = async () => {
            setLoading(true);
            setError(null);

            try {
                const result = await trendService.getLocationTrends(locationId, days);

                if (result) {
                    setTrendData(result);
                    setError(null);
                } else {
                    // No data available is not an error - it's expected initially
                    setTrendData(null);
                    setError(null);
                }
            } catch (err) {
                setTrendData(null);
                setError(err.message || 'Failed to load trend data');
            } finally {
                setLoading(false);
            }
        };

        fetchTrends();
    }, [locationId, days]);

    const refetch = () => {
        if (locationId) {
            const fetchTrends = async () => {
                setLoading(true);
                setError(null);

                try {
                    const result = await trendService.getLocationTrends(locationId, days);

                    if (result) {
                        setTrendData(result);
                    } else {
                        setError('No trend data available');
                    }
                } catch (err) {
                    setError(err.message || 'Failed to load trend data');
                } finally {
                    setLoading(false);
                }
            };

            fetchTrends();
        }
    };

    return { trendData, loading, error, refetch };
};

/**
 * Hook for getting multiple locations' trend summaries
 */
export const useAllLocationsTrends = (days = 7) => {
    const [allTrends, setAllTrends] = useState({});
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        const fetchAllTrends = async () => {
            setLoading(true);
            setError(null);

            try {
                const result = await trendService.getAllLocationsSummary(days);

                if (result && result.locations) {
                    setAllTrends(result.locations);
                } else {
                    setError('No trend data available');
                }
            } catch (err) {
                setError(err.message || 'Failed to load trend data');
            } finally {
                setLoading(false);
            }
        };

        fetchAllTrends();
    }, [days]);

    return { allTrends, loading, error };
};