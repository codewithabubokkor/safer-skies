import React, { useState, useEffect, useRef } from 'react';
import mapboxGeolocationService from '../services/MapboxGeolocationService';
import locationCacheManager from '../services/LocationCacheManager';

const GeolocationSearch = ({ onLocationSelect, onAQIData, className = '', style = {}, skipAutoInit = false }) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [searchResults, setSearchResults] = useState([]);
    const [currentLocation, setCurrentLocation] = useState(null);
    const [userLocation, setUserLocation] = useState(null);
    const [isSearching, setIsSearching] = useState(false);
    const [isGettingLocation, setIsGettingLocation] = useState(false);
    const [showResults, setShowResults] = useState(false);
    const [permissionStatus, setPermissionStatus] = useState('unknown');
    const [error, setError] = useState('');
    const searchTimeoutRef = useRef(null);
    const resultsRef = useRef(null);
    const inputRef = useRef(null); // Add ref for input field

    useEffect(() => {
        if (!skipAutoInit) {
            initializeLocation();
        }
    }, [skipAutoInit]);

    useEffect(() => {
        const isTouchDevice = 'ontouchstart' in window || navigator.maxTouchPoints > 0;

        if (!isTouchDevice) {
            const timer = setTimeout(() => {
                if (inputRef.current) {
                    inputRef.current.focus();
                }
            }, 100);

            return () => clearTimeout(timer);
        }
    }, []);

    useEffect(() => {
        const handleClickOutside = (event) => {
            if (resultsRef.current && !resultsRef.current.contains(event.target) &&
                inputRef.current && !inputRef.current.contains(event.target)) {
                setShowResults(false);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const initializeLocation = async () => {
        try {
            console.log('🚀 INITIALIZING LOCATION SEARCH (Cache First)');

            const cachedLocation = locationCacheManager.getCurrentLocation();
            if (cachedLocation) {
                console.log('✅ FOUND CACHED LOCATION - NO API CALLS NEEDED');

                setCurrentLocation(cachedLocation);
                setUserLocation(cachedLocation);

                if (onLocationSelect) {
                    onLocationSelect(cachedLocation);
                }

                return;
            }

            console.log('❌ NO CACHED LOCATION - User will need to allow location access');

        } catch (error) {
            console.error('❌ Initialization error:', error.message);
        }
    };

    const requestUserLocation = async () => {
        if (isGettingLocation) {
            console.log('⏳ Location request already in progress');
            return;
        }

        try {
            setIsGettingLocation(true);
            setError('');

            console.log('📍 GETTING LOCATION (Cache First, API Last)...');

            const result = await mapboxGeolocationService.getCurrentLocation();

            if (result.success) {
                setUserLocation(result.data);
                setCurrentLocation(result.data);
                setSearchQuery(result.data.address || `${result.data.latitude.toFixed(4)}, ${result.data.longitude.toFixed(4)}`);

                if (onLocationSelect) {
                    onLocationSelect(result.data);
                }

                console.log('✅ Auto-detect location ready - location callback triggered');

                console.log(`✅ LOCATION READY - ${result.fromCache ? 'From Cache' : 'Fresh Data'}`);
            } else {
                throw new Error(result.error);
            }
        } catch (error) {
            console.error('❌ Location failed:', error.message);
            setError(error.message);
            setPermissionStatus('denied');
        } finally {
            setIsGettingLocation(false);
        }
    };

    const handleSearch = async (query) => {
        if (!query.trim()) {
            setSearchResults([]);
            setShowResults(false);
            return;
        }

        setIsSearching(true);
        setError('');

        console.log('🔍 SEARCHING FOR:', query);

        try {
            const result = await mapboxGeolocationService.searchLocation(query);

            if (result.success) {
                setSearchResults(result.data);
                setShowResults(result.data.length > 0);
                console.log(`✅ Search completed: ${result.data.length} results`);
            } else {
                throw new Error(result.error);
            }
        } catch (error) {
            console.error('❌ Search error:', error.message);
            setError('Search temporarily unavailable. Please try again.');
            setSearchResults([]);
            setShowResults(false);
        } finally {
            setIsSearching(false);
        }
    };

    const handleInputChange = (e) => {
        const value = e.target.value;
        setSearchQuery(value);

        if (searchTimeoutRef.current) {
            clearTimeout(searchTimeoutRef.current);
        }

        searchTimeoutRef.current = setTimeout(() => {
            if (value.length >= 2) {
                handleSearch(value);
            } else {
                setSearchResults([]);
                setShowResults(false);
            }
        }, 300);
    };

    const handleResultSelect = async (result) => {
        console.log('📍 LOCATION SELECTED:', result.address);

        setSearchQuery(result.address);
        setShowResults(false);
        setError('');

        try {
            const selectionResult = await mapboxGeolocationService.selectLocation(result);

            if (selectionResult.success) {
                setCurrentLocation(selectionResult.data);

                if (onLocationSelect) {
                    onLocationSelect(selectionResult.data);
                }

                console.log('✅ Search result selected - location callback triggered');
            } else {
                throw new Error(selectionResult.error);
            }
        } catch (error) {
            console.error('❌ Location selection error:', error.message);
            setError('Failed to select location. Please try again.');
        }
    };

    const handleUseCurrentLocation = () => {
        if (userLocation && !isGettingLocation) {
            console.log('📍 Using cached current location');
            setCurrentLocation(userLocation);
            setSearchQuery(userLocation.address || `${userLocation.latitude.toFixed(4)}, ${userLocation.longitude.toFixed(4)}`);
            setShowResults(false);

            if (onLocationSelect) {
                onLocationSelect(userLocation);
            }

            console.log('✅ Cached location button click - location callback triggered');
        } else {
            console.log('📍 Requesting fresh location');
            requestUserLocation();
        }
    };

    return (
        <div className={`geolocation-search-container ${className}`} style={style}>
            <style>{`
                .geolocation-search-container {
                    position: relative;
                    width: 100%;
                    max-width: 500px;
                }

                .search-input-wrapper {
                    position: relative;
                    background: rgba(255, 255, 255, 0.08);
                    backdrop-filter: blur(12px);
                    border: 1px solid rgba(255, 255, 255, 0.15);
                    border-radius: 25px;
                    padding: 0;
                    overflow: hidden;
                    transition: all 0.3s ease;
                }

                .search-input-wrapper:focus-within {
                    border-color: rgba(255, 255, 255, 0.3);
                    box-shadow: 0 0 20px rgba(255, 255, 255, 0.1);
                }

                .search-input {
                    width: 100%;
                    padding: 15px 55px 15px 20px;
                    background: transparent;
                    border: none;
                    outline: none;
                    color: white;
                    font-size: 16px;
                    font-weight: 300;
                    -webkit-appearance: none;
                    -moz-appearance: none;
                    appearance: none;
                    -webkit-text-size-adjust: 100%;
                    touch-action: manipulation;
                }

                .search-input::placeholder {
                    color: rgba(255, 255, 255, 0.6);
                }

                .search-input:focus {
                    -webkit-tap-highlight-color: transparent;
                }

                @media screen and (-webkit-min-device-pixel-ratio: 0) {
                    .search-input {
                        font-size: 16px !important;
                    }
                }

                .location-button {
                    position: absolute;
                    right: 8px;
                    top: 50%;
                    transform: translateY(-50%);
                    background: rgba(255, 255, 255, 0.1);
                    border: none;
                    border-radius: 50%;
                    width: 35px;
                    height: 35px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    cursor: pointer;
                    transition: all 0.3s ease;
                    color: white;
                    touch-action: manipulation;
                    -webkit-tap-highlight-color: transparent;
                }

                .location-button:hover {
                    background: rgba(255, 255, 255, 0.2);
                    transform: translateY(-50%) scale(1.1);
                }

                .location-button:active {
                    transform: translateY(-50%) scale(0.95);
                }

                .location-button:disabled {
                    opacity: 0.6;
                    cursor: not-allowed;
                }

                .location-button:disabled:hover {
                    transform: translateY(-50%) scale(1);
                }

                .search-results {
                    position: absolute;
                    top: 100%;
                    left: 0;
                    right: 0;
                    background: rgba(20, 20, 40, 0.95);
                    backdrop-filter: blur(20px);
                    border: 1px solid rgba(255, 255, 255, 0.1);
                    border-radius: 15px;
                    margin-top: 8px;
                    max-height: 300px;
                    overflow-y: auto;
                    z-index: 1000;
                    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
                }

                .search-result-item {
                    padding: 12px 16px;
                    color: white;
                    cursor: pointer;
                    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
                    transition: background 0.2s ease;
                    display: flex;
                    align-items: center;
                }

                .search-result-item:hover {
                    background: rgba(255, 255, 255, 0.1);
                }

                .search-result-item:last-child {
                    border-bottom: none;
                }

                .result-icon {
                    margin-right: 10px;
                    opacity: 0.7;
                }

                .result-text {
                    flex: 1;
                }

                .result-distance {
                    font-size: 12px;
                    opacity: 0.6;
                    margin-left: 10px;
                }

                .current-location-item {
                    background: rgba(0, 150, 255, 0.1);
                    border-bottom: 1px solid rgba(0, 150, 255, 0.2);
                }

                .current-location-item:hover {
                    background: rgba(0, 150, 255, 0.2);
                }

                .search-section-header {
                    padding: 8px 16px;
                    font-size: 11px;
                    font-weight: 600;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    color: rgba(255, 255, 255, 0.4);
                    background: rgba(255, 255, 255, 0.02);
                    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
                    margin: 0;
                }

                .no-results-message {
                    padding: 16px;
                    text-align: center;
                    color: rgba(255, 255, 255, 0.6);
                    font-style: italic;
                }

                .error-message {
                    color: #ff6b6b;
                    font-size: 12px;
                    margin-top: 8px;
                    text-align: center;
                    background: rgba(255, 107, 107, 0.1);
                    padding: 8px;
                    border-radius: 8px;
                    border-left: 3px solid #ff6b6b;
                }

                .loading-spinner {
                    position: absolute;
                    right: 50px;
                    top: 50%;
                    transform: translateY(-50%);
                    width: 20px;
                    height: 20px;
                    border: 2px solid rgba(255, 255, 255, 0.3);
                    border-top: 2px solid white;
                    border-radius: 50%;
                    animation: spin 1s linear infinite;
                }

                @keyframes spin {
                    0% { transform: translateY(-50%) rotate(0deg); }
                    100% { transform: translateY(-50%) rotate(360deg); }
                }

                .location-status {
                    font-size: 12px;
                    text-align: center;
                    margin-top: 8px;
                    opacity: 0.7;
                }

                .status-granted { color: #4ade80; }
                .status-denied { color: #f87171; }
                .status-prompt { color: #fbbf24; }

                .cache-info {
                    font-size: 10px;
                    text-align: center;
                    margin-top: 4px;
                    opacity: 0.5;
                    font-family: monospace;
                }
            `}</style>

            <div className="search-input-wrapper">
                <input
                    ref={inputRef}
                    type="text"
                    className="search-input"
                    placeholder="Search for a location or use current location..."
                    value={searchQuery}
                    onChange={handleInputChange}
                    onFocus={() => {
                        if (searchResults.length > 0) {
                            setShowResults(true);
                        }
                    }}
                    onBlur={(e) => {
                        setTimeout(() => {
                            if (!resultsRef.current?.contains(document.activeElement)) {
                                setShowResults(false);
                            }
                        }, 150);
                    }}
                    onKeyDown={(e) => {
                        if (e.key === 'Enter' && searchResults.length > 0) {
                            e.preventDefault();
                            console.log('🎯 Enter key pressed - selecting first result');
                            handleResultSelect(searchResults[0]);
                        }
                        else if (e.key === 'Escape') {
                            setShowResults(false);
                            inputRef.current?.blur();
                        }
                    }}
                />

                {isSearching && <div className="loading-spinner"></div>}

                <button
                    className="location-button"
                    onClick={handleUseCurrentLocation}
                    disabled={isGettingLocation}
                    title={isGettingLocation ? 'Getting location...' : 'Use current location'}
                >
                    {isGettingLocation ? '⏳' : '📍'}
                </button>
            </div>

            {showResults && (
                <div className="search-results" ref={resultsRef}>
                    {}
                    {userLocation && (
                        <div
                            className="search-result-item current-location-item"
                            onClick={handleUseCurrentLocation}
                        >
                            <span className="result-icon">
                                {isGettingLocation ? '⏳' : '📍'}
                            </span>
                            <div className="result-text">
                                <div>{isGettingLocation ? 'Getting Location...' : 'Use Current Location'}</div>
                                {currentLocation?.address && !isGettingLocation && (
                                    <div style={{ fontSize: '12px', opacity: 0.7 }}>
                                        {currentLocation.address}
                                    </div>
                                )}
                            </div>
                            {currentLocation?.accuracy && (
                                <div className="result-distance">
                                    ±{Math.round(currentLocation.accuracy)}m
                                </div>
                            )}
                        </div>
                    )}

                    {}
                    {searchResults.length > 0 && (
                        <>
                            {userLocation && (
                                <div className="search-section-header">Search Results</div>
                            )}
                            {searchResults.map((result, index) => (
                                <div
                                    key={`search-${index}`}
                                    className="search-result-item"
                                    onClick={() => handleResultSelect(result)}
                                >
                                    <span className="result-icon">📍</span>
                                    <div className="result-text">
                                        <div>{result.address}</div>
                                        {result.city && result.country && (
                                            <div style={{ fontSize: '12px', opacity: 0.7 }}>
                                                {result.city}, {result.country}
                                            </div>
                                        )}
                                    </div>
                                    <div className="result-distance">
                                        {result.relevance ?
                                            `${(result.relevance * 100).toFixed(0)}% match` :
                                            'Match'
                                        }
                                    </div>
                                </div>
                            ))}
                        </>
                    )}

                    {}
                    {searchResults.length === 0 && !userLocation && !isSearching && (
                        <div className="no-results-message">
                            <span className="result-icon">🔍</span>
                            <div className="result-text">
                                <div>No locations found</div>
                                <div style={{ fontSize: '12px', opacity: 0.7 }}>
                                    Try a different search term
                                </div>
                            </div>
                        </div>
                    )}
                </div>
            )}

            {error && <div className="error-message">{error}</div>}

            {permissionStatus !== 'unknown' && !error && (
                <div className={`location-status status-${permissionStatus}`}>
                    {permissionStatus === 'granted' && '✓ Location access enabled'}
                    {permissionStatus === 'denied' && '✗ Location access denied - search manually'}
                    {permissionStatus === 'prompt' && '⚡ Click 📍 to enable location'}
                </div>
            )}

            {}
            {process.env.NODE_ENV === 'development' && currentLocation && (
                <div className="cache-info">
                    📍 {currentLocation.source} • {currentLocation.city || 'Unknown City'}
                    {currentLocation.timestamp && (
                        <span> • {Math.floor((Date.now() - currentLocation.timestamp) / (1000 * 60))}m old</span>
                    )}
                </div>
            )}
        </div>
    );
};

export default GeolocationSearch;
