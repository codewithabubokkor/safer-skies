/**
 * AQI Data Service - Fetches real-time AQI data from NAQForecast pipeline
 * Connects to your generated AQI files from the fusion pipeline
 */

class AQIDataService {
    constructor() {
        this.API_BASE_URL = '/api/location'; // Use the same unified API as Why Today
        this.CACHE_TTL = 5 * 60 * 1000; // 5 minutes cache
        this.cache = new Map();

        console.log('🌬️ AQIDataService initialized - Using unified smart location API');
    }

    /**
     * Get current AQI data for a specific location
     * @param {number} latitude 
     * @param {number} longitude 
     * @param {string} city - optional city name
     * @returns {Promise<Object>} AQI data
     */
    async getCurrentAQI(latitude, longitude, city = null) {
        const cacheKey = `${latitude.toFixed(3)}_${longitude.toFixed(3)}`;

        if (this.cache.has(cacheKey)) {
            const cached = this.cache.get(cacheKey);
            if (Date.now() - cached.timestamp < this.CACHE_TTL) {
                console.log('✅ AQI Cache Hit:', cached.data.location.city);
                return cached.data;
            }
        }

        try {
            console.log(`🌬️ Fetching from Why Today API: lat=${latitude}, lon=${longitude}`);
            const response = await fetch(`/api/why-today/location?lat=${latitude}&lon=${longitude}`);

            if (response.ok) {
                const apiData = await response.json();
                console.log('🔍 Why Today API response:', apiData);

                if (apiData.success && apiData.data) {
                    const cityName = apiData.data.city_name || apiData.location?.city || 'Unknown';

                    const data = {
                        location: {
                            city: cityName,  // Use city from Why Today API data
                            latitude: latitude,
                            longitude: longitude
                        },
                        current_aqi: apiData.data.aqi_value,
                        aqi_category: this.getAQICategory(apiData.data.aqi_value),
                        dominant_pollutant: apiData.data.primary_pollutant,
                        pollutants: {}, // Why Today doesn't have detailed pollutants
                        epa_message: apiData.data.health_context || '',
                        health_recommendations: [],
                        last_updated: apiData.data.timestamp,
                        data_sources: ['Why Today API - Same Database Table'],
                        confidence: apiData.data.confidence_score || 0.95,
                        timestamp: apiData.timestamp
                    };

                    this.cacheData(cacheKey, data);
                    console.log('✅ AQI data from Why Today API:', {
                        city: cityName,
                        aqi: data.current_aqi,
                        category: data.aqi_category
                    });
                    return data;
                } else {
                    console.warn('⚠️ Why Today API returned error:', apiData);
                }
            } else {
                console.warn('⚠️ Why Today API request failed:', response.status, response.statusText);
            }
        } catch (error) {
            console.error('❌ Why Today API request error:', error.message);
        }        // Fallback 1: Try to load your real AQI files directly
        console.log('🔄 API failed, trying to load real AQI files...');
        const cityData = await this.getClosestCityAQI(latitude, longitude);
        if (cityData) {
            this.cacheData(cacheKey, cityData);
            return cityData;
        }

        console.log('🔄 File loading failed, trying why-today API for real data...');
        try {
            const whyTodayResponse = await fetch(`/api/why-today/location?lat=${latitude}&lon=${longitude}`);
            if (whyTodayResponse.ok) {
                const whyTodayData = await whyTodayResponse.json();
                if (whyTodayData.data && whyTodayData.data.aqi_value) {
                    console.log('✅ Got real AQI from why-today API:', whyTodayData.data.aqi_value);

                    const realAQIData = {
                        location: {
                            city: whyTodayData.location?.city || 'Unknown',
                            latitude: latitude,
                            longitude: longitude,
                            distance_km: whyTodayData.distance_km || 0
                        },
                        current_aqi: whyTodayData.data.aqi_value,
                        aqi_category: this.getAQICategory(whyTodayData.data.aqi_value),
                        dominant_pollutant: whyTodayData.data.primary_pollutant || 'PM25',
                        pollutants: {},
                        epa_message: whyTodayData.data.health_context || '',
                        health_recommendations: [],
                        last_updated: whyTodayData.data.timestamp || new Date().toISOString(),
                        data_sources: ['Why Today API - Real Data'],
                        confidence: whyTodayData.data.confidence_score || 0.95,
                        timestamp: whyTodayData.data.timestamp || new Date().toISOString()
                    };

                    this.cacheData(cacheKey, realAQIData);
                    return realAQIData;
                }
            }
        } catch (error) {
            console.warn('⚠️ Why-today API also failed:', error.message);
        }

        console.log('📊 All real data sources failed, using enhanced demo data');
        return this.generateEnhancedDemoAQI(latitude, longitude, city);
    }

    /**
     * Transform production API response to frontend format
     */
    transformAPIData(apiData) {
        return {
            location: {
                city: apiData.location?.city || apiData.location?.nearest_city || 'Unknown',
                latitude: apiData.location?.latitude,
                longitude: apiData.location?.longitude,
                distance_km: apiData.location?.distance_km || 0
            },
            current_aqi: apiData.current_aqi,
            aqi_category: apiData.category || 'Unknown',
            dominant_pollutant: apiData.dominant_pollutant || 'PM25',
            pollutants: this.transformPollutants(apiData.pollutants || {}),
            epa_message: apiData.description || '',
            health_recommendations: [],
            last_updated: apiData.last_updated || new Date().toISOString(),
            data_sources: [apiData.data_source || 'Safer Skies System'],
            confidence: 0.85,
            distance_km: apiData.location?.distance_km || 0,
            timestamp: apiData.last_updated || new Date().toISOString()
        };
    }

    /**
     * Transform pollutants data from API format
     */
    transformPollutants(pollutants) {
        const transformed = {};
        if (pollutants) {
            for (const [key, value] of Object.entries(pollutants)) {
                transformed[key] = typeof value === 'object' ? value.aqi_value : value;
            }
        }
        return transformed;
    }

    /**
     * Find the closest city from your generated AQI files
     */
    async getClosestCityAQI(latitude, longitude) {
        const cities = [
            { name: 'Boston_42.361_-71.057', display_name: 'Boston', lat: 42.361, lon: -71.057 },
            { name: 'New_York_40.713_-74.006', display_name: 'New York', lat: 40.713, lon: -74.006 },
            { name: 'Philadelphia_39.953_-75.165', display_name: 'Philadelphia', lat: 39.953, lon: -75.165 },
            { name: 'Washington_DC_38.895_-77.036', display_name: 'Washington DC', lat: 38.895, lon: -77.036 }
        ];

        let closestCity = null;
        let minDistance = Infinity;

        for (const city of cities) {
            const distance = this.calculateDistance(latitude, longitude, city.lat, city.lon);
            if (distance < minDistance) {
                minDistance = distance;
                closestCity = city;
            }
        }

        if (closestCity && minDistance <= 100) { // Within 100km
            try {
                console.log(`🎯 Loading real AQI file for ${closestCity.display_name}`);
                const response = await fetch(`/aqi/current/${closestCity.name}/aqi_current.json`);

                if (response.ok) {
                    const cityData = await response.json();
                    console.log('✅ Real AQI data loaded from file:', cityData);

                    const transformedData = {
                        location: {
                            city: cityData.location?.name || closestCity.display_name,
                            latitude: latitude,
                            longitude: longitude,
                            distance_km: Math.round(minDistance * 10) / 10
                        },
                        current_aqi: cityData.aqi?.overall?.value || 0,
                        aqi_category: cityData.aqi?.overall?.category || 'Unknown',
                        dominant_pollutant: cityData.aqi?.overall?.dominant_pollutant || 'PM25',
                        pollutants: this.transformRealPollutants(cityData.aqi?.pollutants || {}),
                        epa_message: cityData.health?.message || '',
                        health_recommendations: cityData.health?.sensitive_groups || [],
                        last_updated: cityData.timestamp || cityData.data_quality?.last_updated || new Date().toISOString(),
                        data_sources: cityData.data_quality?.data_sources || ['Real Pipeline Data'],
                        confidence: 0.95, // High confidence for real data
                        timestamp: cityData.timestamp || new Date().toISOString()
                    };

                    console.log('🔄 Transformed real data:', {
                        city: transformedData.location.city,
                        aqi: transformedData.current_aqi,
                        category: transformedData.aqi_category,
                        source: 'Real File Data'
                    });

                    return transformedData;
                }
            } catch (error) {
                console.warn(`⚠️ Could not load real data for ${closestCity.display_name}:`, error.message);
            }
        }

        return null;
    }

    /**
     * Transform real pollutants data from your files
     */
    transformRealPollutants(pollutants) {
        const transformed = {};
        if (pollutants) {
            for (const [key, value] of Object.entries(pollutants)) {
                transformed[key.toLowerCase()] = value.aqi || value;
            }
        }
        return transformed;
    }

    /**
     * Calculate distance between two coordinates (Haversine formula)
     */
    calculateDistance(lat1, lon1, lat2, lon2) {
        const R = 6371; // Earth's radius in km
        const dLat = this.deg2rad(lat2 - lat1);
        const dLon = this.deg2rad(lon2 - lon1);
        const a =
            Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(this.deg2rad(lat1)) * Math.cos(this.deg2rad(lat2)) *
            Math.sin(dLon / 2) * Math.sin(dLon / 2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c; // Distance in km
    }

    deg2rad(deg) {
        return deg * (Math.PI / 180);
    }

    /**
     * Adapt your AQI file format to frontend format
     */
    adaptAQIData(aqiFileData, requestLat, requestLon) {
        return {
            location: {
                latitude: requestLat,
                longitude: requestLon,
                city: aqiFileData.location.city || 'Current Location',
                address: aqiFileData.location.address || `${requestLat.toFixed(3)}, ${requestLon.toFixed(3)}`
            },
            current_aqi: aqiFileData.overall_aqi,
            aqi_category: aqiFileData.aqi_category,
            dominant_pollutant: aqiFileData.dominant_pollutant,
            pollutants: {
                pm25: aqiFileData.pollutants.PM25?.aqi_value || 0,
                pm10: aqiFileData.pollutants.PM10?.aqi_value || 0,
                o3: aqiFileData.pollutants.O3?.aqi_value || 0,
                no2: aqiFileData.pollutants.NO2?.aqi_value || 0,
                so2: aqiFileData.pollutants.SO2?.aqi_value || 0,
                co: aqiFileData.pollutants.CO?.aqi_value || 0
            },
            epa_message: aqiFileData.epa_advisory?.cautionary_statement || this.getEPAMessage(aqiFileData.overall_aqi),
            health_recommendations: aqiFileData.epa_advisory?.health_recommendations || [],
            last_updated: aqiFileData.last_updated || new Date().toISOString(),
            data_sources: aqiFileData.data_sources || ['NAQForecast Pipeline'],
            confidence: aqiFileData.confidence_metrics?.overall_confidence || 0.85
        };
    }

    /**
     * Generate realistic demo AQI data based on your actual pipeline results
     * Your pipeline produces: NY=44, Philadelphia=35, Boston=36, Washington=37
     */
    generateDemoAQI(latitude, longitude, city) {
        const baseAQI = 35 + Math.floor(Math.random() * 10); // 35-44 range like your actual results

        const pollutants = {
            pm25: Math.max(15, baseAQI + Math.floor(Math.random() * 8) - 4), // PM2.5 usually lower
            pm10: Math.max(20, baseAQI + Math.floor(Math.random() * 6) - 3), // PM10 similar
            o3: Math.max(8, Math.floor(Math.random() * 15) + 8), // O3: 8-23 range (your fix made this realistic)
            no2: Math.max(10, baseAQI + Math.floor(Math.random() * 8) - 4), // NO2 moderate
            so2: Math.max(5, Math.floor(Math.random() * 10) + 5), // SO2 usually low
            co: Math.max(8, Math.floor(Math.random() * 12) + 8) // CO moderate
        };

        const currentAQI = Math.max(...Object.values(pollutants));
        const category = this.getAQICategory(currentAQI);
        const dominantPollutant = Object.keys(pollutants).find(key => pollutants[key] === currentAQI);

        return {
            location: {
                latitude,
                longitude,
                city: city || 'Current Location',
                address: city || `${latitude.toFixed(3)}, ${longitude.toFixed(3)}`
            },
            current_aqi: currentAQI,
            aqi_category: category,
            dominant_pollutant: dominantPollutant?.toUpperCase(),
            pollutants,
            epa_message: this.getEPAMessage(currentAQI),
            health_recommendations: this.getHealthRecommendations(category),
            last_updated: new Date().toISOString(),
            data_sources: ['NAQForecast Pipeline (Demo)', 'Real-time Fusion Engine'],
            confidence: 0.88,
            note: 'Demo data based on your actual pipeline results (AQI 35-44 Good)'
        };
    }    /**
     * Calculate distance between two coordinates (Haversine formula)
     */
    calculateDistance(lat1, lon1, lat2, lon2) {
        const R = 6371; // Earth's radius in kilometers
        const dLat = (lat2 - lat1) * Math.PI / 180;
        const dLon = (lon2 - lon1) * Math.PI / 180;
        const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
            Math.sin(dLon / 2) * Math.sin(dLon / 2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    /**
     * Get AQI category from numeric value
     */
    getAQICategory(aqi) {
        if (aqi <= 50) return 'Good';
        if (aqi <= 100) return 'Moderate';
        if (aqi <= 150) return 'Unhealthy for Sensitive Groups';
        if (aqi <= 200) return 'Unhealthy';
        if (aqi <= 300) return 'Very Unhealthy';
        return 'Hazardous';
    }

    /**
     * Get EPA message for AQI value
     */
    getEPAMessage(aqi) {
        if (aqi <= 50) return 'Air quality is satisfactory and poses little or no health concern.';
        if (aqi <= 100) return 'Air quality is acceptable. However, sensitive individuals may experience minor issues.';
        if (aqi <= 150) return 'Sensitive groups may experience health effects. The general public is less likely to be affected.';
        if (aqi <= 200) return 'Some members of the general public may experience health effects; sensitive groups may experience more serious effects.';
        if (aqi <= 300) return 'Health alert: The risk of health effects is increased for everyone.';
        return 'Health warning: Everyone is more likely to be affected by serious health effects.';
    }

    /**
     * Get health recommendations based on category
     */
    getHealthRecommendations(category) {
        const recommendations = {
            'Good': ['Great day for outdoor activities!', 'All groups can enjoy normal outdoor activities.'],
            'Moderate': ['Sensitive individuals should consider limiting prolonged outdoor activities.', 'Generally acceptable for most people.'],
            'Unhealthy for Sensitive Groups': ['Sensitive groups should limit outdoor activities.', 'Consider moving activities indoors.'],
            'Unhealthy': ['Everyone should limit outdoor activities.', 'Sensitive groups should avoid outdoor activities.'],
            'Very Unhealthy': ['Avoid all outdoor activities.', 'Keep windows closed and use air purifiers.'],
            'Hazardous': ['Stay indoors and keep activity levels low.', 'Follow emergency recommendations from local authorities.']
        };
        return recommendations[category] || [];
    }

    /**
     * Cache AQI data
     */
    cacheData(key, data) {
        this.cache.set(key, {
            data,
            timestamp: Date.now()
        });

        if (this.cache.size > 10) {
            const firstKey = this.cache.keys().next().value;
            this.cache.delete(firstKey);
        }
    }

    /**
     * Get AQI color for visualization
     */
    getAQIColor(aqi) {
        if (aqi <= 50) return '#00E400'; // Green - Good
        if (aqi <= 100) return '#FFFF00'; // Yellow - Moderate  
        if (aqi <= 150) return '#FF7E00'; // Orange - Unhealthy for Sensitive Groups
        if (aqi <= 200) return '#FF0000'; // Red - Unhealthy
        if (aqi <= 300) return '#8F3F97'; // Purple - Very Unhealthy
        return '#7E0023'; // Maroon - Hazardous
    }

    /**
     * Get AQI category name
     */
    getAQICategory(aqi) {
        if (aqi <= 50) return 'Good';
        if (aqi <= 100) return 'Moderate';
        if (aqi <= 150) return 'Unhealthy for Sensitive Groups';
        if (aqi <= 200) return 'Unhealthy';
        if (aqi <= 300) return 'Very Unhealthy';
        return 'Hazardous';
    }

    /**
     * Generate 5-day forecast (placeholder for your forecast system)
     */
    async getForecast(latitude, longitude) {
        const baseAQI = await this.getCurrentAQI(latitude, longitude);
        const forecast = [];

        for (let day = 0; day < 5; day++) {
            const date = new Date();
            date.setDate(date.getDate() + day);

            const variation = Math.floor(Math.random() * 10) - 5;
            const dayAQI = Math.max(25, Math.min(75, baseAQI.current_aqi + variation));

            forecast.push({
                date: date.toISOString().split('T')[0],
                aqi: dayAQI,
                category: this.getAQICategory(dayAQI),
                dominant_pollutant: baseAQI.dominant_pollutant
            });
        }

        return forecast;
    }

    /**
     * Enhanced demo data generator based on real coordinates and your pipeline results
     */
    generateEnhancedDemoAQI(latitude, longitude, city = null) {
        const cityData = {
            'boston': { aqi: 36, city: 'Boston', dominant: 'PM25' },     // Updated from real data
            'new_york': { aqi: 44, city: 'New York', dominant: 'O3' },   // Updated from real data  
            'philadelphia': { aqi: 55, city: 'Philadelphia', dominant: 'PM25' },
            'washington': { aqi: 41, city: 'Washington DC', dominant: 'O3' },
            'chicago': { aqi: 38, city: 'Chicago', dominant: 'PM25' },
            'los_angeles': { aqi: 68, city: 'Los Angeles', dominant: 'NO2' },
            'miami': { aqi: 35, city: 'Miami', dominant: 'O3' },
            'denver': { aqi: 42, city: 'Denver', dominant: 'PM25' },
            'seattle': { aqi: 28, city: 'Seattle', dominant: 'NO2' },
            'default': { aqi: 44, city: 'Demo Location', dominant: 'PM25' }
        };

        let selectedCity = cityData.default;

        if (city) {
            const cityKey = city.toLowerCase().replace(/[^a-z]/g, '_');
            selectedCity = cityData[cityKey] || cityData.default;
        } else {
            if (latitude >= 42.3 && latitude <= 42.4 && longitude >= -71.1 && longitude <= -71.0) {
                selectedCity = cityData.boston;
            } else if (latitude >= 40.7 && latitude <= 40.8 && longitude >= -74.1 && longitude <= -74.0) {
                selectedCity = cityData.new_york;
            } else if (latitude >= 39.9 && latitude <= 40.0 && longitude >= -75.2 && longitude <= -75.1) {
                selectedCity = cityData.philadelphia;
            } else if (latitude >= 38.8 && latitude <= 38.9 && longitude >= -77.1 && longitude <= -77.0) {
                selectedCity = cityData.washington;
            }
        }

        console.log(`🎯 Enhanced demo for ${selectedCity.city}: AQI ${selectedCity.aqi}`);

        return {
            location: {
                city: selectedCity.city,
                latitude: latitude,
                longitude: longitude,
                distance_km: 0
            },
            current_aqi: selectedCity.aqi,
            aqi_category: this.getAQICategory(selectedCity.aqi),
            dominant_pollutant: selectedCity.dominant,
            pollutants: {
                'pm25': selectedCity.aqi - 5,
                'pm10': selectedCity.aqi + 3,
                'no2': selectedCity.aqi - 2,
                'o3': selectedCity.aqi + 7,
                'co': Math.max(15, selectedCity.aqi - 10),
                'so2': Math.max(10, selectedCity.aqi - 15)
            },
            epa_message: this.getHealthMessage(selectedCity.aqi),
            health_recommendations: this.getHealthRecommendations(selectedCity.aqi),
            last_updated: new Date().toISOString(),
            data_sources: ['Enhanced Demo Data - Based on Pipeline Results'],
            confidence: 0.90,
            timestamp: new Date().toISOString()
        };
    }

    /**
     * Get health message based on AQI level
     */
    getHealthMessage(aqi) {
        if (aqi <= 50) return 'Air quality is good. Perfect for outdoor activities!';
        if (aqi <= 100) return 'Air quality is acceptable. Unusually sensitive people should consider limiting prolonged outdoor exertion.';
        if (aqi <= 150) return 'Members of sensitive groups may experience health effects. The general public is less likely to be affected.';
        if (aqi <= 200) return 'Some members of the general public may experience health effects; members of sensitive groups may experience more serious health effects.';
        if (aqi <= 300) return 'Health alert: The risk of health effects is increased for everyone.';
        return 'Health warning of emergency conditions: everyone is more likely to be affected.';
    }

    /**
     * Get health recommendations based on AQI level
     */
    getHealthRecommendations(aqi) {
        if (aqi <= 50) return ['Great day for outdoor activities!', 'Perfect air quality for exercise', 'Enjoy time outside'];
        if (aqi <= 100) return ['Generally safe for outdoor activities', 'Sensitive groups should be cautious', 'Good day for most people'];
        if (aqi <= 150) return ['Reduce prolonged outdoor activities', 'Consider indoor exercises', 'Watch for symptoms if sensitive'];
        if (aqi <= 200) return ['Limit outdoor activities', 'Keep windows closed', 'Consider air purifiers'];
        if (aqi <= 300) return ['Avoid outdoor activities', 'Stay indoors when possible', 'Use air filtration'];
        return ['Emergency conditions', 'Stay indoors', 'Seek medical advice if needed'];
    }
}

const aqiService = new AQIDataService();
export default aqiService;
