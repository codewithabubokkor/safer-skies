/**
 * AQI Utility functions for color coding and status determination
 */

/**
 * Get AQI color based on EPA standards
 * @param {number} aqi - Air Quality Index value
 * @returns {string} - Hex color code
 */
export const getAQIColor = (aqi) => {
    if (aqi <= 50) return '#2ECC71'     // Good - Green
    if (aqi <= 100) return '#F1C40F'    // Moderate - Yellow
    if (aqi <= 150) return '#E67E22'    // Unhealthy for Sensitive - Orange
    if (aqi <= 200) return '#E74C3C'    // Unhealthy - Red
    if (aqi <= 300) return '#8E44AD'    // Very Unhealthy - Purple
    return '#7E0023'                    // Hazardous - Maroon
}

/**
 * Get AQI status text based on EPA standards
 * @param {number} aqi - Air Quality Index value
 * @returns {string} - Status text
 */
export const getAQIStatus = (aqi) => {
    if (aqi <= 50) return 'Good'
    if (aqi <= 100) return 'Moderate'
    if (aqi <= 150) return 'Unhealthy for Sensitive Groups'
    if (aqi <= 200) return 'Unhealthy'
    if (aqi <= 300) return 'Very Unhealthy'
    return 'Hazardous'
}

/**
 * Get AQI category with color and status
 * @param {number} aqi - Air Quality Index value
 * @returns {object} - Object with status and color
 */
export const getAQICategory = (aqi) => {
    return {
        status: getAQIStatus(aqi),
        color: getAQIColor(aqi)
    }
}

/**
 * Get AQI level for health recommendations
 * @param {number} aqi - Air Quality Index value
 * @returns {string} - Health level (good, moderate, unhealthy, etc.)
 */
export const getAQILevel = (aqi) => {
    if (aqi <= 50) return 'good'
    if (aqi <= 100) return 'moderate'
    if (aqi <= 150) return 'sensitive'
    if (aqi <= 200) return 'unhealthy'
    if (aqi <= 300) return 'very-unhealthy'
    return 'hazardous'
}

/**
 * Check if AQI level is concerning (>100)
 * @param {number} aqi - Air Quality Index value
 * @returns {boolean} - True if concerning
 */
export const isAQIConcerning = (aqi) => {
    return aqi > 100
}

/**
 * Check if AQI level is dangerous (>200)
 * @param {number} aqi - Air Quality Index value
 * @returns {boolean} - True if dangerous
 */
export const isAQIDangerous = (aqi) => {
    return aqi > 200
}