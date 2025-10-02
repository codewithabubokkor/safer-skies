import React, { createContext, useContext, useState } from 'react'

const LocationContext = createContext()

export const useLocation = () => {
    const context = useContext(LocationContext)
    if (!context) {
        throw new Error('useLocation must be used within a LocationProvider')
    }
    return context
}

export const LocationProvider = ({ children }) => {
    const [location, setLocation] = useState({
        city: 'New York',
        state: 'NY',
        coordinates: { lat: 40.7128, lng: -74.0060 }
    })

    const updateLocation = (newLocation) => {
        setLocation(newLocation)
    }

    const value = {
        location,
        updateLocation
    }

    return (
        <LocationContext.Provider value={value}>
            {children}
        </LocationContext.Provider>
    )
}
