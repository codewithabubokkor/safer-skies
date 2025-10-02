import React from 'react'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import { AuthProvider } from './context/AuthContext'
import { LocationProvider } from './context/LocationContext'
import Home from './pages/Home'
import AlertsPage from './pages/AlertsPage'

function App() {
    return (
        <AuthProvider>
            <LocationProvider>
                <Router>
                    <Routes>
                        <Route path="/" element={<Home />} />
                        <Route path="/home" element={<Home />} />
                        <Route path="/alerts" element={<AlertsPage />} />
                    </Routes>
                </Router>
            </LocationProvider>
        </AuthProvider>
    )
}

export default App
