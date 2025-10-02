import React, { useState, useEffect, useRef, useCallback } from 'react';
import Globe from 'react-globe.gl';
import { TextureLoader, ShaderMaterial, Vector2 } from 'three';

const VELOCITY = 1; // minutes per frame - realistic day/night cycle

// Mock AQI data for North American cities
const northAmericaCities = [
    { name: 'New York', lat: 40.7128, lng: -74.0060, aqi: 42 },
    { name: 'Los Angeles', lat: 34.0522, lng: -118.2437, aqi: 87 },
    { name: 'Chicago', lat: 41.8781, lng: -87.6298, aqi: 35 },
    { name: 'Houston', lat: 29.7604, lng: -95.3698, aqi: 65 },
    { name: 'Phoenix', lat: 33.4484, lng: -112.0740, aqi: 93 },
    { name: 'Philadelphia', lat: 39.9526, lng: -75.1652, aqi: 28 },
    { name: 'San Antonio', lat: 29.4241, lng: -98.4936, aqi: 55 },
    { name: 'San Diego', lat: 32.7157, lng: -117.1611, aqi: 48 },
    { name: 'Dallas', lat: 32.7767, lng: -96.7970, aqi: 71 },
    { name: 'San Jose', lat: 37.3382, lng: -121.8863, aqi: 52 },
    { name: 'Toronto', lat: 43.6532, lng: -79.3832, aqi: 31 },
    { name: 'Vancouver', lat: 49.2827, lng: -123.1207, aqi: 22 },
    { name: 'Montreal', lat: 45.5017, lng: -73.5673, aqi: 29 },
    { name: 'Mexico City', lat: 19.4326, lng: -99.1332, aqi: 35 },
    { name: 'Atlanta', lat: 33.7490, lng: -84.3880, aqi: 44 },
    { name: 'Miami', lat: 25.7617, lng: -80.1918, aqi: 38 },
    { name: 'Seattle', lat: 47.6062, lng: -122.3321, aqi: 25 },
    { name: 'Denver', lat: 39.7392, lng: -104.9903, aqi: 67 }
];

const getAQIColor = (aqi) => {
    if (aqi <= 50) return '#00e400';      // Good (Green)
    if (aqi <= 100) return '#ffff00';     // Moderate (Yellow)  
    if (aqi <= 150) return '#ff7e00';     // Unhealthy for Sensitive (Orange)
    if (aqi <= 200) return '#ff0000';     // Unhealthy (Red)
    if (aqi <= 300) return '#8f3f97';     // Very Unhealthy (Purple)
    return '#7e0023';                     // Hazardous (Maroon)
};

const dayNightShader = {
    vertexShader: `
        varying vec3 vNormal;
        varying vec2 vUv;
        void main() {
            vNormal = normalize(normalMatrix * normal);
            vUv = uv;
            gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
        }
    `,
    fragmentShader: `
        #define PI 3.141592653589793
        uniform sampler2D dayTexture;
        uniform sampler2D nightTexture;
        uniform vec2 sunPosition;
        uniform vec2 globeRotation;
        varying vec3 vNormal;
        varying vec2 vUv;

        float toRad(in float a) {
            return a * PI / 180.0;
        }

        vec3 Polar2Cartesian(in vec2 c) { // [lng, lat]
            float theta = toRad(90.0 - c.x);
            float phi = toRad(90.0 - c.y);
            return vec3( // x,y,z
                sin(phi) * cos(theta),
                cos(phi),
                sin(phi) * sin(theta)
            );
        }

        void main() {
            float invLon = toRad(globeRotation.x);
            float invLat = -toRad(globeRotation.y);
            mat3 rotX = mat3(
                1, 0, 0,
                0, cos(invLat), -sin(invLat),
                0, sin(invLat), cos(invLat)
            );
            mat3 rotY = mat3(
                cos(invLon), 0, sin(invLon),
                0, 1, 0,
                -sin(invLon), 0, cos(invLon)
            );
            vec3 rotatedSunDirection = rotX * rotY * Polar2Cartesian(sunPosition);
            float intensity = dot(normalize(vNormal), normalize(rotatedSunDirection));
            vec4 dayColor = texture2D(dayTexture, vUv);
            vec4 nightColor = texture2D(nightTexture, vUv);
            float blendFactor = smoothstep(-0.1, 0.1, intensity);
            gl_FragColor = mix(nightColor, dayColor, blendFactor);
        }
    `
};

const sunPosAt = dt => {
    const day = new Date(+dt).setUTCHours(0, 0, 0, 0);
    const dayOfYear = Math.floor((dt - new Date(new Date(dt).getFullYear(), 0, 1)) / 86400000);
    const declination = 23.45 * Math.sin((360 * (284 + dayOfYear) / 365) * Math.PI / 180);
    const longitude = (day - dt) / 864e5 * 360 - 180;
    return [longitude, declination];
};

const getBackgroundGradient = (sunPosition) => {
    const sunLong = ((sunPosition[0] % 360) + 360) % 360;
    const dayStrength = Math.cos((sunLong - 180) * Math.PI / 360);

    if (dayStrength > 0.7) {
        return 'linear-gradient(135deg, #4A90E2 0%, #87CEEB 50%, #B0E0E6 100%)';
    } else if (dayStrength > 0.3) {
        return 'linear-gradient(135deg, #FF6B35 0%, #87CEEB 50%, #B0E0E6 100%)';
    } else if (dayStrength > -0.3) {
        return 'linear-gradient(135deg, #FF6B35 0%, #F7931E 25%, #9B59B6 75%, #2C3E50 100%)';
    } else {
        return 'linear-gradient(135deg, #0F0F23 0%, #1A1A2E 25%, #16213E 75%, #0F3460 100%)';
    }
};

const EarthBackground = () => {
    const [dt, setDt] = useState(+new Date());
    const [globeMaterial, setGlobeMaterial] = useState();
    const [cities, setCities] = useState([]);
    const [autoRotate, setAutoRotate] = useState(0);
    const globeRef = useRef();

    useEffect(() => {
        function iterateTime() {
            setDt(dt => dt + VELOCITY * 60 * 1000);
            requestAnimationFrame(iterateTime);
        }
        iterateTime();
    }, []);

    useEffect(() => {
        function rotateEarth() {
            setAutoRotate(rotate => (rotate + 0.3) % 360);
            requestAnimationFrame(rotateEarth);
        }
        rotateEarth();
    }, []);

    useEffect(() => {
        if (globeRef.current && globeRef.current.controls) {
            const controls = globeRef.current.controls();
            if (controls) {
                controls.autoRotate = true;
                controls.autoRotateSpeed = 0.5;
                controls.enableZoom = true;
                controls.enablePan = true;
                controls.enableRotate = true;
                controls.autoRotateSpeed = 0.5;
            }
        }
    }, [cities]); // Changed dependency to cities to trigger after city data loads

    useEffect(() => {
        Promise.all([
            new TextureLoader().loadAsync('//cdn.jsdelivr.net/npm/three-globe/example/img/earth-day.jpg'),
            new TextureLoader().loadAsync('//cdn.jsdelivr.net/npm/three-globe/example/img/earth-night.jpg')
        ]).then(([dayTexture, nightTexture]) => {
            setGlobeMaterial(new ShaderMaterial({
                uniforms: {
                    dayTexture: { value: dayTexture },
                    nightTexture: { value: nightTexture },
                    sunPosition: { value: new Vector2() },
                    globeRotation: { value: new Vector2() }
                },
                vertexShader: dayNightShader.vertexShader,
                fragmentShader: dayNightShader.fragmentShader
            }));
        });
    }, []);

    useEffect(() => {
        if (globeMaterial) {
            const sunPos = sunPosAt(dt);
            globeMaterial.uniforms.sunPosition.value.set(sunPos[0], sunPos[1]);
        }
    }, [dt, globeMaterial]);

    useEffect(() => {
        const cityData = northAmericaCities.map(city => ({
            ...city,
            color: getAQIColor(city.aqi),
            size: Math.max(0.1, city.aqi / 500), // Much larger size for visibility
            altitude: 0.02 + (city.aqi / 2000) // Higher altitude for better visibility
        }));
        setCities(cityData);
    }, []);

    const currentSunPosition = sunPosAt(dt);

    return (
        <div
            className="fixed inset-0 w-full h-full z-20"
            style={{
                background: getBackgroundGradient(currentSunPosition),
                pointerEvents: 'auto'  // Allow Earth interaction when not touching cards
            }}
        >
            <Globe
                ref={globeRef}
                globeMaterial={globeMaterial}
                backgroundImageUrl="//cdn.jsdelivr.net/npm/three-globe/example/img/night-sky.png"

                labelsData={cities}
                labelLat="lat"
                labelLng="lng"
                labelText="name"
                labelSize={d => Math.max(0.5, d.aqi / 100)} // Size based on AQI
                labelDotRadius={d => Math.max(0.2, d.aqi / 200)} // Dot size based on AQI
                labelColor={d => d.color}
                labelResolution={2}
                labelAltitude={d => d.altitude}
                labelLabel={d => `
                    <div style="background: rgba(0,0,0,0.9); color: white; padding: 10px; border-radius: 6px; font-family: sans-serif; border: 2px solid ${d.color};">
                        <div style="font-weight: bold; color: ${d.color}; font-size: 14px;">${d.name}</div>
                        <div style="margin: 4px 0;">AQI: <span style="color: ${d.color}; font-weight: bold; font-size: 16px;">${d.aqi}</span></div>
                        <div style="color: ${d.color}; font-size: 12px;">
                            ${d.aqi <= 50 ? '✅ Good Air Quality' :
                        d.aqi <= 100 ? '⚠️ Moderate' :
                            d.aqi <= 150 ? '🔶 Unhealthy for Sensitive' :
                                d.aqi <= 200 ? '🔴 Unhealthy' :
                                    d.aqi <= 300 ? '🟣 Very Unhealthy' : '🚨 Hazardous'}
                        </div>
                    </div>
                `}


                onZoom={useCallback(({ lng, lat }) => {
                    if (globeMaterial) {
                        globeMaterial.uniforms.globeRotation.value.set(lng, lat);
                    }
                }, [globeMaterial])}

                width={window.innerWidth}
                height={window.innerHeight}
                showGlobe={true}
                showAtmosphere={true}
                atmosphereColor="#ffffff"
                atmosphereAltitude={0.15}
                enablePointerInteraction={true}
            />
        </div>
    );
};

export default EarthBackground;