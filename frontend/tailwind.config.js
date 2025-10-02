/** @type {import('tailwindcss').Config} */
export default {
    content: [
        "./index.html",
        "./src/**/*.{js,ts,jsx,tsx}",
    ],
    theme: {
        extend: {
            colors: {
                // NASA-inspired color scheme
                'nasa-blue': '#0B3D91',
                'sky-blue': '#4BA3C3',
                'aqi': {
                    'good': '#2ECC71',
                    'moderate': '#F1C40F',
                    'unhealthy': '#E67E22',
                    'very-unhealthy': '#E74C3C',
                    'hazardous': '#8E44AD'
                },
                'bg-light': '#F9FAFB',
                'bg-dark': '#1B1F24',
                'text-dark': '#222222',
                'text-light': '#FFFFFF'
            },
            fontFamily: {
                'heading': ['Poppins', 'sans-serif'],
                'body': ['Inter', 'sans-serif'],
                'mono': ['JetBrains Mono', 'monospace'],
                'inter': ['Inter', 'sans-serif'],
            },
            backgroundImage: {
                'nasa-gradient': 'linear-gradient(135deg, #0B3D91 0%, #4BA3C3 100%)',
                'sky-gradient': 'linear-gradient(to bottom, #4BA3C3 0%, #87CEEB 100%)',
                'earth-gradient': 'linear-gradient(135deg, #1e3c72 0%, #2a5298 100%)',
                'gradient-radial': 'radial-gradient(var(--tw-gradient-stops))',
            },
            animation: {
                'pulse': 'pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
                'fadeIn': 'fadeIn 0.6s ease-out',
                'slideUp': 'slideUp 0.6s ease-out',
                'fade-in': 'fadeIn 0.5s ease-in-out',
                'slide-up': 'slideUp 0.5s ease-out',
                'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite'
            },
            keyframes: {
                fadeIn: {
                    '0%': { opacity: '0', transform: 'translateY(20px)' },
                    '100%': { opacity: '1', transform: 'translateY(0)' },
                },
                slideUp: {
                    '0%': { opacity: '0', transform: 'translateY(40px)' },
                    '100%': { opacity: '1', transform: 'translateY(0)' },
                }
            }
        },
    },
    plugins: [
    ],
}
