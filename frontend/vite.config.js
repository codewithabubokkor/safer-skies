import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
    plugins: [react()],
    server: {
        port: 3000,
        host: true,
        allowedHosts: [
            'localhost',
            '127.0.0.1',
            '.ngrok-free.app',
            '.ngrok.io'
        ],
        headers: {
            'ngrok-skip-browser-warning': 'true'
        },
        proxy: {
            '/api/alerts': {
                target: 'http://localhost:5003',
                changeOrigin: true,
                secure: false,
                headers: {
                    'ngrok-skip-browser-warning': 'true'
                }
            },
            '/api/user': {
                target: 'http://localhost:5003',
                changeOrigin: true,
                secure: false,
                headers: {
                    'ngrok-skip-browser-warning': 'true'
                }
            },
            '/api': {
                target: process.env.VITE_API_URL || 'http://localhost:5000',
                changeOrigin: true,
                secure: false,
                headers: {
                    'ngrok-skip-browser-warning': 'true'
                },
                configure: (proxy, options) => {
                    proxy.on('error', (err, req, res) => {
                        console.log('proxy error', err);
                    });
                    proxy.on('proxyReq', (proxyReq, req, res) => {
                        console.log('Sending Request to the Target:', req.method, req.url);
                    });
                    proxy.on('proxyRes', (proxyRes, req, res) => {
                        console.log('Received Response from the Target:', proxyRes.statusCode, req.url);
                    });
                }
            }
        }
    },
    build: {
        outDir: 'dist',
        sourcemap: false
    }
})
