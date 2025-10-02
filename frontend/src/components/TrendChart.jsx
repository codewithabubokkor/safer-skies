import React from 'react';
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    Title,
    Tooltip,
    Legend,
    Filler
} from 'chart.js';
import { Line } from 'react-chartjs-2';

ChartJS.register(
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    Title,
    Tooltip,
    Legend,
    Filler
);

/**
 * 📈 Trend Chart Component
 * Interactive chart for AQI trend visualization
 */
const TrendChart = ({ data, height = 300 }) => {
    if (!data || !data.time_series) {
        return (
            <div className="flex items-center justify-center h-64 text-gray-400">
                <div className="text-center">
                    <div className="text-lg mb-2">📊</div>
                    <div>No chart data available</div>
                </div>
            </div>
        );
    }

    const { time_series } = data;

    const getAQIColor = (aqi) => {
        if (aqi <= 50) return '#00E400';      // Green
        if (aqi <= 100) return '#FFFF00';     // Yellow  
        if (aqi <= 150) return '#FF7E00';     // Orange
        if (aqi <= 200) return '#FF0000';     // Red
        if (aqi <= 300) return '#8F3F97';     // Purple
        return '#7E0023';                     // Maroon
    };

    const getAQICategory = (aqi) => {
        if (aqi <= 50) return 'Good';
        if (aqi <= 100) return 'Moderate';
        if (aqi <= 150) return 'Unhealthy for Sensitive Groups';
        if (aqi <= 200) return 'Unhealthy';
        if (aqi <= 300) return 'Very Unhealthy';
        return 'Hazardous';
    };

    const chartData = {
        labels: time_series.dates.map(date => {
            const d = new Date(date);
            return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        }),
        datasets: [
            {
                label: 'AQI',
                data: time_series.aqi_values,
                borderColor: '#3B82F6',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                tension: 0.4,
                fill: true,
                pointBackgroundColor: time_series.aqi_values.map(aqi => getAQIColor(aqi)),
                pointBorderColor: '#ffffff',
                pointBorderWidth: 2,
                pointRadius: 5,
                pointHoverRadius: 8,
                pointHoverBorderWidth: 3,
            }
        ]
    };

    const options = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: false, // Hide legend as we only have one dataset
            },
            tooltip: {
                backgroundColor: 'rgba(0, 0, 0, 0.8)',
                titleColor: '#ffffff',
                bodyColor: '#ffffff',
                borderColor: 'rgba(255, 255, 255, 0.2)',
                borderWidth: 1,
                cornerRadius: 8,
                displayColors: false,
                callbacks: {
                    title: function (context) {
                        const index = context[0].dataIndex;
                        const date = time_series.dates[index];
                        return new Date(date).toLocaleDateString('en-US', {
                            weekday: 'short',
                            month: 'short',
                            day: 'numeric'
                        });
                    },
                    label: function (context) {
                        const aqi = context.parsed.y;
                        const category = getAQICategory(aqi);
                        const index = context.dataIndex;
                        const dominantPollutant = time_series.dominant_pollutants[index];

                        return [
                            `AQI: ${aqi}`,
                            `Category: ${category}`,
                            `Main Pollutant: ${dominantPollutant || 'N/A'}`
                        ];
                    }
                }
            }
        },
        scales: {
            x: {
                grid: {
                    color: 'rgba(255, 255, 255, 0.1)',
                    drawBorder: false,
                },
                ticks: {
                    color: '#9CA3AF',
                    font: {
                        size: 12,
                    },
                    maxTicksLimit: 8, // Limit number of x-axis labels
                },
                border: {
                    display: false,
                }
            },
            y: {
                beginAtZero: true,
                max: Math.max(200, Math.max(...time_series.aqi_values) + 20),
                grid: {
                    color: 'rgba(255, 255, 255, 0.1)',
                    drawBorder: false,
                },
                ticks: {
                    color: '#9CA3AF',
                    font: {
                        size: 12,
                    },
                    callback: function (value) {
                        return Math.round(value);
                    }
                },
                border: {
                    display: false,
                }
            }
        },
        elements: {
            point: {
                hoverRadius: 10,
            }
        },
        interaction: {
            intersect: false,
            mode: 'index',
        },
        animation: {
            duration: 1000,
            easing: 'easeInOutQuart',
        }
    };

    return (
        <div style={{ height: `${height}px` }} className="relative">
            {}
            <div className="absolute top-2 right-2 flex space-x-1 z-10">
                {[
                    { range: '0-50', color: '#00E400', label: 'Good' },
                    { range: '51-100', color: '#FFFF00', label: 'Moderate' },
                    { range: '101-150', color: '#FF7E00', label: 'USG' },
                    { range: '151-200', color: '#FF0000', label: 'Unhealthy' },
                    { range: '201-300', color: '#8F3F97', label: 'Very Unhealthy' },
                    { range: '300+', color: '#7E0023', label: 'Hazardous' }
                ].map((item) => (
                    <div
                        key={item.range}
                        className="group relative"
                    >
                        <div
                            className="w-3 h-3 rounded-full opacity-80 hover:opacity-100 transition-opacity cursor-help"
                            style={{ backgroundColor: item.color }}
                        />
                        <div className="absolute bottom-full mb-1 left-1/2 transform -translate-x-1/2 px-2 py-1 bg-black/80 text-white text-xs rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap pointer-events-none">
                            {item.label}<br />{item.range}
                        </div>
                    </div>
                ))}
            </div>

            <Line data={chartData} options={options} />
        </div>
    );
};

export default TrendChart;