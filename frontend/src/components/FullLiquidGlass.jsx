import React, { useState, useEffect, useRef } from 'react';
import { useSpring, animated } from 'react-spring';

const FullLiquidGlass = ({ children, className = '', style = {}, draggable = false }) => {
    const [position, setPosition] = useState({ x: 0, y: 0 });
    const [dragging, setDragging] = useState(false);
    const cardRef = useRef(null);
    const initialPosition = useRef({ x: 0, y: 0 });
    const initialMousePosition = useRef({ x: 0, y: 0 });
    const isMobile = typeof window !== 'undefined' && window.innerWidth <= 768;

    const springProps = useSpring({
        transform: `translate(${position.x}px, ${position.y}px)`,
        config: { tension: 170, friction: 26 }
    });

    useEffect(() => {
        if (!draggable || isMobile) return;

        const handleMouseDown = (e) => {
            setDragging(true);
            initialPosition.current = { x: position.x, y: position.y };
            initialMousePosition.current = { x: e.clientX, y: e.clientY };
        };

        const handleMouseMove = (e) => {
            if (!dragging) return;

            const dx = e.clientX - initialMousePosition.current.x;
            const dy = e.clientY - initialMousePosition.current.y;

            setPosition({
                x: initialPosition.current.x + dx,
                y: initialPosition.current.y + dy
            });
        };

        const handleMouseUp = () => {
            setDragging(false);
        };

        const card = cardRef.current;
        if (card) {
            card.addEventListener('mousedown', handleMouseDown);
            window.addEventListener('mousemove', handleMouseMove);
            window.addEventListener('mouseup', handleMouseUp);
        }

        return () => {
            if (card) {
                card.removeEventListener('mousedown', handleMouseDown);
            }
            window.removeEventListener('mousemove', handleMouseMove);
            window.removeEventListener('mouseup', handleMouseUp);
        };
    }, [dragging, position, draggable]);

    const inlineStyles = `
        .full-liquid-glass {
            position: relative;
            padding: 2rem;
            border-radius: 28px;
            background-color: rgba(255, 255, 255, 0.03);
            backdrop-filter: brightness(1.05) blur(4px) url(#fullLiquidFilter);
            -webkit-backdrop-filter: brightness(1.05) blur(4px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            display: flex;
            align-items: center;
            justify-content: center;
            overflow: hidden;
            cursor: ${draggable ? 'grab' : 'default'};
            user-select: none;
            transition: backdrop-filter 0.3s ease;
        }

        @media (max-width: 768px) {
            .full-liquid-glass {
                padding: 1.5rem;
                border-radius: 20px;
                cursor: default !important; 
                backdrop-filter: brightness(1.05) blur(8px);
                -webkit-backdrop-filter: brightness(1.05) blur(8px);
            }
        }

        @media (max-width: 640px) {
            .full-liquid-glass {
                padding: 1rem;
                border-radius: 16px;
                backdrop-filter: brightness(1.05) blur(6px);
                -webkit-backdrop-filter: brightness(1.05) blur(6px);
            }
        }

        @media (max-width: 480px) {
            .full-liquid-glass {
                padding: 0.75rem;
                border-radius: 12px;
            }
        }

        .full-liquid-glass:active {
            cursor: ${draggable ? 'grabbing' : 'default'};
        }

        .full-liquid-glass::before {
            content: '';
            position: absolute;
            inset: 0;
            z-index: 0;
            border-radius: 28px;
            box-shadow: inset 1px 1px 0px 0px rgba(255, 255, 255, 0.2),
                        inset -1px -1px 0px 0px rgba(255, 255, 255, 0.1);
            pointer-events: none;
        }

        .full-liquid-glass > * {
            position: relative;
            z-index: 1;
        }
    `;

    return (
        <>
            <style dangerouslySetInnerHTML={{ __html: inlineStyles }} />

            <animated.div
                ref={cardRef}
                className={`full-liquid-glass ${className}`}
                style={{
                    ...springProps,
                    ...style,
                }}
            >
                {children}
            </animated.div>

            {}
            <svg style={{ position: 'absolute', width: 0, height: 0 }}>
                <defs>
                    <filter id="fullLiquidFilter">
                        <feTurbulence
                            type="fractalNoise"
                            baseFrequency="0.008"
                            numOctaves="1"
                            result="turbulence"
                        />
                        <feDisplacementMap
                            in="SourceGraphic"
                            in2="turbulence"
                            scale="12"
                            xChannelSelector="R"
                            yChannelSelector="G"
                        />
                    </filter>
                </defs>
            </svg>
        </>
    );
};

export default FullLiquidGlass;
