import React, { useState } from 'react';
import { Scrollama, Step } from 'react-scrollama';
import Dashboard from './Dashboard';

const ScrollyTelling = ({ chapters, onChapterChange, currentData, onExit }) => {
    const [currentStepIndex, setCurrentStepIndex] = useState(0);

    // This callback fires when a Step enters the offset threshold (default 0.5)
    const onStepEnter = ({ data }) => {
        setCurrentStepIndex(data);
        onChapterChange(data);
    };

    return (
        <div style={{ position: 'relative', width: '100%', pointerEvents: 'none' }}>
            {/* Fixed Dashboard Overlay */}
            <div style={{
                position: 'fixed',
                top: '20px',
                right: '20px',
                zIndex: 10,
                pointerEvents: 'auto'
            }}>
                <Dashboard data={currentData} />
                <button
                    onClick={onExit}
                    style={{
                        marginTop: '10px',
                        padding: '8px 16px',
                        background: '#333',
                        color: 'white',
                        border: 'none',
                        borderRadius: '4px',
                        cursor: 'pointer',
                        width: '100%'
                    }}
                >
                    Exit Story Mode
                </button>
            </div>

            {/* Scrolling Narrative Steps */}
            <div style={{ paddingBottom: '50vh', marginTop: '50vh' }}>
                <Scrollama onStepEnter={onStepEnter} offset={0.5}>
                    {chapters.map((chapter, index) => (
                        <Step data={index} key={index}>
                            <div
                                style={{
                                    height: '100vh',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'flex-start',
                                    paddingLeft: '50px',
                                    opacity: currentStepIndex === index ? 1 : 0.3,
                                    transition: 'opacity 0.5s ease'
                                }}
                            >
                                <div style={{
                                    background: 'rgba(255, 255, 255, 0.95)',
                                    padding: '30px',
                                    borderRadius: '8px',
                                    maxWidth: '400px',
                                    boxShadow: '0 4px 20px rgba(0,0,0,0.1)',
                                    pointerEvents: 'auto'
                                }}>
                                    <h2 style={{ marginTop: 0, color: '#2c3e50' }}>{chapter.title}</h2>
                                    <p style={{ lineHeight: '1.6', fontSize: '1.1em', color: '#34495e' }}>
                                        {chapter.content}
                                    </p>
                                </div>
                            </div>
                        </Step>
                    ))}
                </Scrollama>
            </div>
        </div>
    );
};

export default ScrollyTelling;
