import React from 'react';

const MetricCard = ({ label, value, subtext, color = '#333' }) => (
    <div style={{
        background: '#f8f9fa',
        padding: '15px',
        borderRadius: '8px',
        marginBottom: '10px',
        borderLeft: `4px solid ${color}`
    }}>
        <div style={{ fontSize: '0.9em', color: '#666', textTransform: 'uppercase', letterSpacing: '1px' }}>
            {label}
        </div>
        <div style={{ fontSize: '1.8em', fontWeight: 'bold', color: '#333', margin: '5px 0' }}>
            {value}
        </div>
        {subtext && <div style={{ fontSize: '0.8em', color: '#888' }}>{subtext}</div>}
    </div>
);

const Dashboard = ({ data }) => {
    if (!data) return null;

    const {
        zip_code,
        median_household_income,
        poverty_rate,
        rent_index,
        neighborhood_name
    } = data;

    const annual_rent = rent_index * 12;
    const rent_burden = median_household_income > 0
        ? ((annual_rent / median_household_income) * 100).toFixed(1)
        : 'N/A';

    const formatCurrency = (val) => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            maximumSignificantDigits: 3
        }).format(val);
    };

    return (
        <div style={{
            background: 'white',
            padding: '20px',
            borderRadius: '12px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
            width: '300px'
        }}>
            <h2 style={{ marginTop: 0, marginBottom: '20px', borderBottom: '1px solid #eee', paddingBottom: '10px' }}>
                {neighborhood_name} <span style={{ color: '#888', fontSize: '0.6em' }}>({zip_code})</span>
            </h2>

            <MetricCard
                label="Median Income"
                value={formatCurrency(median_household_income)}
                color={
                    median_household_income >= 200000 ? '#388e3c' :
                        median_household_income >= 80000 ? '#fbc02d' :
                            '#d32f2f'
                }
            />

            <MetricCard
                label="Market Rent"
                value={`${formatCurrency(rent_index)}/mo`}
                subtext={`Est. Annual: ${formatCurrency(annual_rent)}`}
                color="#ff0000"
            />

            <MetricCard
                label="Rent Burden"
                value={`${rent_burden}%`}
                subtext="of income goes to rent"
                color={rent_burden > 30 ? '#d32f2f' : '#388e3c'}
            />

            <MetricCard
                label="Poverty Rate"
                value={`${poverty_rate}%`}
                color={poverty_rate > 20 ? '#d32f2f' : '#fbc02d'}
            />
        </div>
    );
};

export default Dashboard;
