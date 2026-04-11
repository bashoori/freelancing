CREATE TABLE dim_staff (
    staff_id INT PRIMARY KEY,
    staff_name VARCHAR(100),
    role VARCHAR(100)
);

CREATE TABLE fact_leads (
    lead_id INT PRIMARY KEY,
    created_at TIMESTAMP,
    client_name VARCHAR(100),
    case_type VARCHAR(50),
    marketing_source VARCHAR(50),
    marketing_channel VARCHAR(50),
    city VARCHAR(50),
    status VARCHAR(50),
    assigned_staff_id INT,
    estimated_case_value DECIMAL(12,2)
);

CREATE TABLE fact_cases (
    case_id VARCHAR(20) PRIMARY KEY,
    lead_id INT,
    opened_at TIMESTAMP,
    closed_at TIMESTAMP,
    case_status VARCHAR(50),
    case_type VARCHAR(50),
    primary_attorney_id INT,
    settlement_amount DECIMAL(12,2)
);

CREATE TABLE fact_communications (
    event_id VARCHAR(50) PRIMARY KEY,
    lead_id INT,
    event_at TIMESTAMP,
    direction VARCHAR(20),
    status VARCHAR(20),
    channel VARCHAR(20)
);

CREATE TABLE fact_financials (
    txn_id VARCHAR(50) PRIMARY KEY,
    case_id VARCHAR(20),
    bill_date TIMESTAMP,
    fee_amount DECIMAL(12,2),
    cost_amount DECIMAL(12,2),
    payment_received DECIMAL(12,2)
);
