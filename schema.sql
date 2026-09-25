CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    account_number VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    amount DECIMAL(15,2),
    location VARCHAR(100),
    transaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    risk_score INTEGER,
    fraud_status VARCHAR(20),
    fraud_reason VARCHAR(255),
    FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    risk_score INTEGER,
    fraud_status VARCHAR(20),
    fraud_reason VARCHAR(255),
    alert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_transaction_summary (
    summary_date DATE PRIMARY KEY,
    total_transactions INTEGER,
    total_amount DECIMAL(15,2),
    fraud_transactions INTEGER,
    suspicious_transactions INTEGER
);