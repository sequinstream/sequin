-- Create the wallet_transactions table
CREATE TABLE IF NOT EXISTS wallet_transactions (
    id INT PRIMARY KEY,
    user_uuid UUID NOT NULL,
    amount VARCHAR(20) NOT NULL,
    old_balance DECIMAL(15, 2) NOT NULL,
    new_balance DECIMAL(15, 2) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    payment_uuid UUID,
    batch_id UUID,
    credit_type VARCHAR(50),
    date TIMESTAMP NOT NULL
);
