-- Momo App Database Setup Script
-- This script creates tables and inserts initial data for the Momo application.

CREATE TABLE Users (
    user_id INT PRIMARY KEY,
    full_name VARCHAR(100),
    phone_number VARCHAR(20)
);


CREATE TABLE Transaction_Categories (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(50),
    description VARCHAR(100)
);


CREATE TABLE Transactions (
    transaction_id INT PRIMARY KEY,
    sender_id INT,
    receiver_id INT,
    category_id INT,
    amount DECIMAL(10,2),
    transaction_date DATE,
    charges DECIMAL(10,2),
    FOREIGN KEY (sender_id) REFERENCES Users(user_id),
    FOREIGN KEY (receiver_id) REFERENCES Users(user_id),
    FOREIGN KEY (category_id) REFERENCES Transaction_Categories(category_id)
);

CREATE TABLE System_Logs (
    log_id INT PRIMARY KEY,
    transaction_id INT,
    log_type VARCHAR(50),
    log_date DATE,
    FOREIGN KEY (transaction_id) REFERENCES Transactions(transaction_id)
);

INSERT INTO Users (user_id, full_name, phone_number) VALUES
(1, 'KAI CENAT', '0788123456'),
(2, 'Brony LENY', '0788234567'),
(3, 'DUKE Denis', '0788345678'),
(4, 'TOTA Jonathan', '0788456789'),
(5, 'Michael B Jordan', '0788567890');

INSERT INTO Transaction_Categories (category_id, category_name, description) VALUES
(1, 'Airtime Purchase', 'Buying mobile airtime'),
(2, 'Bill Payment', 'Paying utility bills'),
(3, 'Money Transfer', 'Sending money to another user'),
(4, 'School Fees', 'Payment for school tuition'),
(5, 'Shopping', 'Purchasing goods and services');

INSERT INTO Transactions (transaction_id, sender_id, receiver_id, category_id, amount, transaction_date, charges) VALUES
(101, 1, 2, 3, 15000.00, '2025-09-06', 500.00),
(102, 3, 4, 5, 7000.00, '2025-09-07', 300.00),
(103, 2, 5, 2, 5000.00, '2025-09-08', 250.00),
(104, 4, 1, 1, 1000.00, '2025-09-09', 100.00),
(105, 5, 3, 4, 30000.00, '2025-09-10', 1000.00);

INSERT INTO System_Logs (log_id, transaction_id, log_type, log_date) VALUES
(201, 101, 'Success', '2025-09-06'),
(202, 102, 'Success', '2025-09-07'),
(203, 103, 'Failed', '2025-09-08'),
(204, 104, 'Pending', '2025-09-09'),
(205, 105, 'Success', '2025-09-10');