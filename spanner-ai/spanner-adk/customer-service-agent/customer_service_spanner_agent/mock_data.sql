-- Copyright 2026 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Insert Sample Data into Customers Table
INSERT INTO Customers (CustomerId, Name, Email, CreatedDate) VALUES
('user123', 'Alice Smith', 'alice.smith@example.com', PENDING_COMMIT_TIMESTAMP()),
('user456', 'Bob Johnson', 'bob.j@example.com', PENDING_COMMIT_TIMESTAMP()),
('user789', 'Carol White', 'carol.w@example.com', PENDING_COMMIT_TIMESTAMP());

---

-- Insert Sample Data into Products Table
INSERT INTO Products (ProductId, Name, Description, Warranty, Price) VALUES
('PROD101', 'Wireless Headphones', 'High-fidelity wireless headphones with noise cancellation and 20-hour battery life.', '1-year manufacturer warranty', 99.99),
('PROD102', 'Smartwatch X', 'Feature-rich smartwatch with heart rate monitor, GPS, and notifications.', '2-year manufacturer warranty', 199.99),
('PROD103', 'Ergonomic Keyboard', 'Comfortable keyboard designed to reduce strain with customizable backlighting.', '6-month manufacturer warranty', 75.00),
('PROD104', 'Gaming Mouse Pro', 'Precision gaming mouse with adjustable DPI, programmable buttons, and RGB lighting.', '1-year manufacturer warranty', 50.00),
('PROD105', 'USB-C Hub', '7-in-1 USB-C hub with HDMI, USB 3.0, and SD card reader.', '1-year limited warranty', 35.50);

---

-- Insert Sample Data into Orders Table
-- Remember to explicitly set ReturnEligible and ReturnWindowDays
INSERT INTO Orders (CustomerId, OrderId, OrderDate, OrderStatus, TrackingNumber, EstimatedDelivery, ReturnEligible, ReturnWindowDays, LastUpdated) VALUES
('user123', 'ORD001', '2025-07-20', 'Shipped', 'TRACK12345', '2025-08-28', TRUE, 30, PENDING_COMMIT_TIMESTAMP()),
('user123', 'ORD002', '2025-08-10', 'Processing', NULL, NULL, TRUE, 30, PENDING_COMMIT_TIMESTAMP()),
('user123', 'ORD003', '2024-12-01', 'Delivered', 'TRACK67890', '2024-12-05', FALSE, 30, PENDING_COMMIT_TIMESTAMP()), -- Past return window
('user456', 'ORD004', '2025-08-22', 'Processing', NULL, NULL, TRUE, 30, PENDING_COMMIT_TIMESTAMP()),
('user789', 'ORD005', '2025-07-15', 'Delivered', 'TRACK00112', '2025-07-20', TRUE, 30, PENDING_COMMIT_TIMESTAMP());

---

-- Insert Sample Data into OrderItems Table
-- Remember the composite primary key (CustomerId, OrderId, ProductId)
INSERT INTO OrderItems (CustomerId, OrderId, ProductId, Quantity, UnitPrice, LineItemTotal) VALUES
('user123', 'ORD001', 'PROD101', 1, 99.99, 99.99),
('user123', 'ORD001', 'PROD102', 1, 199.99, 199.99),
('user123', 'ORD002', 'PROD103', 1, 75.00, 75.00),
('user123', 'ORD003', 'PROD104', 1, 50.00, 50.00),
('user456', 'ORD004', 'PROD101', 2, 99.99, 199.98),
('user456', 'ORD004', 'PROD105', 1, 35.50, 35.50),
('user789', 'ORD005', 'PROD102', 1, 199.99, 199.99);

---

-- Insert Sample Data into Returns Table
-- This table is initially empty in the frontend's mock data, but we can add some historical returns.
INSERT INTO Returns (ReturnId, OrderId, ProductId, CustomerId, ReturnStatus, RequestDate, ApprovalDate, RefundAmount, LastUpdated) VALUES
('RTN001', 'ORD003', 'PROD104', 'user123', 'Refunded', '2024-12-10', '2024-12-12', 50.00, PENDING_COMMIT_TIMESTAMP());