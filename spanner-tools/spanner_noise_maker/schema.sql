CREATE TABLE Users (
  UserId STRING(36) NOT NULL,
  Username STRING(100),
  JoinedAt TIMESTAMP,
) PRIMARY KEY (UserId);

CREATE TABLE Products (
  ProductId STRING(36) NOT NULL,
  OwnerId STRING(36),
  Price INT64,
  Category STRING(50),
) PRIMARY KEY (ProductId);

CREATE TABLE Orders (
  OrderId STRING(36) NOT NULL,
  BuyerId STRING(36) NOT NULL,
  ProductId STRING(36) NOT NULL,
  OrderTime TIMESTAMP NOT NULL,
  Amount INT64,
) PRIMARY KEY (OrderId);
