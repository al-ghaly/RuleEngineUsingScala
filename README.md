# Scala ETL Rule Engine

## Project Overview

This project is a Scala-based ETL (Extract, Transform, Load) application designed as a rule-based discount calculating engine for a large retail company. The engine reads transaction data from a CSV file, processes each transaction according to predefined rules to calculate discounts, and loads the processed data into an Oracle database for further analysis and reporting.

### Project Requirements

1. **Qualifying Rules and Calculation:**
   - Discounts based on product expiry, with a tiered discount system depending on how close the product is to its expiration date.
   - Special discounts for cheese and wine products.
   - A 50% discount for products sold on the 23rd of March.
   - Quantity-based discounts, encouraging bulk purchases.
   - Additional discounts for transactions made through the app and payments made using Visa cards.

2. **Technical Considerations:**
   - All core logic must be written in a pure functional manner:
     - Use only `vals`, no `vars`.
     - Avoid mutable data structures.
     - Exclude loops; instead, use recursion or higher-order functions.
     - Ensure all functions are pure; they should not produce side effects.
   - The application logs events in a specific format and writes them to `rules_engine.log`.

3. **Logging Format:**
   ```
   TIMESTAMP     LOGLEVEL      MESSAGE
   ```

4. **Technical Specifications:**
   - Scala and a JVM-compatible environment for execution.
   - Oracle Database for data persistence.
   - Proper error handling and logging of operations.

## Technology Stack

- **Scala** for the backend logic.
- **Oracle Database** for storing processed transactions.
- **SBT** for project build and dependency management.

## Features

- **Dynamic Discount Calculation:** Applies various discounts based on product expiry, sales category (cheese, wine), special sale dates, quantity, application usage, and Visa payments.
- **Functional Programming:** Adheres strictly to functional programming principles with immutable data structures and pure functions, ensuring predictable and side-effect-free operations.
- **Comprehensive Logging:** Logs detailed operational data to a file, which helps in monitoring and troubleshooting the application.



## Flow Chart

The following flow chart visualizes the sequence of operations from data input to processing and output:

![FlowChart](https://github.com/al-ghaly/RuleEngineUsingScala/assets/61648960/492e9da6-7cae-4729-bb95-abaaeacef2cc)


## Technical Details

### Code Structure

- **Transaction Processing**: Each transaction is parsed and processed through various discount rules.
- **Discount Qualification**: Discounts are determined based on set rules, including product type, expiry, special dates, and quantity thresholds.
- **Database Operations**: After calculating discounts, the transaction data, along with computed discounts, are stored in a database.
- **Logging**: Events and errors are logged in a file, following a specific format that includes a timestamp, log level, and message.

### Key Components

- **Transaction Classes**: Define the structure for transaction data and processed transaction data.
- **Rule Functions**: A set of functions that determine the eligibility for discounts based on different criteria.
- **Data Handling**: Functions to read from and write to CSV files and the Oracle database.
- **Logger Utility**: A utility to handle logging across the system uniformly.

### Core Functionality

- **Read Data:** The engine starts by reading transaction data from a CSV file. Each transaction includes details such as the timestamp of the sale, product name, expiry date, quantity sold, and payment method.

- **Apply Discount Rules:** Based on the transaction details, the engine applies several discount rules:
  - **Product Expiry:** Discounts are offered based on how soon a product is expiring.
  - **Product Type:** Special discounts for specific products like cheese and wine.
  - **Special Dates:** A substantial discount is applied to all products sold on the 23rd of March.
  - **Quantity Discounts:** Discounts increase with the quantity of the product purchased.
  - **App Usage:** Encourages use of the store's app by applying discounts based on the transaction channel.
  - **Payment Method:** Promotes the use of Visa cards by providing a discount for transactions using Visa.

- **Calculate Final Price:** After applying the relevant discounts, the engine calculates the final price for each transaction.

- **Write Results:** The processed transactions, along with the discounts applied and the final calculated prices, are then written to a database for persistence.

- **Logging:** Throughout its operation, the engine logs various events such as the start and end of data processing, errors, and other significant occurrences in the system.


## Getting Started

### Prerequisites

- Scala 2.12.x
- Java 8+
- SBT (Scala Build Tool)
- Oracle Database or another relational database with JDBC support

### Setup and Installation

1. **Clone the repository**:
   ```sh
   git clone <repository-url>
   ```
2. **Navigate to the project directory**:
   ```sh
   cd <project-directory>
   ```
3. **Compile the project using SBT**:
   ```sh
   sbt compile
   ```

### Running the Application

Run the application using SBT:
```sh
sbt run
```

## Configuration

- **Database Connection**: Set up the database connection parameters in the application to point to your Oracle database instance.
- **CSV File Path**: Ensure the path to the CSV file containing transaction data is correctly set in the application.


## Acknowledgements

This project was developed as part of the ITI 44 curriculum, leveraging functional programming techniques to solve practical problems in retail operations.

---
