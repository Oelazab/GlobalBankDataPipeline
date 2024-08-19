# GlobalBankDataPipeline

Extract, Transform and Load real-world data about the world's largest banks into a database for further processing and querying

## Features

- **Data Extraction:** Extracts the list of the top 10 banks by market capitalization from a specified URL.
- **Data Transformation:** Converts market capitalization values into GBP, EUR, and INR based on the exchange rate information provided in a CSV file.
- **Data Loading:** Saves the transformed data to a CSV file and an SQL database.
- **Query Execution:** Executes queries on the database to extract market capitalization values for specific offices in their local currencies.
- **Execution Logging:** Logs the progress and execution details of each function.

## Project Structure

```
GlobalBankDataPipeline/
├── bank_data_pipeline.py  # Main ETL script
├── Largest_banks_data.csv # Output CSV file
├── exchange_rate.csv      # Input CSV file
├── Banks.db               # SQLite database
├── code_log.txt           # Execution log
└── README.md              # Documentation
```

## Usage

Clone the repository:

```bash
git clone https://github.com/Oelazab/GlobalBankDataPipeline.git
cd GlobalBankDataPipeline
```

Run the ETL script:

```bash
python bank_data_pipeline.py
```

Outputs:

- View Largest_banks_data.csv.
- Check the Banks.db database.
- Review the code_log.txt file.
