# Automated Invoice Processing System for Retailers

## Overview

This project focuses on automating the invoice processing workflow for retailers, leveraging technologies like **Optical Character Recognition (OCR)**, **Robotic Process Automation (RPA)**, and **Google Cloud Platform (GCP)** services. The system is designed to streamline accounts payable processes, enhance accuracy, and reduce manual effort.

## Key Features

- **Automated Invoice Capture**: Efficiently captures invoices from multiple sources including emails, scanned documents, and digital portals.
- **Data Extraction and Validation**: Uses OCR technology to extract invoice data and validate it against business rules.
- **Invoice Matching and Reconciliation**: Matches invoices with Purchase Orders (POs) and Goods Receipts to ensure accurate reconciliation.
- **Exception Handling**: Handles discrepancies in invoice data with automated workflows and alerts.
- **Approval Workflows**: Defines tiers of approval based on invoice amounts.
- **Reporting and Analytics**: Provides real-time analytics and dashboards using **Google Looker Studio** for invoice tracking and performance insights.

## Technologies Used

- **Python**: Core logic and data processing.
- **OpenCV and PyTesseract**: Image processing and OCR for text extraction from scanned invoices.
- **Google Cloud Platform (GCP)**:
  - **Cloud Functions**: For automation of tasks and workflow triggers.
  - **Pub/Sub**: Manages real-time messaging between services.
  - **BigQuery**: Data storage and analytics.
  - **Looker Studio**: Reporting and visualization of invoice data.
- **SendGrid API**: For sending exception handling alerts via email.
- **Pandas**: Data manipulation and validation.

## Project Workflow

1. **Invoice Capture**: Invoices are captured from email attachments, uploaded scanned documents, or digital invoice portals.
2. **Preprocessing**: Images are processed (OTSU thresholding, dilation, contouring) to enhance text clarity for OCR extraction.
3. **Data Extraction**: The text is extracted using OCR and stored in a structured format (CSV).
4. **Validation**: Extracted data is validated against business rules, ensuring completeness and accuracy.
5. **Exception Handling**: Any discrepancies are identified and alerts are sent through an exception workflow.
6. **Approval Workflow**: Invoices are routed for approval based on predefined thresholds.
7. **Reporting**: Real-time analytics and reports are generated using Looker Studio.

## Dataset Information

- **FATURA Dataset**: Used for testing OCR capabilities, containing 10,000 scanned invoice images.
- **PASS Dataset**: Contains payment and purchase order data used to simulate invoice processing.

## Project Architecture

1. **OCR Implementation**: Extracts text from invoice images using OpenCV and PyTesseract.
2. **ETL Pipeline**: An ETL process cleans and processes incoming data using GCP services.
3. **Exception Handling**: Identifies issues such as invalid invoice numbers or mismatched dates and sends alerts via email.
4. **Approval Mechanism**: Sends invoice approval requests based on invoice amounts to respective managerial tiers.
5. **Analytics & Reporting**: Visualization and performance monitoring using Looker Studio.

## Results and Findings

- The **OCR process** successfully extracts data from non-digital invoices.
- The **ETL pipeline** processes and cleans large datasets efficiently.
- The system handles exceptions like mismatched dates and missing invoice numbers effectively, sending real-time email alerts.
- Analysis of **purchase order trends** shows peak orders during the fiscal year-end, providing insights for retailers.

## Future Enhancements

1. **Improved OCR Accuracy**: Implement advanced OCR models for better text extraction, especially for handwritten documents.
2. **Machine Learning**: Introduce predictive analytics for exception handling to proactively address discrepancies.
3. **Mobile and Chatbot Support**: Develop mobile accessibility for real-time invoice submission and chatbot assistance for queries.
4. **Vendor Self-Service Portal**: Allow vendors to submit invoices and track payments independently.

## Conclusion

The **Automated Invoice Processing System** offers retailers an efficient, scalable, and accurate solution for handling large volumes of invoices, automating tasks like data capture, validation, and approval. By integrating GCP and OCR technology, the project highlights the potential of automation in retail accounting practices.

## References

- [FATURA Dataset](https://zenodo.org/records/10371464)
- [Purchase Orders from PASS](https://opendata.dc.gov/datasets/5b4f4c2ad7934d9ca74a689301b82b76_16/explore)
- [Payments from PASS](https://opendata.dc.gov/datasets/e08160211e394d9eadbfee515a9856b1_17/explore)
