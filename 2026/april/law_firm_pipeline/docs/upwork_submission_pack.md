# Upwork submission pack

## 1) Recent similar experience
In my recent work, I built and maintained end-to-end data pipelines that pulled data from multiple operational and external systems, standardized it into centralized reporting datasets, and supported dashboards used for decision-making. My background includes SQL-based warehousing, ETL development, Python data processing, API integrations, and reporting layers for both operational and leadership use cases.

A relevant example is a law-firm-style reporting project I built to simulate fragmented client data across leads, cases, communications, and financials. I designed the ingestion layer, created reporting marts, defined KPI logic, and prepared dashboard-ready outputs that answer business questions such as lead volume, case conversion, communication trends, and revenue by case type.

## 2) Certifications related to this project
- Microsoft Azure Data Fundamentals (DP-900)
- Databricks Fundamentals
- Databricks Generative AI Fundamentals
- Ongoing learning in Microsoft Fabric, Azure Data Factory, and lakehouse design

## 3) What techniques would you use to clean a data set?
I start by profiling the data to understand nulls, duplicates, invalid values, inconsistent formats, and mismatched keys across sources. Then I standardize field names, data types, date formats, categorical values, and identifiers. After that, I apply business rules such as deduplication, referential checks, status normalization, and source reconciliation. I also document assumptions so the cleaned dataset remains explainable and repeatable.

## 4) How do you deal with outliers or missing values in a dataset?
I do not treat outliers and missing values as only statistical issues. First I check whether they reflect real business behavior, data entry problems, or integration gaps. For missing values, I decide whether to impute, default, flag, or exclude based on how the field is used downstream. For outliers, I validate them against domain logic, compare them to source systems, and either retain them with flags, cap them for reporting, or isolate them for review. The goal is to protect trust in the reporting layer rather than force the data to look clean.

## 5) What tools do you use for data mining and visualization?
My core stack is SQL and Python for extraction, transformation, profiling, and data quality work. For visualization and business reporting, I use Power BI and can also work with dashboard layers that support executive reporting across multiple domains. For repo-based demos and prototypes, I use Streamlit because it is fast to publish and easy to review.

## 6) Loom video outline
- Who I am and the type of data systems I build
- Why this project stood out to me
- The business problem: fragmented law firm data across operations, marketing, finance, and communications
- How I would approach it: ingestion, standardization, data model, KPI layer, dashboard delivery
- Quick walkthrough of the GitHub project structure
- Why I am a strong fit for ongoing work after the first project
