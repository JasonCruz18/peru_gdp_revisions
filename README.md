# :chart_with_upwards_trend: Peruvian GDP Revisions

Welcome to the **Peruvian GDP Revisions** repository! This project is dedicated to building and processing a comprehensive **Real-Time GDP (RTD)** dataset for Peru, focusing on the **extraction**, **cleaning**, and **analysis** of GDP revisions data from the **Weekly Reports (WR)** published by the **Central Reserve Bank of Peru (BCRP)**. 

This repository contains all the necessary scripts to build, clean, and process the GDP data, enabling users to track Peru’s economic performance over time.

---

## :rocket: Key Features

### **1. Data Extraction**
- **Automated extraction** of GDP growth data from both **scanned** and **digital PDFs** using **tabula** and **pdfplumber**.
- Streamlined process for collecting GDP growth rates from the BCRP’s **Weekly Reports**.

### **2. Data Cleaning & Transformation**
- Functions for cleaning raw GDP data:
  - Handle missing values
  - Normalize sector names
  - Adjust for **base-year revisions**
  - Convert raw data into **vintage formats**
  
### **3. Real-Time GDP Dataset (RTD)**
- The core focus is on creating a **Real-Time GDP (RTD)** dataset, which tracks **monthly** and **quarterly** GDP growth rates over time.

### **4. Benchmark Revisions**
- Apply **benchmark revisions** to GDP data, ensuring consistency across reports, particularly when base-year changes occur.

### **5. User-Independent Setup**
- This repository is designed to work **from scratch for users**—no external datasets are required!
- Simply run the provided scripts to generate the **complete RTD dataset** directly in your environment.

### **6. Results and Outputs**
- The **outputs** of each step (cleaned data, processed tables, etc.) are available **within the notebook** for easy inspection and verification.
- Users can track every stage of the data processing workflow.

---

## :hourglass_flowing_sand: Future Updates

- **Econometric Models**: Future updates will include econometric models for analyzing GDP revisions and forecasting.
- **Data Visualization**: Plans to enhance the project with **data visualizations** for easier analysis of the time series and trends.

---

## :books: How to Use

1. **Clone the repository**   
   Simply clone this repository to your local machine:
   ```
   git clone https://github.com/yourusername/peru_gdp_revisions.git
   ```
2. **Run the Jupyter Notebook**  
    Open the Jupyter Notebook provided in the repository to start the data extraction and cleaning process.

3. **View Outputs**
   After running the notebook, all outputs are available directly within the notebook for inspection.

---

:memo: Special Branch

We’ve created a dedicated branch called “coding_sample” for Columbia’s staff to review and assess the code. This branch contains all relevant scripts, outputs, and is fully ready for review. You can check it out [here](https://github.com/JasonCruz18/peru_gdp_revisions/blob/ea4bc7ef379879e356bb9f439c46d1b1469f8787/gdp_revisions_datasets/new_gdp_rtd.ipynb).

---

:information_source: Additional Information

No External Data Needed: The entire pipeline works from scratch—users don’t need any external datasets. You only need the provided scripts and input data sources (e.g., WR PDFs).

Clear Documentation: The project is well-documented, ensuring that each function and process is explained in detail for easy understanding and replication.

---

:package: Installation

Before running the scripts, ensure you have the necessary dependencies. You can install them using pip:

```
pip install -r requirements.txt
```

---

:warning: Disclaimer:

#If compared with the tables sample displayed in the supplemental document, the tables in this repository are transposed to save space and avoid long datasets with too many columns. If you transpose the tables #back, they will match the original structure in the supplemental document.

   
