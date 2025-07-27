# Fairness in AI: Auditing and Mitigating Bias in Adult Content Categorization

**Author:** Louise Silva Ferreira
**Registration:** 2404017
**Supervisor:** Dr. Vishal Krishna Singh
**Second Supervisor:** Dr. Eirina Bourtsoulatze
**Course:** CE901 - MSc Dissertation

**Last updated:** 2025-07-27  
**License:** MIT (or state “All Rights Reserved” if you don’t want to share the code openly)

---

## Abstract

This research addresses algorithmic fairness in machine learning systems for adult content categorization, a domain where biases can perpetuate harmful stereotypes. By analyzing metadata from the RedTube API, this project identifies sources of bias related to protected attributes and implements a comprehensive methodology for bias mitigation. We compare a fine-tuned BERT model with an interpretable Random Forest classifier, evaluating them against standard fairness metrics to demonstrate a novel framework that balances performance with ethical considerations. This work introduces a hybrid NER pipeline for protected attribute detection in sensitive metadata, rarely addressed in existing literature.

---

## Directory Structure

- `/data`: Contains raw and processed datasets.
  - `/raw`: Original data downloaded from the API.
  - `/processed`: Cleaned, preprocessed, and feature-engineered data.
- `/documentation`: Markdown files explaining key methodological decisions.
- `/models`: Saved, trained machine learning models.
- `/notebooks`: Jupyter notebooks for exploratory data analysis (EDA) and final evaluation.
- `/scripts`: Modular Python scripts for the main data processing and modeling pipeline.
- - `01_data_collection.py`: RedTube API data acquisition
- `02_text_preprocessing.py`: Data cleaning and feature engineering
- `03_train_baselines.py`: Baseline model training
- `04_fairness_audit.py`: Bias analysis
- `05_bias_mitigation.py`: Fairness mitigation modeling

---

## Setup and Installation

1.  **Clone the repository:**

    ```bash
    git clone [Your GitLab Repository URL]
    cd fair_ml_adult_content
    ```

2.  **Create and activate a Python virtual environment:**

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Download additional NLP resources:**  
    Some scripts may require NLTK or spaCy models. Run the following:
    ```bash
    python -m spacy download en_core_web_sm
    python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords')"
    ```

---

## Execution Order

To reproduce the results, please run the scripts in the following order:

1.  `scripts/01_data_collection.py`: To download the raw data from the RedTube API.
2.  `scripts/02_text_preprocessing.py`: To clean the data and generate protected attributes.
3.  `scripts/03_train_baselines.py`: To train the baseline models.
4.  `scripts/04_fairness_audit.py`: To analyze the bias in the baseline models.
5.  `scripts/05_bias_mitigation.py`: To train and save the fairness-mitigated models.

The analysis can be viewed in the Jupyter notebooks located in the `/notebooks` directory.
