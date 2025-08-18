# N-gram Filtering and Analysis Application

An interactive web application for complex filtering and analysis of syntactic n-grams. Built with Streamlit and powered by a PostgreSQL database, this tool allows linguists and researchers to dynamically construct queries, identify linguistic patterns, and perform frequency analysis.

## Key Features

- **Dynamic Filtering:** Create multi-level filtering rules based on phrase length and characteristics of each word (token, lemma, part of speech, tag, syntactic dependency, morphology).
- **Pattern Moderation:** A built-in system for users to review, rate, and comment on linguistic patterns.
- **User Management:** An admin panel for managing user access and roles.
- **Save and Load:** Ability to save and load entire filter sets and individual rule blocks for reuse.
- **SQL Query View:** The application displays the generated SQL query, ensuring transparency and aiding in debugging.
- **Results Analysis:** Perform frequency analysis of words by position in the filtered phrases.
- **Performance Caching:** Repeated database queries are cached to ensure a fast user experience.

## Technology Stack

- **Framework:** Streamlit
- **Language:** Python 3
- **Database:** PostgreSQL
- **Key Python Libraries:** `psycopg2-binary`, `streamlit-cookies-manager`, `bcrypt`

## Getting Started

### Prerequisites

- Python 3.8+
- A running PostgreSQL server.

### Installation & Setup

1.  **Clone the repository:**
    ```bash
    git clone <YOUR_REPOSITORY_URL>
    cd patterns_filtration
    ```

2.  **Install dependencies:**
    ```bash
    # Navigate to the app directory
    cd ngrams_app
    # Install required packages
    pip install -r requirements.txt
    ```

3.  **Configure Database Connection:**
    - In the `ngrams_app/` directory, create a file named `.env`.
    - Add your database connection details to this file. This file is listed in `.gitignore` and will not be tracked by Git.
      ```env
      DB_HOST=your_db_host
      DB_PORT=5432
      DB_NAME=ngrams_2_0
      DB_USER=your_db_user
      DB_PASSWORD=your_db_password
      ```

4.  **Database Schema:**
    - Ensure your PostgreSQL database is created and populated.
    - The application requires several tables: `ngrams`, `users`, `unique_patterns`, `moderation_patterns`, `pattern_examples`, `saved_filters`, and `saved_blocks`.
    - For a detailed schema, please refer to the `app_documentation.md` file.

### Running the Application

1.  **Navigate to the application directory:**
    ```bash
    cd ngrams_app
    ```
2.  **Run the Streamlit app:**
    ```bash
    streamlit run Home.py
    ```
    The application will open in your default web browser.

## Copyright and License

© 2025, [Shushkin Roman Olegovich]. All Rights Reserved.

This code is the exclusive property of the owner. It is provided for viewing and demonstration purposes only. You may not use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software without the express written permission of the copyright holder.

**This project is NOT licensed under any open-source license.** Unauthorized use or redistribution is strictly prohibited.

## Future Development

- Enhanced user roles and permissions.
- Advanced data visualization features.
- Deployment on a cloud platform.
