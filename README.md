# N-gram Filtering and Analysis Application

## Project Description
An interactive Streamlit web application designed for complex filtering and analysis of syntactic n-grams (sequences of words) stored in a PostgreSQL database. The application allows users to dynamically construct queries to identify linguistic patterns and perform frequency analysis of words by position in filtered phrases.

## Key Features
- **Dynamic Filtering:** Create multi-level filtering rules based on phrase length and characteristics of each word (token, lemma, part of speech, tag, syntactic dependency, morphology).
- **Save and Load:** Ability to save and load entire filter sets as well as individual block templates for reuse.
- **SQL Query View:** The application displays the generated SQL query, ensuring transparency and aiding in debugging.
- **Results Analysis:** After obtaining filtered phrases, a frequency analysis of words can be performed for each position, showing the most frequent words and their counts.
- **Caching:** Repeated database queries are cached to speed up performance.
- **Modular Architecture:** The project is organized modularly for ease of development and scalability.

## Getting Started

### Prerequisites
- Python 3.8+
- PostgreSQL (with the `ngrams_2_0` database and necessary schema set up)

### Installation
1. Clone the repository:
   ```bash
   git clone <YOUR_REPOSITORY_URL>
   cd patterns_filtration
   ```
2. Install dependencies:
   ```bash
   pip install -r ngrams_app/requirements.txt
   ```

### Database Setup
1. Ensure you have a PostgreSQL database named `ngrams_2_0` with the `ngrams`, `saved_filters`, and `saved_blocks` tables.
2. Table structure:
   - `ngrams`: Main data table.
     - `id`: Unique identifier.
     - `text` (TEXT): N-gram text.
     - `len` (INTEGER): N-gram length in tokens.
     - `freq_mln` (NUMERIC): Normalized frequency (per million).
     - `frequency` (INTEGER): Absolute frequency.
     - `tokens` (JSONB): Array of tokens. `["word1", "word2"]`
     - `lemmas` (JSONB): Array of lemmas. `["lemma1", "lemma2"]`
     - `pos` (JSONB): Array of parts of speech. `["NOUN", "VERB"]`
     - `tags` (JSONB): Array of grammatical tags. `["Animacy=Anim", "Aspect=Imp"]`
     - `deps` (JSONB): Array of syntactic dependencies. `["nsubj", "root"]`
     - `morph` (JSONB): Array of full morphological features in nested JSON format. `[{"Feat1": "Val1"}, {"Feat2": "Val2"}]`
   - `saved_filters`: Table for storing filter sets.
     - `name` (TEXT, PRIMARY KEY): Unique set name.
     - `filters_json` (JSONB): JSON object with saved parameters.
   - `saved_blocks`: Table for storing block templates.
     - `name` (TEXT, PRIMARY KEY): Unique template name.
     - `block_json` (JSONB): JSON object with the saved block.
3. Create a `.env` file in the `ngrams_app/` directory with the following database connection details:
   ```
   DB_HOST=localhost
   DB_NAME=ngrams_2_0
   DB_USER=postgres
   DB_PASSWORD=YOUR_PASSWORD
   ```
   **Important:** The `.env` file is added to `.gitignore` and will not be uploaded to the repository.

### Running the Application
1. Navigate to the project root directory `patterns_filtration`:
   ```bash
   cd patterns_filtration
   ```
2. Run the application:
   ```bash
   streamlit run ngrams_app/Home.py
   ```
   The application will open in your web browser.

## Project Structure
- `ngrams_app/`: Main application directory.
  - `pages/`: Contains individual Streamlit application pages (`1_Фильтрация_фраз.py`).
  - `core/`: Modules with core logic (DB interaction, authentication, etc., e.g., `database.py`).
  - `.env`: File for storing environment variables (secrets).
  - `Home.py`: Main application page.
  - `requirements.txt`: List of Python dependencies.
  - `app_documentation.md`: Detailed application documentation (for developers).

## Usage
After launching the application, you can:
1. Select the "Phrase Filtering" page from the sidebar menu.
2. Specify the desired phrase length.
3. Add filter blocks, configure positions and rules for each word (token, lemma, part of speech, tag, syntactic dependency, morphology).
4. Use the "DEP", "POS", "TAG" buttons to fill blocks by template based on frequent sequences.
5. Apply filters and view results in the right column.
6. Expand the "Word Analysis by Position" block below the results table to see a frequency analysis of words for each position.
7. Use the "Save Set" and "Load Set" functions to manage filter configurations.
8. View the generated SQL query by clicking the "SQL" button.

## Future Development
- Implementation of an authorization system with different access levels (admin, moderator).
- Addition of new pages and functionality.
- Deployment on Streamlit Cloud.
