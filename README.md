# Running the Project

Follow these steps to run the project:

1. **Build the Docker Image**
    ```sh
    docker build -t jupyter-notebook-spark .
    ```

2. **Start the Jupyter Service and PostgreSQL**
    ```sh
    docker-compose up
    ```

3. **Access Jupyter Notebook**
    - Copy the token from the output of the previous command.
    - Open your web browser and navigate to [http://localhost:8888/lab](http://localhost:8888/lab).
    - Paste the token when prompted.

4. **Run the Application**
    - Open a new terminal.
    - Navigate to the project directory:
        ```sh
        cd bitso_challenge
        ```
    - Run the application:
        ```sh
        python3.11 app.py
        ```

5. **Run the Tests**
    - Simply run:
        ```sh
        pytest
        ```

6. **Access PostgreSQL to view the tables and query them**
    - Use the VSCode extension from Chris Kolkman.
    - Configure it with the following settings:
        - **Hostname:** 0.0.0.0
        - **User:** postgres
        - **Password:** bitso
        - **Port:** 5432
        - **SSL Connection:** Standard Connection
        - **Database to Connect:** bitso

7. **Cleaning all up**
    ```sh
    docker-compose rm -fsv    
    ```