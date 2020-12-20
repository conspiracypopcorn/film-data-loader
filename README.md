# TrueFilm database loader
Combine and load Imdb and Wiki film data.

**Note**: in 76 cases it has not been possible to find a match between Imdb and wikipedia.
These are cases where:

- the film does not exist on wikipedia
- the title on the Wiki page is spelled differently
- there are multiple films with the same title in the same year

## How to run
Download and extract the two datasets:

- https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz
- https://www.kaggle.com/rounakbanik/the-movies-dataset/version/7#movies_metadata.csv 

####Requirements:

- Python 3.6 or later
- Docker

####Setup: run Postgres docker container

    docker run --name film_data -e POSTGRES_PASSWORD=secret -d -p 5433:5432 postgres
    
####Run script:

- Optional: create virtual environment


    python -m virtualenv venv
    source venv/bin/activate
    
- Install dependencies


    pip install -r requirements.txt
 

- Run script. The data will be loaded in table `films`.


    python load_films.py --movies <PATH TO movies_metadata.csv> \
        --wiki <PATH TO enwiki-latest-abstract.xml> \
        --psql_config postgresql://<USER>:<PASSWORD>@<HOST>:<PORT>/<DB>
        
- To run locally use: `--psql_config postgresql://postgres:secret@localhost:5433/postgres`

### Query data

Connect to Postgres instance running in Docker:


    docker exec -it film_data psql -h localhost -p 5432 -U postgres
    

Execute queries on `films` from the terminal:


    SELECT * FROM films;
    
### Cleanup
Stop and delete docker container:


    docker container stop film_data
    docker container rm film_data
    
### Tools Used

- Docker is used to run the local Postgres instance to ensure isolation
from other databases running on the machine
- Python is used as the main scripting tool, for the ease of running and
 good libraries for data analysis.
- Pandas allows to easily load and manipulate csv data. It relies on C implementation, which
speeds up a lot of operations on big datasets.

### Algorithmic choices
The film data is loaded first since it's smaller, and can fit in memory. Then it is
filtered to the top 1000 rows to speed up matching with the wikipedia data.

The wikipedia xml feed is parsed iteratively, and each element is deleted after reading, to
avoid filling the memory. For each element that is a good movie candidate, we perform a lookup
for its title (and year if available) in the 1000 films dataframe.

### Testing
Some techniques could be used to test the script:
- **Unit tests**: write some tests for each of the individual function used
- **End-to-end tests**: using a small dataset perform a full run of the script and 
automatically compare the generated table to the expected output.