# read film data into sqlite table
# read wiki data into sqlite
import sqlite3
import csv
from datetime import datetime
import xml.etree.cElementTree as ET
import re


con = sqlite3.connect("local.db")

con.execute("DROP TABLE IF EXISTS movies")

con.execute('''
    CREATE TABLE movies (
        title TEXT,
        budget INTEGER,
        year INTEGER,
        revenue INTEGER,
        rating REAL,
        production_company TEXT)
    ''')

con.execute("DROP TABLE IF EXISTS wiki")

con.execute('''
    CREATE TABLE wiki (
        title TEXT NOT NULL,
        url TEXT,
        abstract TEXT
    )
    ''')
con.execute("CREATE INDEX wiki_title ON wiki(title)")


def load_film_data(con, film_file):
    with open(film_file) as f:
        reader = csv.DictReader(f)
        for row in reader:

            title, budget, revenue, rating = row["title"], row["budget"], row["revenue"], row["vote_average"]

            try:
                year = datetime.strptime(row["release_date"], "%Y-%m-%d").year
            except:
                year = None

            production_company = row["production_companies"]

            if revenue != "0" and budget != "0":
                con.execute("INSERT INTO movies (title, budget, year, revenue, rating, production_company) values (?,?,?,?,?,?)",
                            (title, budget, year, revenue, rating, production_company))
        con.commit()


def is_film(document):
    film_anchors = ["plot", "cast", "production", "soundtrack", "marketing",
                    "release", "reception", "merchandising", "awards", "sequel"]

    if document["title"].lower().strip().endswith("film)"):
        return True
    if any([anchor in film_anchors for anchor in document["anchors"]]):
        return True


def get_title_and_year(document):
    title = document["title"].strip().lower()
    return title, 1999


def load_wiki_data(con, wiki_file):
    tree = ET.iterparse(wiki_file)
    document = {}
    items = 0
    for event, elem in tree:
        if elem.tag == "doc":
            title, url, abstract = document["title"].replace("Wikipedia: ", ""), document["url"], document["abstract"]
            con.execute("INSERT INTO wiki(title, url, abstract) values (?, ?, ?)", (title, url, abstract))
            document = {}
            elem.clear()
            items += 1
            if items == 100000:
                items = 0
                print("written 100000 rows")
                con.commit()
        elif elem.tag in ["title", "url", "abstract"]:
            document[elem.tag] = elem.text
    con.commit()


load_film_data(con, "/home/giulio/Documents/TrueLayer/archive/movies_metadata.csv")


load_wiki_data(con, "/home/giulio/Documents/TrueLayer/enwiki-latest-abstract.xml")
