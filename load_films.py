import argparse
import ast
import re
import xml.etree.cElementTree as ET
from datetime import datetime
from functools import partial
from typing import Tuple, Optional
from xml.etree.cElementTree import Element

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine


def str_to_num(typ, x):
    try:
        return typ(x)
    except:
        return 0


str_to_int, str_to_float = partial(str_to_num, int), partial(str_to_num, float)


def get_prod_company(companies):

    try:
        return ast.literal_eval(companies)[0]["name"]
    except Exception as e:
        return ""


def load_film_df(film_file: str) -> DataFrame:
    """
    :param film_file: path for file movies_metadata.csv
    :return: dataframe with film data with columns: title, budget, revenue, rating, year, production_company, ratio, wiki_link (empty), abstract (empty)
    """
    print("--- LOADING IMDB DATA ---")
    columns = ["title", "budget", "revenue", "vote_average", "release_date", "production_companies"]
    converters = {
        "revenue": str_to_int,
        "budget": str_to_int,
        "vote_average": str_to_float,
    }
    df = pd.read_csv(film_file, usecols=columns, converters=converters)

    # Some films do not have either "revenue" or "budget" values, or they are not integers. Since we need to compute the
    # top 1000 films based on their ratio, we can discard these entries.
    df = df[(df["revenue"] > 0) & (df["budget"] > 0)]
    df["ratio"] = df["budget"] / df["revenue"]

    df["year"] = df["release_date"].apply(lambda date: datetime.strptime(date, "%Y-%m-%d").year)

    # For each movie there are multiple companies listed. Since only one company is requested, we pick
    # the first one.
    df["production_company"] = df["production_companies"].apply(get_prod_company)

    df = df.drop(columns=["release_date", "production_companies"]).rename(columns={'vote_average': 'rating'})
    df["wiki_link"] = ""
    df["abstract"] = ""
    df.sort_values(by=["ratio"], ascending=False, inplace=True)
    print("--- IMDB DATA LOADED ---")
    return df.head(1000)


def is_film(element: Element) -> bool:
    """
    :param element: "doc" element from wikimedia xml dump
    :return: True if the "doc" is a good film candidate, false otherwise

    This function implements an heuristic to determine if a given wiki page is or not a film. Film pages can be detected
    if their title ends with "film)" or if they contain certain keywords in their subsections.

    Note: this method has a lot of false positives, that will be handled downstream.
    """
    film_paragraphs = ["Plot", "Cast", "Production", "Soundtrack", "Marketing",
                    "Release", "Reception", "Merchandising", "Awards", "Sequel", "Synopsis"]

    title = element.find("title").text
    paragraphs = [sublink.find("anchor").text for sublink in element.find("links").findall("sublink")]
    if title.strip().endswith("film)"):
        return True
    return any([par in film_paragraphs for par in paragraphs])


def get_title_and_year(element: Element) -> Tuple[str, Optional[int]]:
    """
    :param element: "doc" element from wikimedia xml dump
    :return: clean title of the element, and year of release if the title is of type 3)

    There are 4 kinds of titles for wikipedia film pages:
    1) Just title eg. https://en.wikipedia.org/wiki/Toy_Story
    2) Title (film) eg. https://en.wikipedia.org/wiki/Smoke_(film)
    3) Title (<YEAR> film) eg. https://en.wikipedia.org/wiki/Drive_(2011_film)
    4) Title (<YEAR> <COMPANY/COUNTRY> film) eg. https://en.wikipedia.org/wiki/Black_Gold_(2011_Qatari_film)

    Case 3) happens when multiple films with the same title exist. In this case we use the year to query our film dataframe
     to find the correct one.
    Case 4) happens when two films with the same title are released on the same year. In this case there is no way to find
     a correct match with the film dataframe, so these are discarded.
    """
    title = element.find("title").text.replace("Wikipedia: ", "").strip()
    year_regex = r"^.* \((\d{4}) film\)$"
    year = None
    if re.match(year_regex, title):
        year = int(re.search(year_regex, title).group(1))
        title = re.sub(r" \(\d{4} film\)$", "", title)
    elif title.endswith(" (film)"):
        title = title.replace(" (film)", "")

    return title, year


def load_wiki_data(wiki_file: str, df: DataFrame) -> None:
    """
    :param wiki_file: path of enwiki-latest-abstract.xml file
    :param df: dataframe generated with load_film_df function
    :return: None (the dataframe is modified inplace)
    """
    print("--- READING WIKIMEDIA DATA ---")
    tree = ET.iterparse(wiki_file)
    elems = 0
    matches = 0
    for event, elem in tree:
        if elem.tag == "doc":
            if is_film(elem):
                title, year = get_title_and_year(elem)
                film = df[df["title"] == title]
                if year:
                    film = film[film["year"] == year]
                if len(film) >= 1:
                    idx = film.index[0]
                    df.at[idx, "wiki_url"] = elem.find("url").text
                    df.at[idx, "abstract"] = elem.find("abstract").text
                    matches += 1
            elem.clear()
            elems += 1
            if elems % 10_000 == 0:
                print(f"Wiki pages read: {elems}")
                print(f"Matches: {matches}")
    print("--- FINISHED READING WIKIMEDIA DATA ---")


def load_to_db(df: DataFrame, name, psql_config):
    print("--- INSERTING DATA TO POSTGRES ---")
    engine = create_engine(psql_config)
    df.to_sql(name, engine, if_exists='replace', index=False)
    print("--- POSTGRES INSERT COMPLETE ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load wikipedia data to postgres.')
    parser.add_argument('--movies', help='Path for Imdb movies_metadata.csv file.', required=True)
    parser.add_argument('--wiki', help='Path for wikimedia dump file enwiki-latest-abstract.xml.', required=True)
    parser.add_argument('--psql_config', help='Postgres connection string e.g. postgresql://<USER>:<PASSWORD>@<HOST>:<PORT>/<DB>', required=True)
    args = parser.parse_args()

    film_df = load_film_df(args.movies)
    load_wiki_data(args.wiki, film_df)
    load_to_db(film_df, "films", args.psql_config)
