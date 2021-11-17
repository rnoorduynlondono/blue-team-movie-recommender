import streamlit as st
import ibm_db, ibm_db_dbi
import itertools
import numpy as np
import pandas as pd
import requests

MODEL_HOST = "model"
MODEL_PORT = "8000"

DATABASE = "bludb"
HOSTNAME = "c768e784-fa28-48c0-8cd8-fe229292aa98.bs2io90l08kqb1od8lcg.databases.appdomain.cloud"
PORT = 31742
UID = "ebd0118f"
PWD = None
CERT = "ssl.cert"


@st.experimental_singleton
def get_connection():
    dsn = (
        "DATABASE={};"
        "HOSTNAME={};"
        "PORT={};"
        "PROTOCOL=TCPIP;"
        "UID={};"
        "PWD={};"
        "SECURITY=SSL;"
        "SSLServerCertificate={}"
    ).format(DATABASE, HOSTNAME, PORT, UID, PWD, CERT,)

    return ibm_db_dbi.Connection(ibm_db.connect(dsn, "", ""))


@st.experimental_memo
def get_movies(_conn):
    query = """
    SELECT
        MOVIE_ID,
        TITLE,
        GENRES
    FROM RAWDATA.MOVIES
    """
    data = (
        pd.read_sql_query(query, con=_conn)
        .pipe(lambda f: pd.concat([f, series_to_dummies(f.GENRES)], axis=1))
        .drop(columns=["GENRES"])
    )

    return data


def series_to_dummies(series, sep="|"):

    split_values = [x.split("|") for x in series]
    unique = set(itertools.chain.from_iterable(split_values))
    keys = sorted(unique)

    key_to_idx = {k: i for i, k in enumerate(keys)}
    data = np.zeros(shape=(len(series), len(keys)), dtype=np.int8)

    for i, vals in enumerate(split_values):
        for v in vals:
            j = key_to_idx[v]

            data[i, j] = 1

    return pd.DataFrame(data=data, columns=keys)


@st.experimental_memo
def get_users(_conn):
    query = """
    SELECT
        USER_ID,
        GENDER,
        ZIPCODE,
        AGE,
        AGE_DESC,
        OCCUPATION,
        OCC_DESC
    FROM RAWDATA.USERS
    """

    data = pd.read_sql_query(query, con=_conn)
    return data


@st.experimental_memo
def get_ratings(user_id, _conn):
    query = f"""
    SELECT
        USER_ID,
        MOVIE_ID,
        RATING
    FROM RAWDATA.RATINGS
    WHERE USER_ID = {user_id}
    """
    data = pd.read_sql_query(query, con=_conn)
    return data


st.title("Movie Recommender System \n\n\n")

col1, col2, col3 = st.columns([1, 3, 1])
with col1:
    st.write("")

with col2:
    st.image("images/video-player.png", width=350)

with col3:
    st.write("")

st.markdown(
    "\n Please select three of your favourite movies, so that we can recommend your next favourite! \n\n"
)


conn = get_connection()
movie_df = get_movies(conn)
users_df = get_users(conn)

all_users = users_df.USER_ID.to_list()
all_age_groups = sorted(users_df.AGE_DESC.unique().tolist())
all_movies = movie_df.TITLE.to_list()

user = st.selectbox("User", all_users)
age = st.selectbox("Age group", all_age_groups)
movie1 = st.selectbox("Movie #1", all_movies)
movie2 = st.selectbox("Movie #2", all_movies)
movie3 = st.selectbox("Movie #3", all_movies)

user_ratings = (
    get_ratings(user, conn)
    .sort_values("RATING", ascending=False)
    .merge(movie_df, on="MOVIE_ID")
)

favourite_genres = (
    user_ratings.pipe(lambda f: f.loc[f.RATING >= 4])
    .iloc[:, 4:]
    .sum(axis=0)
    .squeeze()
    .sort_values(ascending=False)
)

ids = movie_df.loc[movie_df.TITLE.isin([movie1, movie2, movie3])].MOVIE_ID.to_list()

json = {
    "user": {"gender": "M", "age": "Under 18",},
    "movies": {"ids": ids},
}

st.text("")

if st.button("recommend"):

    st.subheader("You might like:")
    st.write("")
    st.write("")
    st.write("")

    r = requests.get(f"http://{MODEL_HOST}:{MODEL_PORT}/recommendation", json=json)

    recommendations = r.json()

    cols = st.columns(5)

    for i, movie in enumerate(recommendations["recommendation"]):

        with cols[i]:
            st.image(f"images/movies{i}.png", width=130)
            st.subheader(movie["movie"])
