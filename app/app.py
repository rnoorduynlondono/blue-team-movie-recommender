import os
import streamlit as st
import ibm_db, ibm_db_dbi
import itertools
import numpy as np
import pandas as pd
import requests

CERTIFICATE = """-----BEGIN CERTIFICATE-----
MIIDHTCCAgWgAwIBAgIUOwo0/okOBPCyF1VxRqThJEonl0UwDQYJKoZIhvcNAQEL
BQAwHjEcMBoGA1UEAwwTSUJNIENsb3VkIERhdGFiYXNlczAeFw0yMDA4MDQwMjU3
MjZaFw0zMDA4MDIwMjU3MjZaMB4xHDAaBgNVBAMME0lCTSBDbG91ZCBEYXRhYmFz
ZXMwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDoFE4d4HgNyvLQR0Gx
A0tjdWBs85PcL3rE+c7Tw+gbEGPILISEYWz8cX5LmWBM+cQg8oUy++Arw8J1iwQe
rJiHSb5rQxY34spPxdETFdXHRrra0e6Vc81n6NYI/FGJyuCxkLnF1KEAoalv0h36
xCOAoqtpNQkO3i14FyM2D4bj91rB8Di8W/W1ZUuXL4l3euKeEBy4nfhIWy2IW7iC
ljLgte7xYL5GmZJ9GlakkJrurzMDAP/5TbteQB2tIhu0QITEdyDIQXPFFD0Gc9hd
73oIvzUeBwT/nDsw92M4/6JGmejJ7IitPS7v6kgeQXH49AiEI5zPuEiW3Nb/FGJX
f6kbAgMBAAGjUzBRMB0GA1UdDgQWBBTvG6vENLF1UmfgCM72lNrc3H26mDAfBgNV
HSMEGDAWgBTvG6vENLF1UmfgCM72lNrc3H26mDAPBgNVHRMBAf8EBTADAQH/MA0G
CSqGSIb3DQEBCwUAA4IBAQA8/tUgQ6ei6XXvgt2tuGknJokV9Qcdi3YlUEZCCS+c
UPgsg2uA2WqpyVNmfFHcpvuVjtTtXNi65C6ZVlFv1szuqOstPynBxnSxqK4vG4u9
V0VEH1pMmfvAJldWw8PBSdbmNMGtc8K9pOJ99WACVEEuWTgCxrJMqAfzXQybuEtw
tqmiWk0NeW4i9dF8KgSYEZAahuuAJTeupvGdOWU4xExnm7hEQnfOWfHM8CwO15aY
TFCk4CJCRdx2X9So8Wz5gs3rw2FACBRrgCXxQCfvke6TuSG6LEDrGnjViuRBJYun
OTqYtNiPGhznLrk/AsjmK30qBaKNar5GPj8jjZModbgN
-----END CERTIFICATE-----"""

# This should be an env variable
MODEL_HOST = os.getenv("MODEL_HOST")
MODEL_PORT = os.getenv("MODEL_PORT")

DATABASE = os.getenv("DB_NAME")
HOSTNAME = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
UID = os.getenv("DB_UID")
PWD = os.getenv("DB_PWD", "GseChTYMoo1yx8Uk")
CERT = os.getenv("DB_CERT", CERTIFICATE)

with open("ssl.cert", "w") as f:
    f.write(CERT)


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
    ).format(
        DATABASE,
        HOSTNAME,
        PORT,
        UID,
        PWD,
        "ssl.cert",
    )

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
        .set_index("MOVIE_ID")
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


def get_movie_title(movies_dataframe, movie_id):
    return movies_dataframe.loc[movie_id, "TITLE"]


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

ids = movie_df.loc[movie_df.TITLE.isin([movie1, movie2, movie3])].index.to_list()

# json with user information that we need to send with the request
json = {
    "user": {"user_id": user, "age": age},
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

    for i, movie in enumerate(recommendations["recommendation"][:5]):

        with cols[i]:
            st.image(f"images/movies{i}.png", width=130)
            st.subheader(get_movie_title(movie_df, movie["id"]))
