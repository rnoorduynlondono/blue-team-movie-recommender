from typing import Optional, List
from fastapi import FastAPI
from pydantic import BaseModel
from recommender import RecommenderMarco

RMD = RecommenderMarco("./data")

app = FastAPI()


class User(BaseModel):
    user_id: int
    age: str


class Movies(BaseModel):
    ids: List[int]


class Request(BaseModel):
    user: User
    movies: Movies


# here we are getting the recommendation
@app.get("/recommendation/")
def read_item(q: Request):
    return RMD.predict(user_id=None,
                       age_group=q.user.age,
                       list_of_movies=q.movies.ids,
                       n_predictions=10
)
