from typing import Optional, List
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class User(BaseModel):
    gender: str
    age: str


class Movies(BaseModel):
    ids: List[int]


class Request(BaseModel):
    user: User
    movies: Movies


# here we are getting the recommendation
@app.get("/recommendation/")
def read_item(q: Request):
    return {
        "recommendation": [
            {"id": 0, "movie": "A walk to remember"},
            {"id": 1, "movie": "Lion King"},
            {"id": 2, "movie": "Forrest Gump"},
            {"id": 3, "movie": "Toy Story"},
            {"id": 4, "movie": "Titanic"},
        ]
    }
