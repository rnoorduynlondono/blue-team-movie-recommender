from typing import Optional, List
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class User(BaseModel):
    gender: str
    age: int


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
            {"id": 0, "movie": "a"},
            {"id": 1, "movie": "b"},
            {"id": 2, "movie": "c"},
        ]
    }
