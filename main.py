from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Enable CORS so Bubble can access it
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "API is running!"}

@app.get("/hello")
def say_hello():
    return {"message": "Hello World"}

@app.get("/hello/{name}")
def say_hello_to(name: str):
    return {"message": f"Hello {name}!"}