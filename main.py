from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
from typing import Optional
import traceback

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


@app.post("/excel-to-json")
async def excel_to_json(file: Optional[UploadFile] = File(None)):
    try:
        if file is None:
            return {
                "success": False,
                "error": "No file received (Bubble init call)"
            }

        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))

        return {
            "success": True,
            "rows": len(df),
            "data": df.to_dict(orient="records")
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }