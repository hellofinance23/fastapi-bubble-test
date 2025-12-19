from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
from typing import Optional
import traceback
import requests
from pydantic import BaseModel

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


class FileUrl(BaseModel):
    file_url: str

@app.post("/excel-url-to-json")
async def excel_url_to_json(payload: FileUrl):
    try:
        url = payload.file_url
        print(f"Received file URL: {url}")

        # Download the file
        response = requests.get(url)
        if response.status_code != 200:
            return {"success": False, "error": f"Failed to download file: status {response.status_code}"}

        # Read Excel from bytes
        try:
            df = pd.read_excel(io.BytesIO(response.content))
        except Exception as e:
            return {"success": False, "error": f"Failed to read Excel: {str(e)}"}

        return {"success": True, "rows": len(df), "data": df.to_dict(orient="records")}

    except Exception as e:
        print("Unexpected exception:", e)
        traceback.print_exc()
        return {"success": False, "error": f"Unexpected error: {str(e)}", "traceback": traceback.format_exc()}