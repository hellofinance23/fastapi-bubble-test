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
            print("No file received")
            return {"success": False, "error": "No file received (Bubble init call)"}

        print(f"Received file: {file.filename}, content_type: {file.content_type}")
        contents = await file.read()

        # Try reading with pandas
        df = pd.read_excel(io.BytesIO(contents))
        print(f"DataFrame loaded with {len(df)} rows")

        return {"success": True, "rows": len(df), "data": df.to_dict(orient="records")}

    except Exception as e:
        print("Exception occurred:", e)
        traceback.print_exc()
        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}
