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
            return {"success": False, "error": "No file received (Bubble init call)"}

        # Log file info
        print(f"Received file: {file.filename}, content_type: {file.content_type}")

        # Check for empty file
        contents = await file.read()
        if not contents:
            return {"success": False, "error": "File is empty"}

        # Check supported Excel types
        if file.content_type not in [
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel"
        ]:
            return {"success": False, "error": f"Unsupported file type: {file.content_type}"}

        # Try reading the Excel file
        try:
            df = pd.read_excel(io.BytesIO(contents))
        except Exception as e:
            return {"success": False, "error": f"Failed to read Excel: {str(e)}"}

        # Return clean JSON
        return {"success": True, "rows": len(df), "data": df.to_dict(orient="records")}

    except Exception as e:
        # Catch everything else
        print("Unexpected exception:", e)
        traceback.print_exc()
        return {"success": False, "error": f"Unexpected error: {str(e)}", "traceback": traceback.format_exc()}