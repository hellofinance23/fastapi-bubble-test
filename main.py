from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io

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
async def excel_to_json(file: UploadFile = File(...)):
    """
    Upload an Excel file and get back JSON data
    """
    try:
        # Validate file type
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400, 
                detail="Only Excel files (.xlsx, .xls) are allowed"
            )
        
        # Read the uploaded file
        contents = await file.read()
        
        # Parse Excel into DataFrame
        df = pd.read_excel(io.BytesIO(contents))
        
        # Convert DataFrame to JSON-friendly format
        # Option 1: Records format (list of objects)
        data = df.to_dict(orient='records')
        
        # Return JSON response
        return {
            "success": True,
            "filename": file.filename,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "data": data
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error processing file: {str(e)}"
        )