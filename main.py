from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse, FileResponse
import uuid
from database import SessionLocal, engine, Base, Report
from utils import load_csv_data, generate_report
import os

print("Starting server...")
app = FastAPI()

print("Creating database tables...")
Base.metadata.create_all(bind=engine)

print("Loading CSV data...")
load_csv_data()

@app.post("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks):
    report_id = str(uuid.uuid4())
    db = SessionLocal()
    new_report = Report(id=report_id, status="Running")
    db.add(new_report)
    db.commit()
    db.close()
    
    background_tasks.add_task(generate_report, report_id)
    return {"report_id": report_id}

@app.get("/get_report/{report_id}")
async def get_report(report_id: str):
    # Remove any curly braces from the report_id
    report_id = report_id.strip('{}')
    
    db = SessionLocal()
    report = db.query(Report).filter(Report.id == report_id).first()
    db.close()
    
    if not report:
        return JSONResponse(
            status_code=202,
            content={"status": "Running"}
        )
    
    if report.status == "Running":
        return JSONResponse(
            status_code=202,
            content={"status": "Running"}
        )
    elif report.status == "Complete":
        file_path = f"reports/{report_id}.csv"
        if os.path.exists(file_path):
            return JSONResponse(
                status_code=200,
                content={
                    "status": "Complete",
                    "report_url": f"/download_report/{report_id}"
                }
            )
        else:
            return JSONResponse(
                status_code=202,
                content={"status": "Running"}
            )
    else:
        return JSONResponse(
            status_code=500,
            content={"status": "Error", "detail": "An error occurred while generating the report"}
        )

@app.get("/download_report/{report_id}")
async def download_report(report_id: str):
    file_path = f"reports/{report_id}.csv"
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="text/csv", filename=f"report_{report_id}.csv")
    else:
        raise HTTPException(status_code=404, detail="Report file not found")

print("Server is ready!")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
