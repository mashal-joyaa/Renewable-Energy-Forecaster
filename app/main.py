from enum import Enum
from typing import List, Optional

from fastapi import FastAPI, UploadFile, File, Form, HTTPException

from services.universal_pipeline import UniversalPipeline

app = FastAPI()
pipeline = UniversalPipeline()


class UploadMode(str, Enum):
    single = "single"
    multi = "multi"


class MarketFormat(str, Enum):
    aeso = "aeso"
    ieso = "ieso"


class Province(str, Enum):
    alberta = "alberta"
    ontario = "ontario"
    other = "other"


DEFAULT_IESO_CITY = "Goderich"
DEFAULT_AESO_CITY = "Red Deer"


@app.get("/run-ieso")
def run_ieso():
    return pipeline.run_market("ieso", city=DEFAULT_IESO_CITY)


@app.get("/run-aeso")
def run_aeso():
    return pipeline.run_market("aeso", city=DEFAULT_AESO_CITY)


@app.post("/run-upload")
def run_upload(
    upload_mode: UploadMode = Form(...),
    market_format: MarketFormat = Form(...),
    province: Province = Form(...),
    other_city: Optional[str] = Form(None),
    files: List[UploadFile] = File(...),
):
    # Determine file format automatically
    file_format = "csv" if market_format == MarketFormat.aeso else "xml"

    # Province → City mapping
    if province == Province.ontario:
        final_city = DEFAULT_IESO_CITY
    elif province == Province.alberta:
        final_city = DEFAULT_AESO_CITY
    else:
        if not other_city:
            raise HTTPException(status_code=400, detail="Please enter your city when selecting 'other'.")
        final_city = other_city

    # File count validation
    if upload_mode == UploadMode.single and len(files) != 1:
        raise HTTPException(status_code=400, detail="Single-file mode requires exactly 1 file.")
    if upload_mode == UploadMode.multi and len(files) < 2:
        raise HTTPException(status_code=400, detail="Multi-file mode requires at least 2 files.")

    try:
        return pipeline.run_market(
            "upload",
            city=final_city,
            upload_mode=upload_mode.value,
            file_format=file_format,
            files=files,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
