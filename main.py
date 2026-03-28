from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
import pandas as pd
from fastapi.responses import StreamingResponse, FileResponse
import io
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="."), name="static")

client = bigquery.Client(project="ceden-stations")
DATASET = "ceden_dataset"
TABLE = "tisue_table"

def query_bq(sql: str) -> pd.DataFrame:
    return client.query(sql).to_dataframe()

@app.get("/")
def root():
    return FileResponse("index.html")

# --- GET STATIONS ---
@app.get("/stations")
def get_stations():
    """Return all stations with coordinates for the map."""
    sql = f"""
        SELECT DISTINCT CompositeStationName, CompositeLatitude, CompositeLongitude
        FROM `{DATASET}.{TABLE}`
        WHERE CompositeLatitude IS NOT NULL AND CompositeLongitude IS NOT NULL
    """
    df = query_bq(sql)
    return df.rename(
        columns={
            "CompositeStationName": "name",
            "CompositeLatitude": "lat",
            "CompositeLongitude": "lon"
        }
    ).to_dict(orient="records")


# --- GET STATION DATA + SUMMARY (with optional filters) ---
@app.get("/station-data")
def get_station_data(
    names: str = Query(...),
    common_name: str = Query(None),
    composite_id: str = Query(None),
    per_station: int = 200
):
    station_list = [n.strip() for n in names.split(",")]
    if not station_list:
        return {"summary": [], "records": []}

    stations_sql = ", ".join(f"'{s}'" for s in station_list)
    sql = f"SELECT * FROM `{DATASET}.{TABLE}` WHERE CompositeStationName IN ({stations_sql})"

    df = query_bq(sql)

    # Apply Common Name filter
    if common_name:
        df = df[df['CompositeCommonName'].str.upper().str.contains(common_name.upper())]

    # Apply Composite ID filter
    if composite_id:
        df = df[df['CompositeCompositeID'].str.upper().str.contains(composite_id.upper())]

    # Keep only numeric Result values
    df = df[pd.to_numeric(df['Result'], errors='coerce').notna()]
    df['Result'] = df['Result'].astype(float)

    # Summary for PCB, Mercury, Cadmium, DDT
    analytes = ['PCB', 'Mercury', 'Cadmium', 'DDT']
    summary_rows = []
    for ag in analytes:
        temp = df[df['Analyte_Group'].str.upper().str.strip() == ag.upper()]
        if not temp.empty:
            mean_val = round(temp['Result'].mean(), 2)
            min_val = round(temp['Result'].min(), 2)
            max_val = round(temp['Result'].max(), 2)
        else:
            mean_val = min_val = max_val = None
        summary_rows.append({
            "Analyte_Group": ag,
            "mean_result": mean_val if mean_val is not None else "N/A",
            "min_result": min_val if min_val is not None else "N/A",
            "max_result": max_val if max_val is not None else "N/A"
        })

    # Apply per-station limit for detailed records
    records = []
    for station in station_list:
        station_df = df[df["CompositeStationName"] == station].head(per_station)
        for _, row in station_df.iterrows():
            record = {col: val for col, val in row.items() if pd.notna(val) and val not in [float("inf"), float("-inf")]}
            records.append(record)

    return {"summary": summary_rows, "records": records}


# --- DOWNLOAD CSV ---
@app.get("/download-station-data")
def download_station_data(names: str = Query(...),
                          common_name: str = Query(None),
                          composite_id: str = Query(None)):
    station_list = [n.strip() for n in names.split(",")]
    if not station_list:
        return StreamingResponse(io.StringIO(""), media_type="text/csv")

    stations_sql = ", ".join(f"'{s}'" for s in station_list)
    sql = f"SELECT * FROM `{DATASET}.{TABLE}` WHERE CompositeStationName IN ({stations_sql})"
    df = query_bq(sql)

    if common_name:
        df = df[df['CompositeCommonName'].str.upper().str.contains(common_name.upper())]

    if composite_id:
        df = df[df['CompositeCompositeID'].str.upper().str.contains(composite_id.upper())]

    stream = io.StringIO()
    df.to_csv(stream, index=False)
    stream.seek(0)
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=stations.csv"}
    )