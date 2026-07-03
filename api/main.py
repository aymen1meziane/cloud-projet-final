"""
NYC Mobility — API REST (couche Gold).

Expose les 3 datamarts Gold (Parquet sur S3) via une API FastAPI documentée
(Swagger à /docs, ReDoc à /redoc). Conçue pour un déploiement AWS :
les identifiants sont lus via la chaîne standard boto3 (rôle IAM sur EC2/ECS,
ou variables d'environnement / .env en local).

Lancement local :
    pip install -r requirements.txt
    uvicorn main:app --reload
    → http://localhost:8000/docs

Conteneur (AWS ECS / App Runner / EC2) :
    docker build -t nyc-mobility-api .
    docker run -p 8000:8000 --env-file .env nyc-mobility-api
"""

import os
import json
import time
from typing import Optional, List

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

BUCKET = os.getenv("GOLD_BUCKET", "nyc-mobility-gold-am2024")
DATAMARTS = {
    "rentabilite": f"s3://{BUCKET}/rentabilite/",
    "meteo_demande": f"s3://{BUCKET}/meteo_demande/",
    "anomalies": f"s3://{BUCKET}/anomalies/",
}
CACHE_TTL = int(os.getenv("CACHE_TTL", "3600"))
_cache: dict = {}


# ── Accès données (cache mémoire + TTL) ───────────────────────
def get_datamart(name: str) -> pd.DataFrame:
    now = time.time()
    hit = _cache.get(name)
    if hit and now - hit[1] < CACHE_TTL:
        return hit[0]
    df = pd.read_parquet(DATAMARTS[name], engine="pyarrow")
    _cache[name] = (df, now)
    return df


def to_records(df: pd.DataFrame) -> list:
    """DataFrame -> liste de dicts JSON-safe (NaN -> null, dates -> texte)."""
    safe = df.copy()
    for c in safe.columns:
        if pd.api.types.is_datetime64_any_dtype(safe[c]):
            safe[c] = safe[c].astype(str)
    return json.loads(safe.to_json(orient="records"))


def query_df(df, sort_by, order, limit, offset):
    total = len(df)
    if sort_by:
        if sort_by not in df.columns:
            raise HTTPException(400, f"Champ de tri inconnu : '{sort_by}'. "
                                     f"Colonnes valides : {list(df.columns)}")
        df = df.sort_values(sort_by, ascending=(order == "asc"))
    return total, to_records(df.iloc[offset: offset + limit])


# ── Modèles (pour Swagger) ────────────────────────────────────
class RentabiliteRow(BaseModel):
    pickup_borough: Optional[str] = None
    pickup_zone: Optional[str] = None
    taxi_type: Optional[str] = None
    heure: Optional[int] = None
    nb_courses: Optional[int] = None
    revenu_moyen: Optional[float] = None
    revenu_total: Optional[float] = None
    distance_moyenne: Optional[float] = None


class MeteoRow(BaseModel):
    pickup_borough: Optional[str] = None
    tranche_pluie: Optional[str] = None
    tranche_temp: Optional[str] = None
    nb_courses: Optional[int] = None
    revenu_moyen: Optional[float] = None
    moy_signalements_311: Optional[float] = None


class AnomalieRow(BaseModel):
    taxi_type: Optional[str] = None
    pickup_zone: Optional[str] = None
    dropoff_zone: Optional[str] = None
    pickup_datetime: Optional[str] = None
    trip_distance: Optional[float] = None
    total_amount: Optional[float] = None
    moy_tarif: Optional[float] = None
    z_score: Optional[float] = None


class RentabilitePage(BaseModel):
    total: int
    limit: int
    offset: int
    count: int
    items: List[RentabiliteRow]


class MeteoPage(BaseModel):
    total: int
    limit: int
    offset: int
    count: int
    items: List[MeteoRow]


class AnomaliePage(BaseModel):
    total: int
    limit: int
    offset: int
    count: int
    items: List[AnomalieRow]


# ── Application ───────────────────────────────────────────────
app = FastAPI(
    title="NYC Mobility API",
    description="Exposition REST des datamarts Gold (rentabilité, météo, "
                "anomalies) du pipeline NYC Mobility. Données lues depuis S3.",
    version="1.0.0",
    contact={"name": "Projet RNCP36739 — NYC Mobility"},
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                   allow_headers=["*"])


@app.get("/", tags=["Général"], summary="Point d'entrée")
def root():
    return {"service": "NYC Mobility API", "version": "1.0.0",
            "documentation": "/docs", "datamarts": list(DATAMARTS.keys())}


@app.get("/health", tags=["Général"], summary="État de santé")
def health():
    sources, ok = {}, True
    for name in DATAMARTS:
        try:
            n = len(get_datamart(name))
            sources[name] = {"reachable": True, "rows": n}
        except Exception as e:
            sources[name] = {"reachable": False, "error": str(e)}
            ok = False
    return {"status": "ok" if ok else "degraded", "bucket": BUCKET,
            "sources": sources}


@app.get("/stats", tags=["Général"], summary="Statistiques globales")
def stats():
    r = get_datamart("rentabilite")
    m = get_datamart("meteo_demande")
    a = get_datamart("anomalies")
    courses = int(r["nb_courses"].sum())
    revenu = float(r["revenu_total"].sum())
    return {
        "rentabilite": {
            "lignes": len(r), "courses_totales": courses,
            "revenu_total": round(revenu, 2),
            "panier_moyen_global": round(revenu / courses, 2) if courses else 0,
            "zones": int(r["pickup_zone"].nunique()),
        },
        "meteo_demande": {"lignes": len(m),
                          "boroughs": int(m["pickup_borough"].nunique())},
        "anomalies": {"lignes": len(a),
                      "z_score_max": round(float(a["z_score"].abs().max()), 1),
                      "tarif_max": round(float(a["total_amount"].max()), 2)},
    }


@app.get("/rentabilite", response_model=RentabilitePage, tags=["Datamarts"],
         summary="Rentabilité par zone / créneau / type de taxi")
def rentabilite(
    borough: Optional[str] = Query(None, description="Filtre borough de départ"),
    taxi_type: Optional[str] = Query(None, description="yellow ou green"),
    heure_min: int = Query(0, ge=0, le=23),
    heure_max: int = Query(23, ge=0, le=23),
    sort_by: str = Query("revenu_total"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    df = get_datamart("rentabilite")
    df = df[df["heure"].between(heure_min, heure_max)]
    if borough:
        df = df[df["pickup_borough"].str.lower() == borough.lower()]
    if taxi_type:
        df = df[df["taxi_type"].str.lower() == taxi_type.lower()]
    total, items = query_df(df, sort_by, order, limit, offset)
    return {"total": total, "limit": limit, "offset": offset,
            "count": len(items), "items": items}


@app.get("/meteo-demande", response_model=MeteoPage, tags=["Datamarts"],
         summary="Impact météo & 311 sur la demande")
def meteo_demande(
    borough: Optional[str] = Query(None),
    tranche_pluie: Optional[str] = Query(None,
                                         description="1_sec, 2_pluie_faible, 3_pluie_forte"),
    tranche_temp: Optional[str] = Query(None, description="1_gel, 2_froid, 3_doux"),
    sort_by: str = Query("nb_courses"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    df = get_datamart("meteo_demande")
    if borough:
        df = df[df["pickup_borough"].str.lower() == borough.lower()]
    if tranche_pluie:
        df = df[df["tranche_pluie"] == tranche_pluie]
    if tranche_temp:
        df = df[df["tranche_temp"] == tranche_temp]
    total, items = query_df(df, sort_by, order, limit, offset)
    return {"total": total, "limit": limit, "offset": offset,
            "count": len(items), "items": items}


@app.get("/anomalies", response_model=AnomaliePage, tags=["Datamarts"],
         summary="Anomalies tarifaires (z-score)")
def anomalies(
    z_score_min: float = Query(3.0, ge=0, description="Seuil |z-score|"),
    taxi_type: Optional[str] = Query(None),
    pickup_zone: Optional[str] = Query(None, description="Filtre partiel sur la zone"),
    sort_by: str = Query("z_score"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    df = get_datamart("anomalies")
    df = df[df["z_score"].abs() >= z_score_min]
    if taxi_type:
        df = df[df["taxi_type"].str.lower() == taxi_type.lower()]
    if pickup_zone:
        df = df[df["pickup_zone"].str.contains(pickup_zone, case=False, na=False)]
    total, items = query_df(df, sort_by, order, limit, offset)
    return {"total": total, "limit": limit, "offset": offset,
            "count": len(items), "items": items}