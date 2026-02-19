import os
import traceback
from fastapi import FastAPI, Header, HTTPException

app = FastAPI()

API_TOKEN = os.getenv("API_TOKEN", "")
CREDS_PATH = os.getenv("CREDS_PATH", "/etc/secrets/credenciais.json")

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/run")
def run(x_api_key: str | None = Header(default=None)):
    if not API_TOKEN or x_api_key != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not os.path.exists(CREDS_PATH):
        raise HTTPException(status_code=500, detail=f"Credenciais não encontradas em {CREDS_PATH}")

    try:
        # Importa o pipeline só na hora de rodar (não derruba o uvicorn no boot)
        from app.rodar import main as rodar_pipeline
        rodar_pipeline()
        return {"ok": True, "message": "Pipeline executada"}
    except Exception as e:
        tb = traceback.format_exc()
        raise HTTPException(status_code=500, detail=f"{e}\n{tb}")
