import os
from fastapi import FastAPI, Header, HTTPException

# Importa o seu pipeline
from app.rodar import main as rodar_pipeline

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

    # Se o seu código lê credenciais por caminho fixo, a gente ajusta depois.
    rodar_pipeline()
    return {"ok": True, "message": "Pipeline executada"}
