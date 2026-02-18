import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PASTA_ENTRADA = ROOT / "entrada"
PASTA_PROCESSADOS = ROOT / "processados"
PASTA_ERROS = ROOT / "erros"

PASTA_BANCO = ROOT / "banco"
DB_PATH = PASTA_BANCO / "pendencias.db"

# Planilha do gestor (ID)
GESTAO_SPREADSHEET_ID = os.getenv("GESTAO_SPREADSHEET_ID", "1nKE8OFW7bprwToe5sOQFCLKlSI40bMsaAIGMRIQNPow")

# Credenciais (você disse que está dentro de banco/)
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", str(ROOT / "banco" / "credenciais.json"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ✅ SOMENTE 3 ABAS
ABA_RESUMO_PENDENCIAS = "RESUMO_PENDENCIAS"
ABA_DETALHES = "DETALHES"
ABA_STATUS = "STATUS"

MAX_LINHAS_EXPORT = 50000
MAX_LINHAS_DETALHES = 200000  # detalhes pode ser grande
