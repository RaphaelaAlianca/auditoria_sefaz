from __future__ import annotations

from datetime import datetime

from .config import (
    PASTA_ENTRADA, PASTA_PROCESSADOS, PASTA_ERROS,
    DB_PATH,
    GESTAO_SPREADSHEET_ID,
    CREDENTIALS_FILE, SCOPES,
    ABA_RESUMO_PENDENCIAS, ABA_DETALHES, ABA_STATUS,
    MAX_LINHAS_EXPORT, MAX_LINHAS_DETALHES,
)

from .importar import importar_pasta
from .banco import conectar, iniciar
from .resumo import df_resumo_pendencias, df_detalhes
from .exportar import exportar_para_sheets


def main() -> None:
    PASTA_ENTRADA.mkdir(parents=True, exist_ok=True)
    PASTA_PROCESSADOS.mkdir(parents=True, exist_ok=True)
    PASTA_ERROS.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    print("🚀 PENDÊNCIAS 2.0 - Iniciando pipeline")
    print(f"📥 Entrada: {PASTA_ENTRADA}")
    print(f"🗄️ Banco:  {DB_PATH}")

    resumo_import = importar_pasta(PASTA_ENTRADA, PASTA_PROCESSADOS, PASTA_ERROS, DB_PATH)
    print("✅ Importação concluída:", resumo_import)

    con = conectar(str(DB_PATH))
    iniciar(con)

    df_res = df_resumo_pendencias(con)
    df_det = df_detalhes(con)

    con.close()

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = (
        f"{agora} | Arquivos total: {resumo_import['arquivos_total']} | "
        f"Importados: {resumo_import['arquivos_importados']} | "
        f"Linhas lidas: {resumo_import['linhas_lidas']} | "
        f"Inseridas: {resumo_import['linhas_inseridas']} | "
        f"Resumo: {len(df_res)} linhas | Detalhes: {len(df_det)} linhas"
    )

    if not GESTAO_SPREADSHEET_ID or "COLE_AQUI" in GESTAO_SPREADSHEET_ID:
        print("⚠️ Configure o GESTAO_SPREADSHEET_ID em app/config.py")
        print("STATUS:", status)
        return

    try:
        exportar_para_sheets(
            GESTAO_SPREADSHEET_ID,
            CREDENTIALS_FILE,
            SCOPES,
            aba_resumo_pendencias=ABA_RESUMO_PENDENCIAS,
            aba_detalhes=ABA_DETALHES,
            aba_status=ABA_STATUS,
            df_resumo=df_res,
            df_detalhes=df_det,
            texto_status=status,
            max_linhas_resumo=MAX_LINHAS_EXPORT,
            max_linhas_detalhes=MAX_LINHAS_DETALHES,
        )
        print("📤 Exportação concluída.")
    except Exception as e:
        print("⚠️ Exportação para Sheets falhou (pipeline continua).")
        print("ERRO:", e)

    print("STATUS:", status)


if __name__ == "__main__":
    main()
