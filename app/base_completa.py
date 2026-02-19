from __future__ import annotations

from datetime import datetime
import traceback

from .rodar import main as rodar_pipeline
from .organizar_arquivos import main as organizar_arquivos


def main():
    print("==========================================")
    print("🚀 BASE COMPLETA - INICIANDO")
    print("==========================================")
    print("⏰ Início:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print()

    try:
        print("🔹 ETAPA 1 - IMPORTAÇÃO + EXPORTAÇÃO")
        rodar_pipeline()
    except Exception as e:
        print("❌ ERRO NA ETAPA 1 (rodar_pipeline)")
        print(e)
        traceback.print_exc()

    print()
    print("🔹 ETAPA 2 - ORGANIZAÇÃO DE ARQUIVOS")
    try:
        organizar_arquivos()
    except Exception as e:
        print("❌ ERRO NA ETAPA 2 (organizar_arquivos)")
        print(e)
        traceback.print_exc()

    print()
    print("==========================================")
    print("✅ BASE COMPLETA FINALIZADA")
    print("⏰ Fim:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("==========================================")


if __name__ == "__main__":
    main()
