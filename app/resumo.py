import sqlite3
import pandas as pd

def df_resumo_pendencias(con: sqlite3.Connection) -> pd.DataFrame:
    """
    cnpj | cgf | razao | tipo_pendencia | periodo | qtd | valor_total | ultima_coleta
    """
    sql = """
    SELECT
      COALESCE(cnpj,'') AS cnpj,
      COALESCE(cgf,'') AS cgf,
      COALESCE(razao,'') AS razao,
      COALESCE(tipo_pendencia,'') AS tipo_pendencia,
      COALESCE(periodo,'') AS periodo,
      COUNT(*) AS qtd,
      ROUND(SUM(COALESCE(valor,0)), 2) AS valor_total,
      MAX(data_coleta) AS ultima_coleta
    FROM pendencias_raw
    WHERE COALESCE(tipo_pendencia,'') <> ''
    GROUP BY cnpj, cgf, razao, tipo_pendencia, periodo
    ORDER BY ultima_coleta DESC, qtd DESC;
    """
    return pd.read_sql_query(sql, con)


def df_detalhes(con: sqlite3.Connection) -> pd.DataFrame:
    """
    Todas as linhas classificadas, linha por linha (para conferência)
    """
    sql = """
    SELECT
      COALESCE(cnpj,'') AS cnpj,
      COALESCE(cgf,'') AS cgf,
      COALESCE(razao,'') AS razao,
      COALESCE(tipo_pendencia,'') AS tipo_pendencia,
      COALESCE(periodo,'') AS periodo,
      COALESCE(valor,0) AS valor,
      COALESCE(detalhe,'') AS detalhe,
      COALESCE(data_referencia,'') AS data_referencia,
      COALESCE(arquivo_origem,'') AS arquivo_origem,
      COALESCE(aba_origem,'') AS aba_origem,
      COALESCE(linha_origem,'') AS linha_origem,
      COALESCE(data_coleta,'') AS ultima_coleta
    FROM pendencias_raw
    WHERE COALESCE(tipo_pendencia,'') <> ''
    ORDER BY data_coleta DESC, arquivo_origem, aba_origem, linha_origem;
    """
    return pd.read_sql_query(sql, con)
