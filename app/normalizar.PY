import re
from typing import Dict, Any, Optional

def _digits(v: Any) -> str:
    if v is None:
        return ""
    return re.sub(r"\D", "", str(v))

def limpar_cnpj_raiz(v: Any) -> str:
    return _digits(v)

def limpar_cgf(v: Any) -> str:
    return _digits(v)

def texto(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()

def numero(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def periodo_from_data(v: Any) -> str:
    """
    Converte:
      2025-02-01 -> 2025-02
      Timestamp -> 2025-02
      8/2025 -> 2025-08
    """
    if v is None:
        return ""

    try:
        import pandas as pd
        if isinstance(v, pd.Timestamp):
            return f"{v.year:04d}-{v.month:02d}"
    except Exception:
        pass

    s = str(v).strip()
    if not s:
        return ""

    m = re.match(r"^(\d{1,2})/(\d{4})$", s)
    if m:
        mes = int(m.group(1))
        ano = int(m.group(2))
        if 1 <= mes <= 12:
            return f"{ano:04d}-{mes:02d}"

    m = re.search(r"(20\d{2})[-/](0[1-9]|1[0-2])", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    return ""

def _is_sim(v: Any) -> bool:
    s = texto(v).strip().upper()
    return s in ("SIM", "S", "TRUE", "YES", "1")

def normalizar_por_aba(aba: str, row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ajustado para os nomes EXATOS das abas:
      - Omissões de EFD
      - Débitos
      - Omissões e divergências de NFE
      - NFe inexistente declarada
      - Omissões e Divergências CFe
      - CTE escriturado com divergência
      - NFe sem REG_PAS
      - Outros limitadores
    """
    aba_lower = (aba or "").lower().strip()

    cnpj = limpar_cnpj_raiz(row.get("CNPJ RAIZ") or row.get("CNPJ"))
    cgf  = limpar_cgf(row.get("CGF"))
    razao = texto(row.get("RAZÃO") or row.get("RAZAO") or "")

    tipo = ""
    periodo = ""
    detalhe = ""
    valor = None
    data_ref = ""

    # -------------------------
    # 1) Omissões de EFD  -> EFD_OMISSAO (somente ENTREGA_EFD = Omisso)
    # -------------------------
    if "omissões de efd" in aba_lower or "omissoes de efd" in aba_lower:
        entrega = texto(row.get("ENTREGA_EFD")).upper()
        if "OMISS" in entrega:
            tipo = "EFD_OMISSAO"
            periodo = periodo_from_data(row.get("ANO_MES"))
            detalhe = (
                "DOCUMENTO=" + texto(row.get("DOCUMENTO")) +
                "; ENTREGA_EFD=" + texto(row.get("ENTREGA_EFD")) +
                "; ANO_MES=" + texto(row.get("ANO_MES"))
            )

    # -------------------------
    # 2) Débitos -> DEBITO (todos os registros da aba)
    # -------------------------
    elif "débitos" in aba_lower or "debitos" in aba_lower:
        tipo = "DEBITO"
        periodo = periodo_from_data(row.get("PERIODO DE REFERENCIA"))
        data_ref = texto(row.get("DATA VENCIMENTO"))

        total = numero(row.get("VALOR TOTAL"))
        valor = total

        cod = texto(row.get("CÓDIGO DE RECEITA DO DÉBITO") or row.get("CODIGO DE RECEITA DO DEBITO"))
        dias = texto(row.get("DIAS DE ATRASO DO DÉBITO NÃO PAGO") or row.get("DIAS DE ATRASO DO DEBITO NAO PAGO"))

        detalhe = (
            "COD_RECEITA=" + cod +
            "; VENC=" + data_ref +
            "; DIAS_ATRASO=" + dias +
            "; TOTAL=" + str(total)
        )

    # -------------------------
    # 3) Omissões e divergências de NFE -> NFE_DIVERGENCIA
    # -------------------------
    elif "omissões e divergências de nfe" in aba_lower or "omissoes e divergencias de nfe" in aba_lower:
        tipo = "NFE_DIVERGENCIA"
        periodo = periodo_from_data(row.get("MÊS ANO REFERÊNCIA") or row.get("MES ANO REFERENCIA"))

        chave = texto(row.get("CHAVE DFE"))
        num_doc = texto(row.get("NÚMERO DO DOCUMENTO FISCAL") or row.get("NUMERO DO DOCUMENTO FISCAL"))
        origem = texto(row.get("ORIGEM DO DOCUMENTO"))
        desc = texto(row.get("DESCRIÇÃO DO INDICADOR") or row.get("DESCRICAO DO INDICADOR"))

        val_escr = numero(row.get("VALOR ESCRITURADO"))
        val_dfe = numero(row.get("VALOR DO DFE"))
        dif = numero(row.get("DIFERENÇA") or row.get("DIFERENCA"))
        valor = dif if dif is not None else val_dfe

        detalhe = (
            desc +
            " | CHAVE=" + chave +
            " | NUM_DOC=" + num_doc +
            " | ORIGEM=" + origem +
            " | ESCR=" + str(val_escr) +
            " | DFE=" + str(val_dfe) +
            " | DIF=" + str(dif)
        )

    # -------------------------
    # 4) NFe inexistente declarada -> NFE_INEXISTENTE
    # -------------------------
    elif "nfe inexistente declarada" in aba_lower:
        tipo = "NFE_INEXISTENTE"
        chave = texto(row.get("CHAVE DFE"))
        valor = numero(row.get("VALOR DIVERGENTE"))
        detalhe = "CHAVE=" + chave + "; VALOR_DIVERGENTE=" + str(valor)

    # -------------------------
    # 5) Omissões e Divergências CFe -> CFE_DIVERGENCIA
    # -------------------------
    elif "cfe" in aba_lower:
        # pega tanto "Omissões e Divergências CFe" como variações
        tipo = "CFE_DIVERGENCIA"
        periodo = periodo_from_data(row.get("MÊS ANO REFERÊNCIA") or row.get("MES ANO REFERENCIA"))
        chave = texto(row.get("CHAVE DFE") or row.get("CHAVE"))
        desc = texto(row.get("DESCRIÇÃO DO INDICADOR") or row.get("DESCRICAO DO INDICADOR"))
        val_dfe = numero(row.get("VALOR DO DFE") or row.get("VALOR"))
        dif = numero(row.get("DIFERENÇA") or row.get("DIFERENCA"))
        valor = dif if dif is not None else val_dfe
        detalhe = (desc + " | CHAVE=" + chave).strip(" |")

    # -------------------------
    # 6) CTE escriturado com divergência -> CTE_DIVERGENCIA
    # -------------------------
    elif "cte" in aba_lower:
        tipo = "CTE_DIVERGENCIA"
        periodo = periodo_from_data(row.get("MÊS ANO REFERÊNCIA") or row.get("MES ANO REFERENCIA"))
        chave = texto(row.get("CHAVE DFE") or row.get("CHAVE"))
        desc = texto(row.get("DESCRIÇÃO DO INDICADOR") or row.get("DESCRICAO DO INDICADOR"))
        val_dfe = numero(row.get("VALOR DO DFE") or row.get("VALOR"))
        dif = numero(row.get("DIFERENÇA") or row.get("DIFERENCA"))
        valor = dif if dif is not None else val_dfe
        detalhe = (desc + " | CHAVE=" + chave).strip(" |")

    # -------------------------
    # 7) NFe sem REG_PAS -> REGISTRO_PASSAGEM
    # -------------------------
    elif "reg_pas" in aba_lower or "reg pas" in aba_lower:
        tipo = "REGISTRO_PASSAGEM"
        periodo = periodo_from_data(row.get("MÊS ANO REFERÊNCIA") or row.get("MES ANO REFERENCIA"))
        chave = texto(row.get("CHAVE DFE") or row.get("CHAVE"))
        num_doc = texto(row.get("NÚMERO DO DOCUMENTO FISCAL") or row.get("NUMERO DO DOCUMENTO FISCAL"))
        origem = texto(row.get("ORIGEM DO DOCUMENTO"))
        val_dfe = numero(row.get("VALOR DO DFE") or row.get("VALOR"))
        valor = val_dfe
        detalhe = (
            "CHAVE=" + chave +
            " | NUM_DOC=" + num_doc +
            " | ORIGEM=" + origem
        ).strip(" |")

    # -------------------------
    # 8) Outros limitadores -> OUTROS_LIMITADORES (só se tiver SIM)
    # -------------------------
    elif "outros limitadores" in aba_lower:
        cadastral = row.get("PENDENCIA NA SITUAÇÃO CADASTRAL") or row.get("PENDENCIA NA SITUACAO CADASTRAL")
        cadin = row.get("INSCRITO NO CADINE") or row.get("INSCRITO NO CADIN")
        devedor = row.get("DEVEDOR CONTUMAZ")
        invent = row.get("INVENTÁRIO OMISSO") or row.get("INVENTARIO OMISSO")

        if _is_sim(cadastral) or _is_sim(cadin) or _is_sim(devedor) or _is_sim(invent):
            tipo = "OUTROS_LIMITADORES"
            partes = []
            if _is_sim(cadastral): partes.append("CADASTRAL=SIM")
            if _is_sim(cadin): partes.append("CADIN=SIM")
            if _is_sim(devedor): partes.append("DEVEDOR_CONTUMAZ=SIM")
            if _is_sim(invent): partes.append("INVENTARIO_OMISSO=SIM")
            detalhe = "; ".join(partes)

    # qualquer outra aba: tipo vazio (não entra no resumo/detalhes)
    return {
        "cnpj": cnpj,
        "cgf": cgf,
        "razao": razao,
        "tipo_pendencia": tipo,
        "periodo": periodo,
        "detalhe": detalhe,
        "valor": valor,
        "data_referencia": data_ref,
    }
