import csv
import json
import re
import shutil
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# CONFIGURAÇÕES
# ============================================================

URL_DADOS = (
    "https://infosiga.detran.sp.gov.br/"
    "painel/download/file/dados_infosiga.zip"
)

URL_DICIONARIO = (
    "https://infosiga.detran.sp.gov.br/"
    "painel/download/file/Infosiga_dicionario_dados.zip"
)

# Código IBGE de Diadema
CODIGO_IBGE_DIADEMA = "3513801"

# Município
NOME_DIADEMA = "DIADEMA"

# Nome da planilha no Google Sheets
NOME_PLANILHA = "Dados Infosiga - Diadema"

# Abas da planilha
NOME_ABA_SINISTROS = "Sinistros"
NOME_ABA_PESSOAS = "Pessoas"

# Nome do arquivo de credencial do Google
ARQUIVO_CREDENCIAL = "credentials.json"

# Quantidade de linhas que o Pandas lê por vez
CHUNK_SIZE = 100_000

# Quantidade de linhas enviadas ao Google Sheets por requisição
GOOGLE_BATCH_SIZE = 2_000

# Tamanho de cada pedaço do download
DOWNLOAD_CHUNK_SIZE = 16 * 1024 * 1024  # 16 MB

# Número máximo de tentativas por pedaço
MAX_TENTATIVAS = 8

# Timeout
TIMEOUT_CONNECT = 30
TIMEOUT_READ = 180

# Busca a versao mais recente dos microdados em cada execucao. Altere para
# True somente quando quiser trabalhar offline com os arquivos ja baixados.
REUTILIZAR_ARQUIVOS_LOCAIS = False


# ============================================================
# PASTAS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

DOWNLOAD_DIR = BASE_DIR / "downloads"

EXTRACT_DIR = BASE_DIR / "extraidos"

DADOS_DIR = EXTRACT_DIR / "dados"

DICIONARIO_DIR = EXTRACT_DIR / "dicionario"


# ============================================================
# HEADERS
# ============================================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/140.0 Safari/537.36"
    )
}

# O Google Sheets permite, em geral, 60 escritas por minuto por usuario.
# Mantemos margem abaixo desse limite e repetimos requisicoes temporariamente
# recusadas para que a carga continue sem intervencao manual.
INTERVALO_MINIMO_ESCRITA_GOOGLE = 1.2
MAX_TENTATIVAS_GOOGLE = 8
ultima_escrita_google = 0.0


def executar_escrita_google(operacao, descricao):

    global ultima_escrita_google

    for tentativa in range(1, MAX_TENTATIVAS_GOOGLE + 1):

        espera = (
            INTERVALO_MINIMO_ESCRITA_GOOGLE
            - (time.monotonic() - ultima_escrita_google)
        )

        if espera > 0:

            time.sleep(espera)

        try:

            resultado = operacao()
            ultima_escrita_google = time.monotonic()

            return resultado

        except gspread.exceptions.APIError as erro:

            status = getattr(
                erro.response,
                "status_code",
                None
            )

            if status != 429 or tentativa == MAX_TENTATIVAS_GOOGLE:

                raise

            espera = min(60, 2 ** tentativa)

            print(
                f"{descricao}: limite do Google atingido. "
                f"Tentando novamente em {espera}s "
                f"({tentativa}/{MAX_TENTATIVAS_GOOGLE})."
            )

            ultima_escrita_google = time.monotonic()
            time.sleep(espera)


# ============================================================
# DOWNLOAD POR PARTES
# ============================================================

def zip_valido(arquivo):

    try:

        return (
            arquivo.exists()
            and arquivo.stat().st_size > 0
            and zipfile.is_zipfile(arquivo)
        )

    except OSError:

        return False

def obter_tamanho_arquivo(url):

    print("\nVerificando tamanho do arquivo...")

    # Primeiro tenta HEAD
    try:

        resposta = requests.head(
            url,
            headers=HEADERS,
            allow_redirects=True,
            timeout=(
                TIMEOUT_CONNECT,
                TIMEOUT_READ
            )
        )

        tamanho = resposta.headers.get(
            "Content-Length"
        )

        if tamanho:

            tamanho = int(tamanho)

            if tamanho > 0:

                print(
                    f"Tamanho encontrado: "
                    f"{tamanho / 1024 / 1024:.2f} MB"
                )

                return tamanho

    except Exception as erro:

        print(
            f"HEAD não funcionou: {erro}"
        )

    # Tenta descobrir usando Range
    try:

        resposta = requests.get(
            url,
            headers={
                **HEADERS,
                "Range": "bytes=0-0"
            },
            stream=True,
            allow_redirects=True,
            timeout=(
                TIMEOUT_CONNECT,
                TIMEOUT_READ
            )
        )

        content_range = resposta.headers.get(
            "Content-Range",
            ""
        )

        resposta.close()

        match = re.search(
            r"/(\d+)",
            content_range
        )

        if match:

            tamanho = int(
                match.group(1)
            )

            print(
                f"Tamanho encontrado: "
                f"{tamanho / 1024 / 1024:.2f} MB"
            )

            return tamanho

    except Exception as erro:

        print(
            f"Range não funcionou: {erro}"
        )

    print("Server did not provide the file size; using regular download.")

    return None

    raise Exception(
        "Não foi possível descobrir o tamanho "
        "do arquivo."
    )


def baixar(url, destino):

    destino.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    if (
        REUTILIZAR_ARQUIVOS_LOCAIS
        and zip_valido(destino)
    ):

        print(
            f"\nUsando arquivo local: {destino.name}"
        )

        return

    print("\n" + "=" * 60)

    print("BAIXANDO ARQUIVO")

    print("=" * 60)

    print(url)

    tamanho_total = obter_tamanho_arquivo(
        url
    )

    if tamanho_total is None:

        baixar_normal(
            url,
            destino,
            None
        )

        return

    tamanho_total_mb = (
        tamanho_total /
        1024 /
        1024
    )

    print(
        f"Tamanho total: "
        f"{tamanho_total_mb:.2f} MB"
    )

    # ========================================================
    # VERIFICAR SE JÁ EXISTE PARTE DO ARQUIVO
    # ========================================================

    if destino.exists():

        tamanho_atual = (
            destino.stat().st_size
        )

        if tamanho_atual > tamanho_total:

            print(
                "Arquivo existente maior que "
                "o esperado. Reiniciando."
            )

            destino.unlink()

            tamanho_atual = 0

    else:

        tamanho_atual = 0

    print(
        f"Já baixado: "
        f"{tamanho_atual / 1024 / 1024:.2f} MB"
    )

    # ========================================================
    # TESTAR SE O SERVIDOR ACEITA RANGE
    # ========================================================

    print(
        "\nTestando download por partes..."
    )

    try:

        teste = requests.get(
            url,
            headers={
                **HEADERS,
                "Range": "bytes=0-0"
            },
            stream=True,
            allow_redirects=True,
            timeout=(
                TIMEOUT_CONNECT,
                TIMEOUT_READ
            )
        )

        aceita_range = (
            teste.status_code == 206
        )

        teste.close()

    except Exception:

        aceita_range = False

    if not aceita_range:

        print(
            "O servidor não confirmou "
            "download por Range."
        )

        print(
            "Tentando download normal..."
        )

        baixar_normal(
            url,
            destino,
            tamanho_total
        )

        return

    print(
        "Servidor aceita download por partes."
    )

    # ========================================================
    # DOWNLOAD POR PARTES
    # ========================================================

    with open(
        destino,
        "ab"
    ) as arquivo:

        inicio = tamanho_atual

        while inicio < tamanho_total:

            fim = min(
                inicio +
                DOWNLOAD_CHUNK_SIZE -
                1,
                tamanho_total -
                1
            )

            bytes_esperados = (
                fim - inicio + 1
            )

            sucesso = False

            for tentativa in range(
                1,
                MAX_TENTATIVAS + 1
            ):

                print()

                print(
                    f"Bloco: "
                    f"{inicio / 1024 / 1024:.2f} MB "
                    f"→ "
                    f"{(fim + 1) / 1024 / 1024:.2f} MB"
                )

                print(
                    f"Tentativa "
                    f"{tentativa}/"
                    f"{MAX_TENTATIVAS}"
                )

                try:

                    resposta = requests.get(
                        url,
                        headers={
                            **HEADERS,
                            "Range": (
                                f"bytes={inicio}-"
                                f"{fim}"
                            )
                        },
                        stream=True,
                        allow_redirects=True,
                        timeout=(
                            TIMEOUT_CONNECT,
                            TIMEOUT_READ
                        )
                    )

                    if resposta.status_code != 206:

                        status = (
                            resposta.status_code
                        )

                        resposta.close()

                        raise Exception(
                            "Servidor retornou "
                            f"HTTP {status} "
                            "em vez de 206."
                        )

                    bytes_recebidos = 0

                    for bloco in resposta.iter_content(
                        chunk_size=1024 * 1024
                    ):

                        if not bloco:
                            continue

                        arquivo.write(
                            bloco
                        )

                        bytes_recebidos += (
                            len(bloco)
                        )

                    resposta.close()

                    # ----------------------------------------
                    # VERIFICAR BLOCO
                    # ----------------------------------------

                    if (
                        bytes_recebidos
                        != bytes_esperados
                    ):

                        # Remover o pedaço incompleto
                        arquivo.truncate(
                            tamanho_atual
                        )

                        raise Exception(
                            "Bloco incompleto. "
                            f"Recebido: "
                            f"{bytes_recebidos} bytes. "
                            f"Esperado: "
                            f"{bytes_esperados} bytes."
                        )

                    # ----------------------------------------
                    # BLOCO OK
                    # ----------------------------------------

                    tamanho_atual += (
                        bytes_recebidos
                    )

                    inicio = (
                        fim + 1
                    )

                    porcentagem = (
                        tamanho_atual /
                        tamanho_total *
                        100
                    )

                    print(
                        f"OK - "
                        f"{porcentagem:.2f}% concluído"
                    )

                    sucesso = True

                    break

                except Exception as erro:

                    print(
                        f"Erro no bloco: {erro}"
                    )

                    if tentativa < MAX_TENTATIVAS:

                        print(
                            "Tentando este bloco "
                            "novamente..."
                        )

                        time.sleep(3)

                    else:

                        raise Exception(
                            "Não foi possível baixar "
                            "este bloco após "
                            f"{MAX_TENTATIVAS} "
                            "tentativas."
                        )

            if not sucesso:

                raise Exception(
                    "Falha no download."
                )

    # ========================================================
    # VALIDAR ARQUIVO
    # ========================================================

    tamanho_final = (
        destino.stat().st_size
    )

    print()

    print(
        f"Tamanho final: "
        f"{tamanho_final / 1024 / 1024:.2f} MB"
    )

    if tamanho_final != tamanho_total:

        raise Exception(
            "O arquivo final ficou "
            "incompleto.\n"
            f"Esperado: {tamanho_total} bytes\n"
            f"Recebido: {tamanho_final} bytes"
        )

    print(
        "Download concluído com sucesso!"
    )


# ============================================================
# DOWNLOAD NORMAL - CASO RANGE NÃO FUNCIONE
# ============================================================

def baixar_normal(
    url,
    destino,
    tamanho_total
):

    for tentativa in range(
        1,
        MAX_TENTATIVAS + 1
    ):

        print(
            f"\nTentativa normal "
            f"{tentativa}/{MAX_TENTATIVAS}"
        )

        if destino.exists():

            destino.unlink()

        try:

            resposta = requests.get(
                url,
                headers=HEADERS,
                stream=True,
                allow_redirects=True,
                timeout=(
                    TIMEOUT_CONNECT,
                    TIMEOUT_READ
                )
            )

            resposta.raise_for_status()

            recebido = 0

            with open(
                destino,
                "wb"
            ) as arquivo:

                for bloco in resposta.iter_content(
                    chunk_size=1024 * 1024
                ):

                    if not bloco:
                        continue

                    arquivo.write(
                        bloco
                    )

                    recebido += len(
                        bloco
                    )

                    if tamanho_total:

                        porcentagem = (
                            recebido /
                            tamanho_total *
                            100
                        )

                        print(
                            f"\r"
                            f"{porcentagem:.2f}%",
                            end=""
                        )

            resposta.close()

            if (
                tamanho_total is None
                or recebido == tamanho_total
            ):

                print(
                    "\nDownload concluído!"
                )

                return

            raise Exception(
                f"Download incompleto: "
                f"{recebido} de "
                f"{tamanho_total} bytes."
            )

        except Exception as erro:

            print(
                f"\nErro: {erro}"
            )

            if tentativa < MAX_TENTATIVAS:

                print(
                    "Tentando novamente..."
                )

                time.sleep(5)

            else:

                raise Exception(
                    "Não foi possível baixar "
                    "o arquivo."
                )


# ============================================================
# EXTRAIR ZIP
# ============================================================

def extrair_zip(
    arquivo_zip,
    destino
):

    print(
        f"\nExtraindo: "
        f"{arquivo_zip.name}"
    )

    if (
        REUTILIZAR_ARQUIVOS_LOCAIS
        and destino.exists()
        and any(destino.rglob("*.csv"))
    ):

        print(
            "Usando arquivos ja extraidos."
        )

        return

    if destino.exists():

        shutil.rmtree(
            destino
        )

    destino.mkdir(
        parents=True,
        exist_ok=True
    )

    with zipfile.ZipFile(
        arquivo_zip,
        "r"
    ) as zip_file:

        zip_file.extractall(
            destino
        )

    print(
        "Extração concluída."
    )


# ============================================================
# DETECTAR ENCODING
# ============================================================

def detectar_encoding(
    arquivo
):

    with open(
        arquivo,
        "rb"
    ) as f:

        dados = f.read(
            100_000
        )

    encodings = [
        "utf-8-sig",
        "utf-8",
        "cp1252",
        "latin1"
    ]

    for encoding in encodings:

        try:

            dados.decode(
                encoding
            )

            return encoding

        except UnicodeDecodeError:

            continue

    return "latin1"


# ============================================================
# DETECTAR SEPARADOR
# ============================================================

def detectar_separador(
    arquivo,
    encoding
):

    with open(
        arquivo,
        "r",
        encoding=encoding,
        errors="replace"
    ) as f:

        texto = f.read(
            50_000
        )

    linhas = texto.splitlines()

    if not linhas:

        return ";"

    primeira_linha = linhas[0]

    try:

        resultado = csv.Sniffer().sniff(
            primeira_linha,
            delimiters=";,|\t"
        )

        return resultado.delimiter

    except csv.Error:

        separadores = [
            ";",
            ",",
            "|",
            "\t"
        ]

        return max(
            separadores,
            key=lambda separador:
            primeira_linha.count(
                separador
            )
        )


# ============================================================
# LOCALIZAR A TABELA CORRETA
# ============================================================

def localizar_csv():

    arquivos = list(
        DADOS_DIR.rglob(
            "*.csv"
        )
    )

    if not arquivos:

        arquivos = list(
            DADOS_DIR.rglob(
                "*.txt"
            )
        )

    if not arquivos:

        raise Exception(
            "Nenhum CSV ou TXT encontrado "
            "dentro do ZIP."
        )

    print(
        f"\nEncontrados "
        f"{len(arquivos)} arquivos."
    )

    for arquivo in arquivos:

        print(
            f"- {arquivo.name}"
        )

    print(
        "\nProcurando tabela com "
        "cod_ibge e municipio..."
    )

    for arquivo in arquivos:

        try:

            encoding = detectar_encoding(
                arquivo
            )

            separador = detectar_separador(
                arquivo,
                encoding
            )

            cabecalho = pd.read_csv(
                arquivo,
                sep=separador,
                encoding=encoding,
                dtype=str,
                nrows=0
            )

            colunas = [
                str(coluna)
                .strip()
                .lower()
                for coluna
                in cabecalho.columns
            ]

            if (
                "cod_ibge" in colunas
                and
                "municipio" in colunas
            ):

                print(
                    "\nTABELA CORRETA:"
                )

                print(
                    arquivo
                )

                print(
                    f"Encoding: "
                    f"{encoding}"
                )

                print(
                    f"Separador: "
                    f"{repr(separador)}"
                )

                return (
                    arquivo,
                    encoding,
                    separador
                )

        except Exception as erro:

            print(
                f"Não foi possível analisar "
                f"{arquivo.name}: {erro}"
            )

    raise Exception(
        "Não encontrei nenhuma tabela "
        "contendo cod_ibge e municipio."
    )


# ============================================================
# LOCALIZAR ARQUIVOS DE UM TIPO
# ============================================================

def localizar_csvs_por_tipo(tipo):

    arquivos = sorted(
        DADOS_DIR.glob(
            f"{tipo}_*.csv"
        )
    )

    if not arquivos:

        raise Exception(
            f"Nenhum arquivo de {tipo} foi encontrado."
        )

    resultado = []

    for arquivo in arquivos:

        encoding = detectar_encoding(
            arquivo
        )

        separador = detectar_separador(
            arquivo,
            encoding
        )

        cabecalho = pd.read_csv(
            arquivo,
            sep=separador,
            encoding=encoding,
            dtype=str,
            nrows=0
        )

        colunas = {
            str(coluna).strip().lower()
            for coluna in cabecalho.columns
        }

        if not {"cod_ibge", "municipio"}.issubset(colunas):

            raise Exception(
                f"{arquivo.name} nao possui cod_ibge e municipio."
            )

        resultado.append(
            (arquivo, encoding, separador)
        )

    return resultado


# ============================================================
# CONECTAR AO GOOGLE SHEETS
# ============================================================

def conectar_google_sheets():

    caminho_credencial = (
        BASE_DIR /
        ARQUIVO_CREDENCIAL
    )

    if not caminho_credencial.exists():

        raise Exception(
            "Arquivo credentials.json "
            "não encontrado.\n\n"
            "Coloque o arquivo:\n"
            "credentials.json\n\n"
            "na mesma pasta do main.py."
        )

    if caminho_credencial.stat().st_size == 0:

        raise Exception(
            "O arquivo credentials.json esta vazio.\n\n"
            "Baixe a chave JSON da conta de servico no Google Cloud "
            "e salve o arquivo completo nesta pasta."
        )

    try:

        with open(
            caminho_credencial,
            "r",
            encoding="utf-8"
        ) as arquivo:

            dados_credencial = json.load(
                arquivo
            )

    except json.JSONDecodeError as erro:

        raise Exception(
            "credentials.json nao contem um JSON valido. "
            "Baixe novamente a chave da conta de servico.\n\n"
            f"Detalhe: {erro.msg}."
        ) from erro

    campos_obrigatorios = {
        "type",
        "client_email",
        "private_key"
    }

    if (
        dados_credencial.get("type") != "service_account"
        or not campos_obrigatorios.issubset(
            dados_credencial
        )
    ):

        raise Exception(
            "credentials.json nao e uma chave de conta de servico valida."
        )

    print(
        "\nCarregando credentials.json..."
    )

    escopos = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    credenciais = (
        Credentials
        .from_service_account_file(
            caminho_credencial,
            scopes=escopos
        )
    )

    cliente = gspread.authorize(
        credenciais
    )

    print(
        "Conexão com Google concluída."
    )

    return cliente


# ============================================================
# PREPARAR PLANILHA
# ============================================================

def preparar_planilha(
    cliente,
    nome_aba,
    nome_aba_legado=None
):

    print(
        f"\nProcurando planilha:"
    )

    print(
        NOME_PLANILHA
    )

    try:

        planilha = cliente.open(
            NOME_PLANILHA
        )

    except gspread.SpreadsheetNotFound:

        raise Exception(
            "Não encontrei a planilha "
            f"'{NOME_PLANILHA}'.\n\n"
            "Confira se:\n"
            "1. A planilha existe.\n"
            "2. O nome está correto.\n"
            "3. A planilha foi compartilhada "
            "com o e-mail da conta de serviço."
        )

    print(
        "Planilha encontrada."
    )

    # ========================================================
    # PROCURAR ABA
    # ========================================================

    try:

        aba = planilha.worksheet(
            nome_aba
        )

        print(
            f"Aba '{nome_aba}' encontrada."
        )

    except gspread.WorksheetNotFound:

        if nome_aba_legado:

            try:

                aba = planilha.worksheet(
                    nome_aba_legado
                )

                executar_escrita_google(
                    lambda: aba.update_title(nome_aba),
                    "Renomeando aba"
                )

                executar_escrita_google(
                    aba.clear,
                    "Limpando aba"
                )

                executar_escrita_google(
                    lambda: aba.resize(rows=1, cols=1),
                    "Redimensionando aba"
                )

                print(
                    f"Aba '{nome_aba_legado}' reutilizada como '{nome_aba}'."
                )

                return aba

            except gspread.WorksheetNotFound:

                pass

        print(
            f"Aba '{nome_aba}' não existe."
        )

        print(
            "Criando aba..."
        )

        aba = executar_escrita_google(
            lambda: planilha.add_worksheet(
                title=nome_aba,
                rows=1,
                cols=1
            ),
            "Criando aba"
        )

        print(
            "Aba criada."
        )

    # ========================================================
    # LIMPAR DADOS ANTIGOS
    # ========================================================

    print(
        "\nLimpando dados antigos..."
    )

    executar_escrita_google(
        aba.clear,
        "Limpando aba"
    )

    executar_escrita_google(
        lambda: aba.resize(rows=1, cols=1),
        "Redimensionando aba"
    )

    print(
        "Dados antigos removidos."
    )

    return aba


# ============================================================
# CONVERTER NÚMERO DA COLUNA PARA LETRA
# ============================================================

def numero_para_coluna(
    numero
):

    resultado = ""

    while numero > 0:

        numero, resto = divmod(
            numero - 1,
            26
        )

        resultado = (
            chr(65 + resto)
            + resultado
        )

    return resultado


# ============================================================
# ENVIAR DADOS PARA GOOGLE SHEETS
# ============================================================

def enviar_para_google(
    aba,
    dados,
    linha_inicial
):

    dados = dados.fillna("")

    dados = dados.astype(str)

    valores = dados.values.tolist()

    if not valores:

        return linha_inicial

    total_linhas = len(
        valores
    )

    total_colunas = len(
        dados.columns
    )

    coluna_final = (
        numero_para_coluna(
            total_colunas
        )
    )

    # A aba pode ter sido criada com poucas colunas. Garanta espaco
    # suficiente antes de montar os intervalos de envio.
    if aba.col_count < total_colunas:

        executar_escrita_google(
            lambda: aba.add_cols(
                total_colunas - aba.col_count
            ),
            "Expandindo colunas"
        )

    inicio = 0

    linha_atual = linha_inicial

    while inicio < total_linhas:

        fim = min(
            inicio +
            GOOGLE_BATCH_SIZE,
            total_linhas
        )

        lote = valores[
            inicio:fim
        ]

        quantidade = len(
            lote
        )

        linha_final = (
            linha_atual +
            quantidade -
            1
        )

        # clear() nao aumenta a grade da planilha. Sem esta expansao,
        # lotes posteriores falham quando ultrapassam o numero de linhas
        # inicialmente disponivel na aba.
        if aba.row_count < linha_final:

            executar_escrita_google(
                lambda: aba.add_rows(
                    linha_final - aba.row_count
                ),
                "Expandindo linhas"
            )

        intervalo = (
            f"A{linha_atual}:"
            f"{coluna_final}"
            f"{linha_final}"
        )

        print(
            f"\nEnviando linhas "
            f"{linha_atual:,} até "
            f"{linha_final:,}..."
        )

        executar_escrita_google(
            lambda: aba.update(
                lote,
                intervalo,
                value_input_option="RAW"
            ),
            "Enviando lote"
        )

        print(
            "Lote enviado."
        )

        linha_atual = (
            linha_final + 1
        )

        inicio = fim

    return linha_atual


# ============================================================
# PROCESSAR CSV
# ============================================================

def processar_e_enviar(
    arquivo,
    encoding,
    separador,
    aba,
    linha_google=2,
    cabecalho_enviado=False
):

    print(
        "\n" + "=" * 60
    )

    print(
        "PROCESSANDO INFOSIGA"
    )

    print(
        "=" * 60
    )

    print(
        f"Arquivo: {arquivo.name}"
    )

    print(
        f"Encoding: {encoding}"
    )

    print(
        f"Separador: {repr(separador)}"
    )

    leitor = pd.read_csv(
        arquivo,
        sep=separador,
        encoding=encoding,
        dtype=str,
        chunksize=CHUNK_SIZE,
        low_memory=False,
        on_bad_lines="warn"
    )

    total_processado = 0

    total_diadema = 0

    for numero_chunk, chunk in enumerate(
        leitor,
        start=1
    ):

        print(
            "\n" + "-" * 60
        )

        print(
            f"CHUNK {numero_chunk}"
        )

        print(
            "-" * 60
        )

        total_processado += (
            len(chunk)
        )

        # ====================================================
        # MAPEAR COLUNAS
        # ====================================================

        colunas = {}

        for coluna in chunk.columns:

            nome = (
                str(coluna)
                .strip()
                .lower()
            )

            colunas[nome] = coluna

        if "cod_ibge" not in colunas:

            raise Exception(
                "A coluna cod_ibge "
                "não foi encontrada."
            )

        if "municipio" not in colunas:

            raise Exception(
                "A coluna municipio "
                "não foi encontrada."
            )

        coluna_ibge = colunas[
            "cod_ibge"
        ]

        coluna_municipio = colunas[
            "municipio"
        ]

        # ====================================================
        # LIMPAR CODIGO IBGE
        # ====================================================

        codigo = (
            chunk[coluna_ibge]
            .astype(str)
            .str.strip()
            .str.replace(
                r"\.0$",
                "",
                regex=True
            )
        )

        # ====================================================
        # LIMPAR MUNICIPIO
        # ====================================================

        municipio = (
            chunk[coluna_municipio]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        # ====================================================
        # FILTRO DIADEMA
        # ====================================================

        filtro = (
            (codigo == CODIGO_IBGE_DIADEMA)
            &
            (municipio == NOME_DIADEMA)
        )

        dados_diadema = (
            chunk[filtro]
            .copy()
        )

        quantidade_diadema = (
            len(dados_diadema)
        )

        total_diadema += (
            quantidade_diadema
        )

        print(
            f"Registros no chunk: "
            f"{len(chunk):,}"
        )

        print(
            f"Registros de Diadema: "
            f"{quantidade_diadema:,}"
        )

        print(
            f"Total processado: "
            f"{total_processado:,}"
        )

        print(
            f"Total Diadema: "
            f"{total_diadema:,}"
        )

        # ====================================================
        # SE NÃO TEM DIADEMA
        # ====================================================

        if dados_diadema.empty:

            continue

        # ====================================================
        # ENVIAR CABEÇALHO
        # ====================================================

        if not cabecalho_enviado:

            cabecalho = [
                str(coluna)
                for coluna
                in dados_diadema.columns
            ]

            if aba.col_count < len(cabecalho):

                executar_escrita_google(
                    lambda: aba.add_cols(
                        len(cabecalho) - aba.col_count
                    ),
                    "Expandindo colunas"
                )

            print(
                "\nEnviando cabeçalho..."
            )

            executar_escrita_google(
                lambda: aba.update(
                    [cabecalho],
                    "A1",
                    value_input_option="RAW"
                ),
                "Enviando cabecalho"
            )

            cabecalho_enviado = True

            print(
                "Cabeçalho enviado."
            )

        # ====================================================
        # ENVIAR DADOS
        # ====================================================

        linha_google = enviar_para_google(
            aba,
            dados_diadema,
            linha_google
        )

    # ========================================================
    # RESULTADO
    # ========================================================

    print(
        "\n" + "=" * 60
    )

    print(
        "PROCESSAMENTO FINALIZADO"
    )

    print(
        "=" * 60
    )

    print(
        f"Total processado: "
        f"{total_processado:,}"
    )

    print(
        f"Total de Diadema: "
        f"{total_diadema:,}"
    )

    return (
        total_processado,
        total_diadema,
        linha_google,
        cabecalho_enviado
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 60)
    print(
        "       INFOSIGA SP → GOOGLE SHEETS"
    )
    print(
        "              DIADEMA/SP"
    )
    print("=" * 60)

    try:

        # ====================================================
        # 1 - DOWNLOAD DOS MICRODADOS
        # ====================================================

        print(
            "\n[1/6] Baixando microdados..."
        )

        caminho_dados = (
            DOWNLOAD_DIR /
            "dados_infosiga.zip"
        )

        baixar(
            URL_DADOS,
            caminho_dados
        )

        # ====================================================
        # 2 - DOWNLOAD DO DICIONÁRIO
        # ====================================================

        print(
            "\n[2/6] Baixando dicionário..."
        )

        caminho_dicionario = (
            DOWNLOAD_DIR /
            "Infosiga_dicionario_dados.zip"
        )

        baixar(
            URL_DICIONARIO,
            caminho_dicionario
            )

        # ====================================================
        # 3 - EXTRAIR
        # ====================================================

        print(
            "\n[3/6] Extraindo arquivos..."
        )

        extrair_zip(
            caminho_dados,
            DADOS_DIR
        )

        extrair_zip(
            caminho_dicionario,
            DICIONARIO_DIR
        )

        # ====================================================
        # 4 - LOCALIZAR TABELA
        # ====================================================

        print(
            "\n[4/6] Localizando tabela..."
        )

        arquivos_sinistros = localizar_csvs_por_tipo(
            "sinistros"
        )

        arquivos_pessoas = localizar_csvs_por_tipo(
            "pessoas"
        )

        # ====================================================
        # 5 - GOOGLE SHEETS
        # ====================================================

        print(
            "\n[5/6] Conectando ao Google Sheets..."
        )

        cliente = (
            conectar_google_sheets()
        )

        aba_pessoas = preparar_planilha(
            cliente,
            NOME_ABA_PESSOAS,
            "Infosiga"
        )

        aba_sinistros = preparar_planilha(
            cliente,
            NOME_ABA_SINISTROS
        )

        # ====================================================
        # 6 - PROCESSAR
        # ====================================================

        print(
            "\n[6/6] Filtrando Diadema "
            "e enviando para Google Sheets..."
        )

        conjuntos = [
            ("Sinistros", arquivos_sinistros, aba_sinistros),
            ("Pessoas", arquivos_pessoas, aba_pessoas)
        ]

        totais = {}

        for nome, arquivos, aba in conjuntos:

            print(f"\nProcessando tabela: {nome}")

            linha_google = 2
            cabecalho_enviado = False
            total_processado = 0
            total_diadema = 0

            for arquivo, encoding, separador in arquivos:

                (
                    processados_arquivo,
                    diadema_arquivo,
                    linha_google,
                    cabecalho_enviado
                ) = processar_e_enviar(
                    arquivo,
                    encoding,
                    separador,
                    aba,
                    linha_google,
                    cabecalho_enviado
                )

                total_processado += processados_arquivo
                total_diadema += diadema_arquivo

            totais[nome] = (
                total_processado,
                total_diadema
            )

        total_processado = sum(
            total[0]
            for total in totais.values()
        )

        total_diadema = sum(
            total[1]
            for total in totais.values()
        )

        # ====================================================
        # FINAL
        # ====================================================

        print()
        print("=" * 60)
        print(
            "                 CONCLUÍDO"
        )
        print("=" * 60)

        print()
        print(
            f"Registros processados: "
            f"{total_processado:,}"
        )

        print(
            f"Registros de Diadema: "
            f"{total_diadema:,}"
        )

        print()
        print(
            f"Filtro utilizado:"
        )

        print(
            f"cod_ibge = "
            f"{CODIGO_IBGE_DIADEMA}"
        )

        print(
            f"municipio = "
            f"{NOME_DIADEMA}"
        )

        print()
        print(
            f"Planilha: "
            f"{NOME_PLANILHA}"
        )

        print(
            f"Abas: "
            f"{NOME_ABA_SINISTROS} e {NOME_ABA_PESSOAS}"
        )

        print()
        print(
            "Os dados foram enviados "
            "para o Google Sheets."
        )

    except KeyboardInterrupt:

        print(
            "\n\nProcesso interrompido "
            "pelo usuário."
        )

    except Exception as erro:

        print()
        print("=" * 60)
        print(
            "                    ERRO"
        )
        print("=" * 60)

        print()
        print(erro)

        print()
        print(
            "Verifique a mensagem acima."
        )


# ============================================================
# EXECUTAR
# ============================================================

if __name__ == "__main__":

    main()
