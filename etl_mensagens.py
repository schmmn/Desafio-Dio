import csv

# -----------------------------
# EXTRAÇÃO
# -----------------------------
def extrair_dados(caminho_arquivo):
    with open(caminho_arquivo, mode="r", encoding="utf-8") as arquivo:
        leitor = csv.DictReader(arquivo)
        return list(leitor)


# -----------------------------
# TRANSFORMAÇÃO
# -----------------------------
def gerar_mensagem(cliente):
    return (
        f"Olá {cliente['nome']}, sua conta {cliente['conta']} "
        f"associada ao cartão {cliente['cartao']} possui novidades disponíveis!"
    )


def transformar_dados(dados):
    dados_transformados = []

    for cliente in dados:
        dados_transformados.append({
            "nome": cliente["nome"],
            "mensagem": gerar_mensagem(cliente)
        })

    return dados_transformados


# -----------------------------
# CARREGAMENTO
# -----------------------------
def carregar_dados(caminho_saida, dados):
    with open(caminho_saida, mode="w", encoding="utf-8", newline="") as arquivo:
        campos = ["nome", "mensagem"]
        escritor = csv.DictWriter(arquivo, fieldnames=campos)

        escritor.writeheader()
        escritor.writerows(dados)


# -----------------------------
# PIPELINE ETL
# -----------------------------
def executar_etl():
    entrada = "dados_entrada.csv"
    saida = "dados_saida.csv"

    dados_extraidos = extrair_dados(entrada)
    dados_transformados = transformar_dados(dados_extraidos)
    carregar_dados(saida, dados_transformados)

    print("ETL executado com sucesso!")


if __name__ == "__main__":
    executar_etl()
