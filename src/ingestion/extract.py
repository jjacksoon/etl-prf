import os
import re
import requests
import zipfile
from bs4 import BeautifulSoup

# --- Configurações globais ---
# URL da página da PRF onde os links estão listados
BASE_URL = "https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf"
# Caminho da pasta onde os arquivos serão salvos (camada bronze do Data Lake)
DATA_DIR = "data/bronze"
# Cabeçalho para simular um navegador e evitar bloqueios por parte do servidor do governo
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def format_drive_url(url):
    """
    Transforma o link de visualização do Google Drive em link de download direto.
    O Regex busca o ID único do arquivo que fica entre '/d/' e '/view'.
    """
    # Procura o padrão de ID do Google Drive na URL fornecida
    match = re.search(r'/d/([^/]+)', url)
    if match:
        # Extrai o ID encontrado (grupo 1 do regex)
        file_id = match.group(1)
        # Retorna a URL formatada para disparar o download direto pelo Google
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url

def download_file(url, save_path):
    """
    Faz o download do arquivo de forma eficiente usando stream=True (pedaço por pedaço).
    """
    # Converte a URL do Drive para o formato de download direto antes de baixar
    url_final = format_drive_url(url)
    try:
        # Abre a conexão HTTP; stream=True impede que o arquivo seja carregado todo na memória RAM
        with requests.get(url_final, headers=HEADERS, stream=True, timeout=60) as r:
            # Verifica se a requisição foi bem-sucedida (status 200)
            r.raise_for_status() 
            # Abre o arquivo local em modo de escrita binária ('wb')
            with open(save_path, 'wb') as f:
                # Itera sobre o conteúdo em blocos de 8KB (chunks)
                for chunk in r.iter_content(chunk_size=8192):
                    # Se o bloco contiver dados, escreve no arquivo em disco
                    if chunk: f.write(chunk)
        return True
    except:
        # Se ocorrer qualquer erro, remove o arquivo parcial/incompleto para não corromper a pasta
        if os.path.exists(save_path): os.remove(save_path)
        return False

def main():
    # Verifica se a pasta data/bronze existe; se não, cria a estrutura de diretórios
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
    
    print("Iniciando extração filtrada...")
    # Faz a requisição para obter o código HTML da página principal da PRF
    response = requests.get(BASE_URL, headers=HEADERS)
    # Transforma o HTML bruto em um objeto BeautifulSoup para facilitar a busca
    soup = BeautifulSoup(response.text, 'html.parser')

    # PASSO 1: Localizar strings que contenham "Acidentes" seguido de "Agrupados por ocorrência"
    # O regex re.IGNORECASE garante que não haverá erro se houver letras maiúsculas ou minúsculas diferentes
    elementos_texto = soup.find_all(string=re.compile(r"Acidentes.*Agrupados por ocorrência", re.IGNORECASE))

    print(f"Encontrados {len(elementos_texto)} blocos de texto de ocorrências.")

    # Itera sobre cada ocorrência de texto encontrada na página
    for texto_obj in elementos_texto:
        # PASSO 2: Usa regex para extrair o ano (exatamente 4 dígitos começando com 20) de dentro do texto
        match_ano = re.search(r'(20\d{2})', texto_obj)
        # Se não encontrar um padrão de ano, pula para o próximo texto
        if not match_ano: continue
        
        # Armazena o ano em string e também em inteiro para comparações numéricas
        ano = match_ano.group(1)
        ano_int = int(ano)

        # Filtro de escopo: Se o ano não estiver entre 2005 e 2025, ignora o arquivo
        if not (2005 <= ano_int <= 2025):
            continue
        
        # PASSO 3: Localiza o elemento "pai" do texto e busca o próximo link (tag <a>) disponível depois dele
        parent = texto_obj.find_parent()
        link_tag = parent.find_next('a', href=True) if parent else None
        
        # Verifica se o link encontrado aponta para o Google Drive
        if link_tag and "drive.google.com" in link_tag['href']:
            href = link_tag['href']
            # Define o nome final esperado para o arquivo CSV
            caminho_final = os.path.join(DATA_DIR, f"datatran{ano}.csv")
            
            # PASSO 4: Idempotência - Verifica se o arquivo já existe no disco para evitar baixar novamente
            if os.path.exists(caminho_final):
                print(f"Ano {ano} já existe na Bronze. Pulando...")
                continue

            print(f"\n--- Alvo encontrado: {ano} ---")
            
            # Define um caminho temporário para o arquivo ZIP ou CSV que será baixado
            temp_path = os.path.join(DATA_DIR, f"temp_{ano}.zip")
            
            # Inicia o processo de download
            if download_file(href, temp_path):
                # PASSO 5: Tenta tratar o arquivo como um pacote compactado (.zip)
                try:
                    # Tenta abrir o arquivo baixado como um arquivo ZIP
                    with zipfile.ZipFile(temp_path, 'r') as zip_ref:
                        # Extrai todo o conteúdo do ZIP para a pasta data/bronze
                        zip_ref.extractall(DATA_DIR)
                    # Após extrair com sucesso, remove o arquivo ZIP temporário
                    os.remove(temp_path) 
                    print(f"Sucesso: Dados de {ano} extraídos e salvos.")
                except:
                    # Se o arquivo não for um ZIP (ex: download direto do CSV), apenas renomeia para o nome final
                    os.rename(temp_path, caminho_final)
                    print(f"Sucesso: Dados de {ano} salvos diretamente como CSV.")

if __name__ == "__main__":
    # Ponto de entrada que executa a função principal
    main()