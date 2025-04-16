import pandas as pd
import requests
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from threading import Lock
import time
import random

# 1. Lê a planilha
df = pd.read_csv('spotify-2023.csv')

# 2. Extrai artistas únicos
artistas_unicos = set()
for lista in df['artist(s)_name']:
    for nome in lista.split(','):
        artistas_unicos.add(nome.strip())

# Converter para lista para facilitar o processamento
artistas_unicos = list(artistas_unicos)

# 3. Configuração do arquivo de saída
lock = Lock()
dados_salvos = {}
output_file = 'artistas_info_3.json'

# Carregar dados já salvos (se existirem)
try:
    with open(output_file, "r", encoding="utf-8") as f:
        dados_salvos = json.load(f)
    print(f"Carregados {len(dados_salvos)} artistas do arquivo existente")
except (FileNotFoundError, json.JSONDecodeError):
    print("Criando novo arquivo de dados")

# 4. Função de busca com retry e backoff
def buscar_info_musicbrainz(artist_name):
    # Verificar se já temos os dados deste artista
    if artist_name in dados_salvos:
        return {'Nome': artist_name, **dados_salvos[artist_name]}
    
    max_retries = 5
    retry_count = 0
    base_wait_time = 1  # tempo base em segundos
    
    while retry_count < max_retries:
        try:
            # Adicionar um delay aleatório entre 1-3 segundos antes de cada tentativa
            time.sleep(random.uniform(1, 3))
            
            url = f'https://musicbrainz.org/ws/2/artist/?query=artist:{quote(artist_name)}&fmt=json'
            headers = {
                'User-Agent': 'SpotifyArtistAnalysis/0.1 (emanuelleguse@example.com)',
                'Accept': 'application/json'
            }
            
            response = requests.get(url, headers=headers, timeout=15)
            
            # Se receber 503, aguardar e tentar novamente
            if response.status_code == 503:
                retry_count += 1
                wait_time = base_wait_time * (2 ** retry_count) + random.uniform(0, 1)  # exponential backoff
                print(f"Recebido 503 para {artist_name}. Tentativa {retry_count}/{max_retries}. Aguardando {wait_time:.2f}s")
                time.sleep(wait_time)
                continue
            
            response.raise_for_status()  # Lança exceção para outros códigos de erro HTTP
            data = response.json()
            artistas = data.get('artists', [])
            
            artista_certo = None
            # Procura um match exato (ignorando caixa)
            for artista in artistas:
                if artista['name'].lower() == artist_name.lower():
                    artista_certo = artista
                    break
            
            if not artista_certo and artistas:
                artista_certo = artistas[0]  # fallback pro primeiro
            
            if artista_certo:
                genero = artista_certo.get('gender', 'Desconhecido')
                tags = artista_certo.get('tags', [])
                tags_list = [tag['name'] for tag in tags]
                nacionalidade = artista_certo.get('country', 'Desconhecido')
                typee = artista_certo.get('type', 'Desconhecido')
            else:
                genero = 'Desconhecido'
                tags_list = []
                nacionalidade = 'Desconhecido'
                typee = 'Desconhecido'
            
            break  # Saímos do loop se chegamos até aqui sem exceções
                
        except requests.exceptions.RequestException as e:
            retry_count += 1
            wait_time = base_wait_time * (2 ** retry_count) + random.uniform(0, 1)
            print(f"Erro com {artist_name}: {e}. Tentativa {retry_count}/{max_retries}. Aguardando {wait_time:.2f}s")
            
            if retry_count >= max_retries:
                print(f"Desistindo após {max_retries} tentativas para {artist_name}")
                genero = 'Erro'
                tags_list = []
            else:
                time.sleep(wait_time)
        
        except Exception as e:
            print(f"Erro inesperado com {artist_name}: {e}")
            genero = 'Erro'
            tags_list = []
            break
    
    resultado = {
        'Gender': genero,
        'Genres': tags_list,
        'Nationality': nacionalidade,
        'Type': typee
        
    }
    
    # Salvar no dicionário em memória
    with lock:
        dados_salvos[artist_name] = resultado
        
    return {'Nome': artist_name, **resultado}

# 5. Salvar dados periodicamente
def salvar_dados():
    with lock:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(dados_salvos, f, indent=2, ensure_ascii=False)
    print(f"Dados salvos: {len(dados_salvos)} artistas")

# 6. Busca com um artista por vez (sem threading) para respeitar limites de taxa
resultados = []
contador = 0
total_artistas = len(artistas_unicos)

last_artist = "Giant Rooks"
found = False

# Processar artistas em ordem alfabética
artistas_ordenados = sorted(artistas_unicos)

for artista in artistas_ordenados:
    contador += 1
    if not found:
        if artista == last_artist:
            print(f"Reiniciando a partir de {artista}")
            found = True
            continue
        else:
            continue
    print(f"Processando {contador}/{total_artistas} - {artista}")
    
    resultado = buscar_info_musicbrainz(artista)
    resultados.append(resultado)
    
    # Salvar periodicamente (a cada 10 artistas)
    if contador % 10 == 0:
        salvar_dados()
        print(f"Progresso: {contador}/{total_artistas} ({(contador/total_artistas)*100:.1f}%)")
    
    # Aguardar entre 1-2 segundos entre requisições para evitar 503
    time.sleep(random.uniform(1, 2))

# Salvar ao final
salvar_dados()

# 7. Criar DataFrame final com os resultados
df_info = pd.DataFrame(resultados)
df_info.to_csv('artistas_info_threaded.csv', index=False)
print("Processo concluído!")