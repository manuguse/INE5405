import pandas as pd
import json

# Carregando os dados
spotify_df = pd.read_csv('spotify-2023.csv')
with open('artistas_info_3.json', 'r', encoding='utf-8') as f:
    artistas_info = json.load(f)

# Criando novas colunas
spotify_df['artist_gender'] = ''
spotify_df['artist_nationality'] = ''
spotify_df['artist_type'] = ''
spotify_df['artist_gender'] = ''

# Processando cada linha do dataframe
for idx, row in spotify_df.iterrows():
    # Verificando se há múltiplos artistas
    if 'artist(s)_name' in row:
        artistas = row['artist(s)_name'].split(', ')
        
        genders = []
        nationalities = []
        types = []
        genres = []
        
        # Processando cada artista
        for artista in artistas:
            if artista in artistas_info:
                info = artistas_info[artista]
                
                genders.append(info.get('Gender', 'Unknown'))
                nationalities.append(info.get('Nationality', 'Unknown'))
                types.append(info.get('Type', 'Unknown'))
                genres.append(info['Genres'][0] if 'Genres' in info and info['Genres'] else 'Unknown')
            else:
                # Caso o artista não esteja no JSON
                genders.append('Não encontrado')
                nationalities.append('Não encontrado')
                types.append('Não encontrado')
        
        # Concatenando as informações
        spotify_df.at[idx, 'artist_gender'] = ', '.join(genders)
        spotify_df.at[idx, 'artist_nationality'] = ', '.join(nationalities)
        spotify_df.at[idx, 'artist_type'] = ', '.join(types)
        
        spotify_df.at[idx, 'artist_genre'] = genres[0] if len(genres) > 0 else None

# Mostrando as primeiras linhas do resultado
print(spotify_df[['artist(s)_name', 'artist_gender', 'artist_type', 'artist_genre']].head())

spotify_df.to_csv('spotify-2023-enriquecido.csv', index=False)
