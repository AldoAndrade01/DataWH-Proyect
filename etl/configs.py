# Esquemas objetivo
SCHEMA_SONG = [
    "track_id","title","artist","album","release_date","duration_ms",
    "explicit","popularity","genre","danceability","energy","valence",
    "tempo","loudness","acousticness","speechiness","liveness","source"
]

SCHEMA_ARTIST = [
    "artist","followers","artist_popularity","primary_genre","country",
    "monthly_listeners","world_rank"
]

# --- MAPEOS POR FUENTE ---
# D1: 30k Spotify Songs (CSV) - nombres típicos
CONFIG_D1 = {
    "source": "D1",
    "key": ["title","artist"],   # clave lógica para duplicados
    "mapping": {
        "track_id": "track_id",
        "track_name": "title",
        "title": "title",                 # por si ya viene como title
        "artists": "artist",
        "artist": "artist",
        "album": "album",
        "album_name": "album",
        "release_date": "release_date",
        "duration_ms": "duration_ms",
        "explicit": "explicit",
        "popularity": "popularity",
        "genre": "genre",
        "danceability": "danceability",
        "energy": "energy",
        "valence": "valence",
        "tempo": "tempo",
        "loudness": "loudness",
        "acousticness": "acousticness",
        "speechiness": "speechiness",
        "liveness": "liveness"
    }
}

# D2: Spotify Dashboard (XLSM) - nombres comunes en dashboards
CONFIG_D2 = {
    "source": "D2",
    "key": ["title","artist"],
    "mapping": {
        "Track Name": "title",
        "track_name": "title",
        "Title": "title",
        "Artist": "artist",
        "Album": "album",
        "Release Date": "release_date",
        "Duration_ms": "duration_ms",
        "duration_ms": "duration_ms",
        "Explicit": "explicit",
        "Popularity": "popularity",
        "Genre": "genre",
        "Danceability": "danceability",
        "Energy": "energy",
        "Valence": "valence",
        "Tempo": "tempo",
        "Loudness": "loudness",
        "Acousticness": "acousticness",
        "Speechiness": "speechiness",
        "Liveness": "liveness"
    }
}

# D3: Spotify Artist Stats (CSV) - orientado a artistas
CONFIG_D3 = {
    "source": "D3",
    "key": ["artist"],
    "mapping": {
        "artist": "artist",
        "followers": "followers",
        "popularity": "artist_popularity",
        "genres": "primary_genre",           # nos quedaremos con el 1er género
        "monthly_listeners": "monthly_listeners",
        "world_rank": "world_rank",
        "country": "country"
    }
}
