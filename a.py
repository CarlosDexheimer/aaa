import requests
import csv

OLLAMA_ENDPOINT = 'http://localhost:11434/api/generate'

def generate_prompt(lyric):
    return f"""
    Aqui está uma letra de música:
    "{lyric}"

    Por favor, classifique a letra acima como "positiva", "negativa" ou "neutra".
    apenas retorne a classificação: "neutra" ou "positiva" ou "negativa" e nada mais além disso.
    """

def classify_lyric(lyric):
    prompt = generate_prompt(lyric)
    try:
        response = requests.post(OLLAMA_ENDPOINT, json={
            "model": "gemma:2b",
            "prompt": prompt,
            "stream": False
        })
        response_data = response.json()
        print(response_data)

        classification = response_data.get("response", "").strip()
        print(classification)
        return classification
    except Exception as e:
        print(f"Erro ao classificar a letra: {e}")
        return "Erro"

def process_lyrics_from_csv(file_path):
    results = {"positivas": 0, "negativas": 0, "neutras": 0}
    classified_lyrics = []

    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)

        for row in reader:
            artist = row[0]
            lyric = row[1]
            
            classification = classify_lyric(lyric)

            classified_lyrics.append({
                "artist": artist,
                "lyric": lyric,
                "classification": classification
            })

            if "positiva" in classification.lower():
                results["positivas"] += 1
            elif "negativa" in classification.lower():
                results["negativas"] += 1
            elif "neutra" in classification.lower():
                results["neutras"] += 1

    return results, classified_lyrics

csv_file_path = 'data.csv'
results, classified_lyrics = process_lyrics_from_csv(csv_file_path)

print(f"Músicas Positivas: {results['positivas']}")
print(f"Músicas Negativas: {results['negativas']}")
print(f"Músicas Neutras: {results['neutras']}")