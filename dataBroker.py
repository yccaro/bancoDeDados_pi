import requests
import sqlite3
import paho.mqtt.client as mqtt
import json

# Definir a URL da API com os parâmetros fornecidos
API_URL = "https://api.open-meteo.com/v1/forecast"
LATITUDE = -5.6344
LONGITUDE = -35.4256
PARAMETERS = "temperature_2m,rain"
TIMEZONE = "America/Fortaleza"

# Montar os parâmetros da requisição
params = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "hourly": PARAMETERS,
    "timezone": TIMEZONE
}

# Fazer a requisição GET para a API
response = requests.get(API_URL, params=params)

# Verificar se a requisição foi bem-sucedida
if response.status_code == 200:
    # Converter a resposta para JSON
    data = response.json()
    # Exibir os dados retornados pela API
    print(data)
else:
    # Exibir mensagem de erro
    print(f"Erro na requisição: {response.status_code}")

# Criar o Banco de Dados SQLite
conn = sqlite3.connect('weather_data.db')
cursor = conn.cursor()

# Criar tabela para armazenar os dados recebidos
cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        temperature REAL,
        rain REAL
    )
''')
conn.commit()

# Função para armazenar dados no banco de dados
def store_data(timestamp, temperature, rain):
    cursor.execute(
        "INSERT INTO weather (timestamp, temperature, rain) VALUES (?, ?, ?)",
        (timestamp, temperature, rain)
    )
    conn.commit()

# Função callback para quando uma mensagem é recebida pelo subscriber
def on_message(client, userdata, msg):
    try:
        # Decodificar a mensagem recebida
        payload = json.loads(msg.payload.decode())
        timestamp = payload.get('timestamp')
        temperature = payload.get('temperature_2m')
        rain = payload.get('rain')
        # Armazenar os dados no banco de dados
        store_data(timestamp, temperature, rain)
        print(f"Dados armazenados: {payload}")
    except Exception as e:
        print(f"Erro ao processar a mensagem: {e}")

# Configurar o cliente MQTT
broker_address = "test.mosquitto.org"
client = mqtt.Client("WeatherSubscriber")
client.on_message = on_message

# Conectar ao broker e se inscrever no tópico
client.connect(broker_address)
client.subscribe("weather/data")

# Iniciar o loop para receber mensagens
print("Conectado ao broker MQTT e esperando mensagens...")
client.loop_forever()

