import asyncio
from amqtt.client import MQTTClient, ClientError

# Konstrukcja URL: mqtt://użytkownik:hasło@IP_brokera:port/
BROKER_URL = "mqtt://czujnik1:haslo_czujnika_1@192.168.1.20:1883/"

# Generacja haseł dla użytkowników w pliku passwd.txt:
# 1. Utwórz plik passwd.txt w tym samym katalogu co asyncClient.py
# 2. W terminalu uruchom poniższe polecenia (zmień użytkownika i hasło):
#    python -c "from pwdlib import PasswordHash; print(PasswordHash.recommended().hash('haslo_czujnika_1'))"

async def send_message():
    client = MQTTClient()
    
    try:
        print("Łączenie z bezpiecznym brokerem...", end="")
        await client.connect(BROKER_URL)
        print("połączono pomyślnie!")
        
        # Definiujemy temat i treść wiadomości
        topic = "dom/czujniki/salon"
        message_text = "Temperatura: 22.5 C"
        
        # Konwertujemy tekst na bajty (UTF-8), aby uniknąć SyntaxError
        message_bytes = message_text.encode("utf-8")
        
        # Publikujemy wiadomość z potwierdzeniem dostarczenia (QoS 1)
        print(f"Wysyłanie wiadomości do tematu '{topic}'...", end="")
        await client.publish(topic, message_bytes, qos=1)
        print("Wiadomość została wysłana.")
        
        # Eleganckie rozłączenie z brokerem
        await client.disconnect()
        print("Rozłączono.")
        
    except ClientError as ce:
        print(f"Błąd klienta MQTT: {ce}")

if __name__ == "__main__":
    asyncio.run(send_message())
