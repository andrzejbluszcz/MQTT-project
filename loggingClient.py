import asyncio
from datetime import datetime
from amqtt.client import MQTTClient, ClientError

# PAMIĘTAJ: Zmień na swój adres IP Wi-Fi, jeśli uruchamiasz na innym urządzeniu!
BROKER_URL = "mqtt://klient_logu:haslo_klientu_logu@192.168.1.20:1883/"
TOPIC = "test/topic/#"  # '#' oznacza subskrypcję wszystkich podtematów

def zapisz_do_pliku(tekst):
    # Funkcja zapisująca odebrane dane do pliku tekstowego
    with open("logi_mqtt.txt", "a", encoding="utf-8") as f:
        f.write(tekst + "\n")

async def monitor_mqtt():
    client = MQTTClient()
    
    while True:
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Łączenie z bezpiecznym brokerem...")
            await client.connect(BROKER_URL)
            print(f"Połączono! Subskrybuję temat: {TOPIC}")
            
            await client.subscribe([(TOPIC, 0)])
            
            # Pętla ciągłego nasłuchiwania na wiadomości
            while True:
                message_delivery = await client.deliver_message()
                packet = message_delivery.publish_packet
                
                temat = packet.variable_header.topic_name
                tresc = packet.payload.data.decode("utf-8", errors="ignore")
                czas = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                wpis = f"[{czas}] Temat: {temat} -> Wiadomość: {tresc}"
                print(wpis)
                zapisz_do_pliku(wpis)
                
        except ClientError as ce:
            print(f"Błąd klienta: {ce}. Ponowna próba za 5 sekund...")
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            print("\nZamykanie klienta...")
            try:
                await client.disconnect()
            except Exception:
                pass
            break
        except Exception as e:
            print(f"Niespodziewany błąd: {e}. Reconnect za 5 sekund...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(monitor_mqtt())
    except KeyboardInterrupt:
        print("Program przerwany przez użytkownika.")
