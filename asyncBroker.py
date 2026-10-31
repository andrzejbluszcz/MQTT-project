import asyncio
import sys
import logging
from amqtt.broker import Broker

# Wyciszamy bardzo szczegółowe logi, zostawiając tylko błędy krytyczne
logging.getLogger('amqtt.broker').setLevel(logging.CRITICAL)

# Generacja haseł dla użytkowników w pliku passwd.txt:
# 1. Utwórz plik passwd.txt w tym samym katalogu co asyncBroker.py
# 2. W terminalu uruchom poniższe polecenia (zmień użytkownika i hasło): 
#    python -c "from pwdlib import PasswordHash; print(PasswordHash.recommended().hash('haslo_czujnika_1'))"


config = {
    "listeners": {
        "default": {
            "type": "tcp",
            "bind": "0.0.0.0:1883", 
        }
    },
    "sys_interval": 10,
    "plugins": {
        # Włączamy wtyczkę sprawdzającą użytkowników z pliku
        "amqtt.plugins.authentication.FileAuthPlugin": {
            "password_file": "passwd.txt"
        }
    }
}

async def start_broker():
    broker = Broker(config)
    print("Uruchamianie bezpiecznego brokera MQTT...", end="")
    await broker.start()
    print("broker działa (port 1883). Autoryzacja jest aktywna.")
    
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nZamykanie brokera...", end="")
        try:
            await asyncio.wait_for(broker.shutdown(), timeout=1.0)
        except Exception:
            pass
        print("koniec.")

if __name__ == "__main__":
    try:
        asyncio.run(start_broker())
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            pass
