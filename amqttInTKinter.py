import asyncio
import logging
import tkinter as tk
from amqtt.broker import Broker
from amqtt.client import MQTTClient

# Opcjonalne: włączenie logowania amqtt w konsoli (przydatne do debugowania)
logging.basicConfig(level=logging.INFO)

# Prosta konfiguracja dla wbudowanego brokera amqtt
BROKER_CONFIG = {
    "listeners": {
        "default": {
            "type": "tcp",
            "bind": "127.0.0.1:1883",  # Nasłuchuj na localhost
        }
    },
    "sys_interval": 0,  # Wyłącz publikowanie statystyk $SYS (dla uproszczenia)
    "auth": {"allow-anonymous": True},  # Zezwól na połączenia bez hasła
}


class MqttGuiApp:

    def __init__(self, root):
        self.root = root
        self.root.title("aMQTT + Tkinter (Broker & Client)")
        self.root.geometry("450x400")

        self.client = MQTTClient()
        self.topic = "test/topic"

        # --- UI: Panel wysyłania ---
        tk.Label(root, text="Wiadomość do wysłania:", font=("Arial", 10, "bold")).pack(
            pady=5
        )
        self.entry_msg = tk.Entry(root, width=40)
        self.entry_msg.pack(pady=5)
        self.entry_msg.insert(0, "Cześć z Tkintera!")

        self.btn_send = tk.Button(
            root, text="Publikuj przez MQTT", command=self.trigger_publish
        )
        self.btn_send.pack(pady=5)

        # --- UI: Panel odbierania ---
        tk.Label(root, text="Odebrane wiadomości:", font=("Arial", 10, "bold")).pack(
            pady=5
        )
        self.txt_logs = tk.Text(root, height=12, width=50, state=tk.DISABLED)
        self.txt_logs.pack(pady=5)

    def log_message(self, text):
        """Bezpieczne dopisywanie tekstu do okna logów."""
        self.txt_logs.config(state=tk.NORMAL)
        self.txt_logs.insert(tk.END, text + "\n")
        self.txt_logs.see(tk.END)
        self.txt_logs.config(state=tk.DISABLED)

    def trigger_publish(self):
        """Pobiera tekst z pola tekstowego i wrzuca zadanie publikacji do pętli asyncio."""
        msg = self.entry_msg.get()
        asyncio.create_task(self.publish_message(msg))

    async def publish_message(self, message):
        """Asynchroniczne publikowanie wiadomości."""
        try:
            payload = message.encode("utf-8")
            await self.client.publish(self.topic, payload)
            self.log_message(f"[Wysłano] -> {message}")
        except Exception as e:
            self.log_message(f"[Błąd wysyłania]: {e}")

    async def start_mqtt_client(self):
        """Łączy klienta z lokalnym brokerem i zaczyna nasłuchiwać wiadomości."""
        await asyncio.sleep(1)  # Daj sekunde brokerowi na pełne uruchomienie
        self.log_message("[Klient] Łączenie z brokerem...")

        try:
            # Łączymy się z własnym, lokalnym brokerem
            await self.client.connect("mqtt://127.0.0.1:1883/")
            # Subskrybujemy temat (QoS 0)
            await self.client.subscribe([(self.topic, 0)])
            self.log_message(f"[Klient] Połączono i zasubskrybowano: {self.topic}")

            # Pętla ciągłego odbierania wiadomości
            while True:
                message = await self.client.deliver_message()
                packet = message.publish_packet
                decoded_msg = packet.payload.data.decode("utf-8")
                self.log_message(f"[Odebrano] {packet.topic_name}: {decoded_msg}")

        except asyncio.CancelledError:
            self.log_message("[Klient] Zatrzymywanie klienta...")
            await self.client.disconnect()
        except Exception as e:
            self.log_message(f"[Klient Błąd]: {e}")

    async def tk_loop(self):
        """Asynchroniczne zastępstwo dla root.mainloop()."""
        while True:
            try:
                self.root.update()
            except tk.TclError:
                # Wyjątek rzucany, gdy użytkownik zamknie okno krzyżykiem
                break
            await asyncio.sleep(0.02)  # Częstotliwość odświeżania UI (ok. 50 FPS)


async def main():
    # 1. Inicjalizacja i uruchomienie wbudowanego brokera amqtt
    broker = Broker(BROKER_CONFIG)
    print("Uruchamianie wbudowanego brokera MQTT...")
    await broker.start()

    # 2. Inicjalizacja okna Tkinter i aplikacji
    root = tk.Tk()
    app = MqttGuiApp(root)

    # 3. Uruchomienie klienta MQTT jako zadania w tle
    client_task = asyncio.create_task(app.start_mqtt_client())

    # 4. Uruchomienie pętli okna Tkinter (główny punkt blokujący funkcję main)
    await app.tk_loop()

    # 5. Czyszczenie zasobów po zamknięciu okna GUI
    print("Zamykanie aplikacji...")
    client_task.cancel()  # Zatrzymujemy klienta
    await broker.shutdown()  # Zamykamy broker
    print("Broker i klient zostali wyłączeni.")


if __name__ == "__main__":
    # Uruchomienie wszystkiego w jednej pętli asyncio
    asyncio.run(main())
