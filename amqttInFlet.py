# https://amqtt.readthedocs.io/en/v0.11.3/
# https://flet.dev/docs/

import asyncio
import logging
import flet as ft
from amqtt.broker import Broker
from amqtt.client import MQTTClient
from amqtt.plugins.base import BasePlugin  # Potrzebne do stworzenia wtyczki

# --- KULOOPORNY FILTR LOGÓW DLA AMQTT ---
class AmqttShutdownFilter(logging.Filter):
    def filter(self, record):
        # Jeśli komunikat zawiera informację o przerwaniu pętli broadcast, ucisz go
        if "Broadcast loop stopped by exception" in record.getMessage():
            return False
        return True

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO)
# Nakładamy filtr bezpośrednio na loggera biblioteki amqtt
logging.getLogger("amqtt.broker").addFilter(AmqttShutdownFilter())
# ----------------------------------------

# Globalna zmienna trzymająca referencję do funkcji logowania dla wtyczki
_gui_log_callback = None

# --- WŁASNA WTYCZKA BROKERA DO MONITOROWANIA ZDARZEŃ ---
class BrokerMonitorPlugin(BasePlugin):
    """Wtyczka przechwytująca zdarzenia sieciowe wprost z jądra brokera."""
    
    def __init__(self, context):
        super().__init__(context)
        self.handled_clients = set()
    
    async def on_broker_client_connected(self, *, client_id: str, client_session) -> None:
        """Wykrywa nowe, udane połączenie klienta."""
        msg = f"[BROKER INTERFEJS] Nowy klient połączony: ID={client_id} z adresu {client_session.remote_address}"
        if _gui_log_callback:
            _gui_log_callback(msg)
            
        # Gdyby klient łączył się ponownie, usuwamy go z listy czystych rozłączeń
        self.clean_disconnects.discard(client_id)

        # Podpinamy się pod maszynę stanów tej konkretnej sesji, aby wykryć nagłe zerwanie sieci
        client_session.transitions.on_enter_disconnected(
            lambda: asyncio.create_task(self._handle_disconnect_event(client_id, client_session))
       )

    async def on_broker_client_disconnected(self, *, client_id: str, client_session) -> None:
        """Wykrywa normalne rozłączenie (gdy pakiet DISCONNECT zdąży dotrzeć)."""
        if client_id not in self.handled_clients:
            self.handled_clients.add(client_id)
            msg = f"[BROKER] Klient odłączył się poprawnie: ID={client_id}"
            if _gui_log_callback:
                _gui_log_callback(msg)

    async def _handle_disconnect_event(self, client_id: str, client_session):
        """Sprawdza realny powód zamknięcia połączenia na poziomie sieci TCP."""
        await asyncio.sleep(0.05)  # Krótka pauza na synchronizację stanów
        
        if client_id in self.handled_clients:
            return  # Klient został już obsłużony przez on_broker_client_disconnected

        self.handled_clients.add(client_id)

        # Sprawdzamy, czy w obiekcie sesji lub strumienia pojawił się błąd połączenia.
        # Jeśli połączenie zostało zamknięte bez błędów (np. szybki koniec skryptu klienta),
        # traktujemy to jako normalne zamknięcie programu.
        reader = getattr(client_session, 'reader', None)
        has_network_error = False
        
        if reader and hasattr(reader, '_exception') and reader._exception is not None:
            has_network_error = True

        if has_network_error:
            msg = f"[BROKER] ⚠️ UTRACONO POŁĄCZENIE z klientem: ID={client_id} (Awaria / brak sieci)"
        else:
            # System zamknął gniazdo TCP bez błędów - to zwykłe zamknięcie aplikacji klienta
            msg = f"[BROKER] Klient zakończył połączenie: ID={client_id}"
            
        if _gui_log_callback:
            _gui_log_callback(msg)

    def _handle_sudden_disconnect(self, client_id: str, client_session):
        """Sprawdza maszynę stanów sesji i wykrywa nagłą utratę połączenia."""
        # Jeśli sesja weszła w stan 'disconnected', ale klient NIE wysłał pakietu DISCONNECT
        if not getattr(client_session, 'clean_disconnect', False):
            msg = f"[BROKER INTERFEJS] ⚠️ UTRACONO POŁĄCZENIE z klientem: ID={client_id} (KeepAlive timeout / brak sieci)"
            if _gui_log_callback:
                _gui_log_callback(msg)

BROKER_CONFIG = {
    "listeners": {
        "default": {
            "type": "tcp",
            "bind": "0.0.0.0:1883",
        }
    },
    "sys_interval": 0,
    # Umieszczamy wtyczkę oraz jej parametry bezpośrednio w słowniku "plugins".
    # Tylko w ten sposób amqtt prawidłowo powiąże bazę użytkowników z silnikiem.
    "plugins": {
        "amqtt.plugins.logging_amqtt.EventLoggerPlugin": {},
        "amqtt.plugins.authentication.FileAuthPlugin": {
            "password_file": "passwd.txt"  # Klucz z myślnikiem!
        },
        # Rejestrujemy naszą własną klasę wtyczki bez używania stringów (bezpieczne dla exe!)
        "__main__.BrokerMonitorPlugin": {}
    }
}

async def main(page: ft.Page):
    global _gui_log_callback
    page.title = "aMQTT + Flet 1.0"
    # page.theme_mode = ft.ThemeMode.DARK
    page.theme_mode = ft.ThemeMode.LIGHT
    page.vertical_alignment = ft.MainAxisAlignment.START

    # topic = "test/topic"
    client = MQTTClient()
    broker = Broker(BROKER_CONFIG)
    shutdown_event = asyncio.Event()
    # Generacja haseł dla użytkowników w pliku passwd.txt:
    # 1. Utwórz plik passwd.txt w tym samym katalogu co asyncBroker.py
    # 2. W terminalu uruchom poniższe polecenia (zmień użytkownika i hasło): 
    #    python -c "from pwdlib import PasswordHash; print(PasswordHash.recommended().hash('tu_wstaw_haslo'))"

    # PAMIĘTAJ: Zmień na swój adres IP Wi-Fi, jeśli uruchamiasz na innym urządzeniu!
    BROKER_URL = "mqtt://moj_uzytkownik:super_tajne_haslo@192.168.1.20:1883/"
    SUBSCRIBE_TOPIC = "test/topic/#"
    PUBLISH_TOPIC = "test/topic"

    # Prosta flaga: dopóki True, piszemy do okna. Po zamknięciu przełączamy na print()
    is_gui_active = True

    # --- Elementy interfejsu ---
    input_msg = ft.TextField(
        label="Wiadomość do wysłania", value="Cześć z Fleta 1.0!", width=300
    )
    log_column = ft.Column(scroll=ft.ScrollMode.AUTO, expand=True)

    # --- POPRAWIONA FUNKCJA LOGOWANIA ---
    def log_message(text: str):
        nonlocal is_gui_active
        if is_gui_active:
            try:
                log_column.controls.append(ft.Text(text))
                page.update()  # To teraz na pewno zadziała w oknie programu
            except Exception:
                is_gui_active = False
                print(f"[Terminal]: {text}")
        else:
            print(f"[Terminal po zamknięciu]: {text}")

    # Rejestrujemy callback dla wtyczki, aby miała dostęp do logowania w GUI
    _gui_log_callback = log_message

    async def send_click(e):
        if input_msg.value:
            try:
                payload = input_msg.value.encode("utf-8")
                await client.publish(PUBLISH_TOPIC, payload)
                log_message(f"[Wysłano] -> {input_msg.value}")
            except Exception as ex:
                log_message(f"[Błąd wysyłania]: {ex}")

    send_button = ft.Button("Publikuj przez MQTT", on_click=send_click)

    page.add(
        ft.Container(
            content=ft.Column(
                [
                    ft.Text("Panel Sterowania MQTT", size=20, weight=ft.FontWeight.BOLD),
                    ft.Row([input_msg, send_button]),
                    ft.Text("Odebrane wiadomości:", weight=ft.FontWeight.BOLD),
                    ft.Container(
                        content=log_column,
                        border=ft.Border.all(1, ft.Colors.OUTLINE),
                        border_radius=5,
                        padding=10,
                        height=250,
                        expand=True,
                    ),
                ],
                expand=True,
            ),
            padding=20,
            expand=True,
        )
    )

    # --- Pętla Odbiorcza Klienta MQTT ---
    async def mqtt_listener():
        log_message("[Klient GUI] Łączenie z brokerem...")
        try:
            await client.connect(BROKER_URL)
            await client.subscribe([(SUBSCRIBE_TOPIC, 0)])
            log_message(f"[Klient GUI] Połączono i zasubskrybowano: {SUBSCRIBE_TOPIC}")

            while not shutdown_event.is_set():
                try:
                    message = await asyncio.wait_for(client.deliver_message(), timeout=1.0)
                    packet = message.publish_packet
                    decoded_msg = packet.payload.data.decode("utf-8")
                    log_message(f"[Odebrano] {packet.topic_name}: {decoded_msg}")
                except asyncio.TimeoutError:
                    continue
                
        except Exception as ex:
            log_message(f"[Klient Błąd]: {ex}")
        finally:
            log_message("[Klient GUI] Rozłączanie...")
            try:
                await client.disconnect()
            except Exception:
                pass

    # --- Główny Koordynator Systemu MQTT ---
    async def run_mqtt_system():
        log_message("[Broker] Uruchamianie wbudowanego brokera...")
        try:
            await broker.start()
            log_message("[Broker] Działa online na porcie 1883")
            
            listener_task = asyncio.create_task(mqtt_listener())
            await shutdown_event.wait()
            
            listener_task.cancel()
            await asyncio.gather(listener_task, return_exceptions=True)

        except Exception as ex:
            log_message(f"[Broker Błąd]: {ex}")
        finally:
            log_message("[Broker] Zamykanie brokera...")
            await broker.shutdown()
            log_message("[System] Wszystkie zasoby MQTT zostały wyczyszczone.")

    system_task = asyncio.create_task(run_mqtt_system())

    # --- Obsługa zamknięcia aplikacji ---
    async def on_disconnect(e):
        nonlocal is_gui_active
        print("Wykryto zamknięcie okna. Rozpoczynam czyszczenie zasobów...")
        
        # Dopiero TUTAJ odcinamy GUI, żeby ostatnie logi zamykania szły na terminal
        is_gui_active = False 
        shutdown_event.set()
        system_task.cancel()
        
        try:
            await asyncio.wait_for(system_task, timeout=0.5)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        
        print("Aplikacja została bezpiecznie zamknięta.")

    page.on_disconnect = on_disconnect


if __name__ == "__main__":
    ft.run(main)
