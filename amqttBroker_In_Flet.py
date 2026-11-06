# https://amqtt.readthedocs.io/en/v0.11.3/
# https://flet.dev/docs/

import asyncio
import logging
import flet as ft
from amqtt.broker import Broker
# from amqtt.client import MQTTClient
from amqtt.plugins.base import BasePlugin  # Potrzebne do stworzenia wtyczki
from amqtt.mqtt import MQTTPacket
from amqtt.mqtt.packet import MQTTFixedHeader, MQTTPayload, MQTTVariableHeader
from amqtt.session import Session, ApplicationMessage
from amqtt.mqtt.publish import PublishPacket
from amqtt.mqtt.packet import CONNECT, PUBLISH, SUBSCRIBE, SUBACK, UNSUBSCRIBE

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

# Globalna zmienna ze wskaźnikiem do funkcji zapisu logów dla wtyczki
_gui_log_callback = None
_gui_status_callback = None
# Zmienne globalne, do których wtyczka uzyska bezpośredni dostęp bez modyfikacji configu amqtt
active_log_view = None
active_page = None

mqttPacketTypeNames = {
    1: "CONNECT",       
    2: "CONNACK",      #no payload
    3: "PUBLISH",
    4: "PUBACK",       #no payload
    5: "PUBREC",       #no payload
    6: "PUBREL",       #no payload
    7: "PUBCOMP",      #no payload
    8: "SUBSCRIBE",
    9: "SUBACK",
    10: "UNSUBSCRIBE",
    11: "UNSUBACK",    #no payload
    12: "PINGREQ",     #no payload
    13: "PINGRESP",    #no payload
    14: "DISCONNECT"   #no payload
}
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
        self.handled_clients.discard(client_id)

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
            
    async def on_broker_client_subscribed(self, *, client_id: str, topic: str, qos: int) -> None:
        """Wykrywa subskrypcję klienta na temat."""
        msg = f"[BROKER] Klient ID={client_id} zasubskrybował: '{topic}' (QoS={qos})"
        if _gui_log_callback:
            _gui_log_callback(msg)
            
    async def on_broker_client_unsubscribed(self, *, client_id: str, topic: str) -> None:
        """Wykrywa rezygnację klienta z subskrypcji."""
        msg = f"[BROKER] Klient ID={client_id} zrezygnował z subskrypcji: '{topic}'"
        if _gui_log_callback:
            _gui_log_callback(msg)
            
    async def on_broker_message_received(self, *, client_id: str, message: ApplicationMessage) -> None:
        """Wykrywa odebranie wiadomości od klienta."""
        decoded_msg = message.data.decode("utf-8")
        # decoded_msg = message.data[2:].decode("utf-8")
        msg = f"[BROKER] Odebrano od klienta ID={client_id}: '{message.topic}' -> '{decoded_msg}'"
        if _gui_log_callback:
            _gui_log_callback(msg)
            
    async def on_broker_message_broadcast(self, *, client_id: str, message: ApplicationMessage) -> None:
        """Wykrywa wysłanie wiadomości do subskrybentów."""
        decoded_msg = message.data.decode("utf-8")
        # decoded_msg = message.data[2:].decode("utf-8")
        msg = f"[BROKER] Wysłano do subskrybentów '{message.topic}' wiadomość '{decoded_msg}' od klienta '{client_id}'"
        if _gui_log_callback:
            _gui_log_callback(msg)
            
    async def on_broker_post_start(self) -> None:
        """Wykrywa uruchomienie brokera."""
        msg = "[BROKER] Broker został uruchomiony i działa online."
        if _gui_log_callback:
            _gui_log_callback(msg)
        msg = "Status Brokera: DZIAŁA"
        if _gui_status_callback:
            _gui_status_callback(msg)
            
    async def on_broker_post_shutdown(self) -> None:
        """Wykrywa zamknięcie brokera."""
        msg = "[BROKER] Broker został bezpiecznie zamknięty."
        if _gui_log_callback:
            _gui_log_callback(msg)
        msg = "Status Brokera: ZAMKNIĘTY"
        if _gui_status_callback:
            _gui_status_callback(msg)

    async def on_mqtt_packet_sent(self, *, packet: MQTTPacket[MQTTVariableHeader, MQTTPayload[MQTTVariableHeader], MQTTFixedHeader], 
                                  session: Session | None = None) -> None:
        packetTypeNum = packet.fixed_header.packet_type
        ptn = mqttPacketTypeNames.get(packetTypeNum, "UNKNOWN")
        if isinstance(packet, PublishPacket):
            topic = packet.variable_header.topic_name
            payload = packet.payload.data or b""
            text = payload.decode("utf-8", errors="replace")
            msg = f"[WIADOMOŚĆ] wysłana {topic}: {text}"
            if _gui_log_callback:
                _gui_log_callback(msg)
        else:
            if packetTypeNum in [CONNECT, PUBLISH, SUBSCRIBE, SUBACK, UNSUBSCRIBE]:
                try:
                    payload_len = len(packet.payload.to_bytes())
                except Exception:
                    payload_len = 0
                msg = f"[PAKIET PROTOKOŁU] wysłany typ={ptn}, długość={payload_len}"
            else:
                msg = f"[PAKIET PROTOKOŁU] wysłany typ={ptn}"
            if _gui_log_callback:
                _gui_log_callback(msg)
            
    async def on_mqtt_packet_received(self, *, packet: MQTTPacket[MQTTVariableHeader, MQTTPayload[MQTTVariableHeader], MQTTFixedHeader], 
                                      session: Session | None = None) -> None:
        packetTypeNum = packet.fixed_header.packet_type
        ptn = mqttPacketTypeNames.get(packetTypeNum, "UNKNOWN")
        if isinstance(packet, PublishPacket):
            topic = packet.variable_header.topic_name
            payload = packet.payload.data or b""
            text = payload.decode("utf-8", errors="replace")
            msg = f"[WIADOMOŚĆ] odebrana {topic}: {text}"
            if _gui_log_callback:
                _gui_log_callback(msg)
        else:
            if packetTypeNum in [CONNECT, PUBLISH, SUBSCRIBE, SUBACK, UNSUBSCRIBE]:
                try:
                    payload_len = len(packet.payload.to_bytes())
                except Exception:
                    payload_len = 0
                msg = f"[PAKIET PROTOKOŁU] odebrany typ={ptn}, długość={payload_len}"
            else:
                msg = f"[PAKIET PROTOKOŁU] odebrany typ={ptn}"
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
    global _gui_status_callback
    page.title = "aMQTT + Flet 1.0"
    # page.theme_mode = ft.ThemeMode.DARK
    page.theme_mode = ft.ThemeMode.LIGHT
    page.vertical_alignment = ft.MainAxisAlignment.START
    page.window.width = 700
    page.window.height = 500
    
    # broker = Broker(BROKER_CONFIG)
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
    log_column = ft.Column(scroll=ft.ScrollMode.AUTO, auto_scroll = True, expand=True)

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

    # Rejestrujemy callback dla wtyczki, aby miała dostęp do zapisu logów w GUI
    _gui_log_callback = log_message
    
    def status_message(text: str):
        # TODO: is_running = broker_instance.transitions.state == "started"
        nonlocal is_gui_active
        if is_gui_active:
            try:
                if text == "Status Brokera: DZIAŁA":
                    status_text.color = ft.Colors.GREEN
                else:
                    status_text.color = ft.Colors.RED
                status_text.value = text
                page.update()
            except Exception:
                is_gui_active = False
                print(f"[Terminal]: {text}")
        else:
            print(f"[Terminal po zamknięciu]: {text}")
            
    _gui_status_callback = status_message

    broker_instance = None
    broker_task = None
    broker_stop_event: asyncio.Event | None = None

    async def start_broker_task(stop_event: asyncio.Event):
        nonlocal broker_instance
        broker_instance = Broker(BROKER_CONFIG)
        try:
            await broker_instance.start()
            await stop_event.wait()
        finally:
            if broker_instance:
                await broker_instance.shutdown()
            status_text.value = "Status Brokera: Zatrzymany"
            status_text.color = ft.Colors.RED
            start_btn.disabled = False
            stop_btn.disabled = True
            page.update()

    start_btn = ft.Button(content="Uruchom", icon=ft.Icons.PLAY_ARROW)
    stop_btn = ft.Button(content="Zatrzymaj", icon=ft.Icons.STOP, disabled=True)
    status_text = ft.Text("Status Brokera: Wyłączony", color=ft.Colors.RED, weight=ft.FontWeight.BOLD)

    def on_start(e):
        start_btn.disabled = True
        stop_btn.disabled = False
        nonlocal broker_task, broker_stop_event
        stop_event = asyncio.Event()
        broker_stop_event = stop_event
        broker_task = page.run_task(start_broker_task, stop_event)
        page.update()

    def on_stop(e):
        status_text.value = "Status Brokera: Zatrzymywanie..."
        page.update()
        if broker_stop_event:
            broker_stop_event.set()

    start_btn.on_click = on_start
    stop_btn.on_click = on_stop
    
    page.add(
        ft.Container(
            content=ft.Column(
                [
                    ft.Text("Panel Brokera MQTT", size=20, weight=ft.FontWeight.BOLD),
                    ft.Row([start_btn, stop_btn, status_text], alignment=ft.MainAxisAlignment.START, spacing=20),
                    ft.Text("Wiadomości:", weight=ft.FontWeight.BOLD),
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

    page.window.prevent_close = True

    async def on_window_event(e: ft.WindowEvent):
        if e.type == ft.WindowEventType.CLOSE:
            if broker_task and not broker_task.done():
                if broker_stop_event:
                    broker_stop_event.set()
                try:
                    await asyncio.wrap_future(broker_task)
                finally:
                    await page.window.destroy()
            else:
                await page.window.destroy()

    page.window.on_event = on_window_event

if __name__ == "__main__":
    ft.run(main)
