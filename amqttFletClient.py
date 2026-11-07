import asyncio
import flet as ft
from flet import run
from amqtt.client import MQTTClient, ClientError
from amqtt.plugins.base import BasePlugin
from amqtt.mqtt import MQTTPacket
from amqtt.mqtt.packet import MQTTFixedHeader, MQTTPayload, MQTTVariableHeader, CONNECT, PUBLISH, SUBSCRIBE, SUBACK, UNSUBSCRIBE
from amqtt.session import Session
from amqtt.mqtt.publish import PublishPacket

# Konstrukcja URL: mqtt://użytkownik:hasło@IP_brokera:port/
BROKER_URL = "mqtt://moj_klient:haslo_mojego_klienta@127.0.0.1:1883/"
# Generacja haseł dla użytkowników w pliku passwd.txt:
# 1. Utwórz plik passwd.txt w tym samym katalogu co asyncClient.py
# 2. W terminalu uruchom poniższe polecenia (zmień użytkownika i hasło):
#    python -c "from pwdlib import PasswordHash; print(PasswordHash.recommended().hash('haslo_czujnika_1'))"
CLIENT_CONFIG = {
    "keep_alive": 60,
	"ping_delay": 6,
	"broker": {
	    "uri": "mqtt://moj_klient:haslo_mojego_klienta@127.0.0.1:1883/",
	},
    # Umieszczamy wtyczkę oraz jej parametry bezpośrednio w słowniku "plugins".
    "plugins": {
        "amqtt.plugins.logging_amqtt.EventLoggerPlugin": {},
        "__main__.ClientMonitorPlugin": {}
    }
}

_gui_log_callback = None

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
class ClientMonitorPlugin(BasePlugin):
    """
    Wtyczka monitorująca zdarzenia klienta MQTT.
    Loguje wszystkie zdarzenia do konsoli i może być rozszerzona o dodatkowe funkcje.
    """
    def __init__(self, context):
        super().__init__(context)

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

def main(page: ft.Page):
    global _gui_log_callback
    page.title = "Panel Sterowania - Klient MQTT"
    page.window.width = 500
    page.window.height = 450
    
    client = MQTTClient(config=CLIENT_CONFIG)
    status_text = ft.Text("Status: Rozłączony", color=ft.Colors.ORANGE, weight=ft.FontWeight.BOLD)
    logs_list = ft.ListView(expand=True, spacing=3, auto_scroll=True)
    profile_input = ft.TextField(label="ID Profilu Zwift", value="user_123", width=250)
    is_gui_active = True  # Flaga do sprawdzania, czy GUI jest aktywne
    
    # --- POPRAWIONA FUNKCJA LOGOWANIA ---
    def log_message(text: str):
        nonlocal is_gui_active
        if is_gui_active:
            try:
                logs_list.controls.append(ft.Text(text))
                page.update()  # To teraz na pewno zadziała w oknie programu
            except Exception:
                is_gui_active = False
                print(f"[Terminal]: {text}")
        else:
            print(f"[Terminal po zamknięciu]: {text}")

    # Rejestrujemy callback dla wtyczki, aby miała dostęp do zapisu logów w GUI
    _gui_log_callback = log_message
    # Przechowujemy referencję do zadania sieciowego
    network_task = None

    # Obsługa zamknięcia przez systemowy przycisk "X"
    async def on_disconnect_async(e):
        try:
            await client.disconnect()
        except:
            pass
        # Tutaj nie potrzebujemy os._exit, Flet sam zamknie proces po zniszczeniu sesji
    page.on_disconnect = on_disconnect_async
    
    connect_btn = ft.Button(content="Połącz z brokerem", icon=ft.Icons.CAST)
    disconnect_btn = ft.Button(content="Rozłącz", icon=ft.Icons.CAST_CONNECTED, disabled=True)
    send_btn = ft.Button(content="Wyślij 'Ride On'", icon=ft.Icons.THUMB_UP, disabled=True)

    async def client_network_task():
        try:
            status_text.value = "Status: Łączenie z serwerem..."
            page.update()
            await client.connect(BROKER_URL)
            status_text.value = "Status: POŁĄCZONY"
            status_text.color = ft.Colors.GREEN
            send_btn.disabled = False
            page.update()
            while True:
                await asyncio.sleep(5)
        except ClientError as ce:
            status_text.value = f"Status: Błąd sieci ({ce})"
            status_text.color = ft.Colors.RED
            connect_btn.disabled = False
            send_btn.disabled = True
            page.update()
        except asyncio.CancelledError:
            try:
                await client.disconnect()
            except:
                pass
            # Gdy zadanie zostanie anulowane przyciskiem, bezpiecznie ustawiamy status:
            status_text.value = "Status: Rozłączony"
            status_text.color = ft.Colors.ORANGE
            connect_btn.disabled = False
            disconnect_btn.disabled = True
            send_btn.disabled = True
            page.update()

    async def send_ride_on_action(e):
        profile_id = profile_input.value
        if not profile_id:
            return
        topic = "zwift/action/rideon"
        payload = f"Wysłano Ride On do profilu: {profile_id}"
        try:
            await client.publish(topic, payload.encode("utf-8"), qos=1)
            logs_list.controls.append(ft.Text(f"🧡 [Wysłano] {payload}", color=ft.Colors.ORANGE_700))
            page.update()
        except Exception as ex:
            logs_list.controls.append(ft.Text(f"❌ Błąd wysyłania: {ex}", color=ft.Colors.RED))
            page.update()

    def do_connect(e):
        nonlocal network_task
        connect_btn.disabled = True
        disconnect_btn.disabled = False
        network_task = page.run_task(client_network_task)
        page.update()

    async def do_disconnect(e):
        nonlocal network_task
        status_text.value = "Status: Rozłączanie..."
        page.update()
        
        # 1. Anulujemy zadanie sieciowe działające w tle (wywoła to CancelledError powyżej)
        if network_task:
            network_task.cancel()
            
        # 2. Dajemy Fletowi chwilę na przetworzenie zamknięcia asynchronicznego
        await asyncio.sleep(0.1)

    connect_btn.on_click = do_connect
    disconnect_btn.on_click = lambda e: page.run_task(do_disconnect, e)
    send_btn.on_click = lambda e: page.run_task(send_ride_on_action, e)

    page.add(
        ft.Row([connect_btn, disconnect_btn]),
        ft.Divider(),
        status_text,
        ft.Divider(),
        ft.Text("Akcja sterująca urządzeniem / API:", weight=ft.FontWeight.BOLD),
        ft.Row([profile_input, send_btn], vertical_alignment=ft.CrossAxisAlignment.CENTER),
        ft.Divider(),
        ft.Text("Historia wysłanych komend:", weight=ft.FontWeight.BOLD),
        ft.Container(content=logs_list, border=ft.Border.all(1, ft.Colors.OUTLINE), height=150, padding=10)
    )

if __name__ == "__main__":
    run(main)
