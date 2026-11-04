import asyncio
import os
import flet as ft
from flet import run
from amqtt.broker import Broker

# Zmienna globalna, do której wtyczka uzyska bezpośredni dostęp bez modyfikacji configu amqtt
active_log_view = None
active_page = None

# ==========================================================
# 1. TWOJA WTYCZKA PRZECHWYTUJĄCA ZDARZENIA DLA GUI FLET
# ==========================================================
class FletBrokerPlugin:
    def __init__(self, context):
        self.context = context

    def _append_log(self, text: str, color: str = ft.Colors.ON_SURFACE):
        """Bezpieczne odwoływanie się do globalnych referencji Fleta"""
        global active_log_view, active_page
        if active_log_view and active_page:
            active_log_view.controls.append(ft.Text(text, color=color))
            active_page.update()

    # --- Implementacja zdarzeń z dokumentacji amqtt ---

    async def on_broker_post_start(self) -> None:
        self._append_log("[SYSTEM] Serwer wystartował i nasłuchuje...", ft.Colors.GREEN)

    async def on_broker_client_connected(self, *, client_id: str) -> None:
        self._append_log(f"🟢 [URZĄDZENIE] Podłączono klienta: {client_id}", ft.Colors.GREEN_400)

    async def on_broker_client_disconnected(self, *, client_id: str) -> None:
        self._append_log(f"🔴 [URZĄDZENIE] Rozłączono klienta: {client_id}", ft.Colors.RED_400)

    async def on_broker_client_subscribed(self, *, client_id: str, topic: str, qos: int) -> None:
        self._append_log(f"✍️ [SUBSKRYPCJA] {client_id} subskrybuje temat: '{topic}' (QoS: {qos})", ft.Colors.AMBER_400)

    async def on_broker_message_received(self, *, client_id: str, message) -> None:
        try:
            payload = message.payload.data.decode("utf-8", errors="ignore")
            topic = message.topic
            self._append_log(f"📥 [WIADOMOŚĆ] Od: {client_id} | Temat: '{topic}' -> Treść: {payload}", ft.Colors.BLUE_400)
        except Exception as e:
            self._append_log(f"⚠️ Błąd dekodowania wiadomości: {e}", ft.Colors.RED_200)


# ==========================================================
# 2. INTERFEJS GRAFICZNY I URUCHOMIENIE BROKERA
# ==========================================================
def main(page: ft.Page):
    global active_log_view, active_page
    page.title = "Centralny Serwer - Broker MQTT"
    page.width = 700
    page.height = 500

    # Czyste zamknięcie procesu przy krzyżyku X
    async def on_disconnect_async(e):
        if broker_instance:
            try:
                await broker_instance.shutdown()
            except:
                pass
        os._exit(0)
    page.on_disconnect = on_disconnect_async

    log_list = ft.ListView(expand=True, spacing=5, auto_scroll=True)
    status_text = ft.Text("Status Brokera: Wyłączony", color=ft.Colors.RED, weight=ft.FontWeight.BOLD)
    
    # Przypisujemy komponenty do zmiennych globalnych, z których korzysta wtyczka
    active_log_view = log_list
    active_page = page

    broker_instance = None
    broker_task = None

    # Czysta konfiguracja – dacite nie wyrzuci błędu o nieznanych polach
    config = {
        "listeners": {
            "default": {
                "type": "tcp",
                "bind": "0.0.0.0:1883",
            }
        },
        "sys_interval": 10,
        "plugins": [
            FletBrokerPlugin 
        ]
    }

    async def start_broker_task():
        nonlocal broker_instance
        broker_instance = Broker(config)
        await broker_instance.start()
        
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            if broker_instance:
                try:
                    await broker_instance.shutdown()
                except:
                    pass
            status_text.value = "Status Brokera: Zatrzymany"
            status_text.color = ft.Colors.RED
            start_btn.disabled = False
            stop_btn.disabled = True
            page.update()

    start_btn = ft.Button(content="Uruchom Serwer", icon=ft.Icons.PLAY_ARROW)
    stop_btn = ft.Button(content="Zatrzymaj Serwer", icon=ft.Icons.STOP, disabled=True)
    
    def on_start(e):
        start_btn.disabled = True
        stop_btn.disabled = False
        status_text.value = "Status Brokera: DZIAŁA"
        status_text.color = ft.Colors.GREEN
        nonlocal broker_task
        broker_task = page.run_task(start_broker_task)
        page.update()

    def on_stop(e):
        status_text.value = "Status Brokera: Zatrzymywanie..."
        page.update()
        if broker_task:
            broker_task.cancel()

    start_btn.on_click = on_start
    stop_btn.on_click = on_stop
    
    page.add(
        ft.Row([start_btn, stop_btn, status_text], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
        ft.Divider(),
        ft.Text("Konsola zdarzeń MQTT (Wtyczka amqtt):", weight=ft.FontWeight.BOLD),
        ft.Container(content=log_list, border=ft.Border.all(1, ft.Colors.OUTLINE), height=350, padding=10)
    )

if __name__ == "__main__":
    run(main)
