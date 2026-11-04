import asyncio
import flet as ft
from flet import run
from amqtt.client import MQTTClient, ClientError

BROKER_URL = "mqtt://127.0.0.1:1883/"

def main(page: ft.Page):
    page.title = "Panel Sterowania - Klient MQTT"
    page.width = 500
    page.height = 450
    
    client = MQTTClient()
    status_text = ft.Text("Status: Rozłączony", color=ft.Colors.ORANGE, weight=ft.FontWeight.BOLD)
    logs_list = ft.ListView(expand=True, spacing=3, auto_scroll=True)
    profile_input = ft.TextField(label="ID Profilu Zwift", value="user_123", width=250)
    
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
    
    connect_btn = ft.Button(content="Połącz z bazą", icon=ft.Icons.CAST)
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
