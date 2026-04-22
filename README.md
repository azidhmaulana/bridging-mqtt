# Bridging MQTT

Desktop application berbasis Tauri + React untuk antarmuka operasional bridging MQTT.

Status project saat ini:

- Frontend sudah berupa dashboard/landing page bergaya tactical HUD.
- Backend Tauri masih minimal dan belum punya command Rust untuk koneksi broker, routing topic, atau telemetry nyata.
- Konfigurasi build sudah diselaraskan ke output Vite `dist` agar packaging Tauri konsisten.

## Stack

- React 19
- Vite 8
- Tauri 2
- Rust 2021

## Development

```bash
npm install
npm run dev
```

Untuk menjalankan mode desktop Tauri:

```bash
npm run tauri dev
```

## Build

```bash
npm run build
npm run tauri build
```

## Catatan

Jika target berikutnya adalah aplikasi bridge MQTT yang benar-benar fungsional, area yang perlu ditambahkan berikutnya:

- form koneksi broker
- konfigurasi source topic dan destination topic
- status koneksi realtime
- command Tauri untuk start/stop bridge
- logging event dan error panel
