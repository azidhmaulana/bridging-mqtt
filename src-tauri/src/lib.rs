use std::{
  collections::HashMap,
  sync::Mutex,
  time::{Duration, Instant},
};

use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS, Transport};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tauri::{async_runtime::JoinHandle, AppHandle, Emitter, State};
use tokio::time::sleep;

#[derive(Default)]
struct BridgeManager {
  runtimes: Mutex<HashMap<String, BridgeRuntime>>,
}

struct BridgeRuntime {
  task: JoinHandle<()>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct TopicMapping {
  source: String,
  destination: String,
  collect_data: bool,
}

#[derive(Clone, Debug)]
struct CollectedPayloadEntry {
  data: Value,
  updated_at: Instant,
  source_timestamp_ms: i64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AggregateSnapshot {
  generated_at: String,
  reason: String,
  entries: HashMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BridgeConfig {
  id: String,
  #[serde(rename = "bridgeName")]
  _bridge_name: String,
  source_protocol: String,
  source_host: String,
  source_port: String,
  source_client_id: String,
  source_username: String,
  source_password: String,
  destination_protocol: String,
  destination_host: String,
  destination_port: String,
  destination_client_id: String,
  destination_username: String,
  destination_password: String,
  topic_mappings: Vec<TopicMapping>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeStatusEvent {
  id: String,
  connection_state: String,
  enabled: bool,
  error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeLogEvent {
  id: String,
  level: String,
  message: String,
}

#[tauri::command]
async fn start_bridge(
  app: AppHandle,
  state: State<'_, BridgeManager>,
  config: BridgeConfig,
) -> Result<(), String> {
  stop_bridge_runtime(&app, &state, &config.id);

  validate_bridge(&config)?;

  emit_bridge_status(
    &app,
    &config.id,
    "reconnect",
    true,
    Some("Starting bridge".into()),
  );
  emit_bridge_log(&app, &config.id, "info", "Starting bridge runtime");

  let bridge_id = config.id.clone();
  let task = tauri::async_runtime::spawn(run_bridge_task(app.clone(), config));

  state
    .runtimes
    .lock()
    .map_err(|_| "Failed to lock bridge manager".to_string())?
    .insert(bridge_id, BridgeRuntime { task });

  Ok(())
}

#[tauri::command]
async fn stop_bridge(
  app: AppHandle,
  state: State<'_, BridgeManager>,
  bridge_id: String,
) -> Result<(), String> {
  stop_bridge_runtime(&app, &state, &bridge_id);
  Ok(())
}

fn stop_bridge_runtime(app: &AppHandle, state: &State<'_, BridgeManager>, bridge_id: &str) {
  if let Ok(mut runtimes) = state.runtimes.lock() {
    if let Some(runtime) = runtimes.remove(bridge_id) {
      runtime.task.abort();
    }
  }

  emit_bridge_status(app, bridge_id, "disconnected", false, None);
  emit_bridge_log(app, bridge_id, "warn", "Bridge stopped");
}

fn validate_bridge(config: &BridgeConfig) -> Result<(), String> {
  if config.source_host.trim().is_empty() {
    return Err("Source host is required".into());
  }

  if config.destination_host.trim().is_empty() {
    return Err("Destination host is required".into());
  }

  if config.topic_mappings.is_empty() {
    return Err("At least one topic mapping is required".into());
  }

  if config
    .topic_mappings
    .iter()
    .all(|mapping| mapping.source.trim().is_empty() || mapping.destination.trim().is_empty())
  {
    return Err("At least one complete topic mapping is required".into());
  }

  Ok(())
}

async fn run_bridge_task(app: AppHandle, config: BridgeConfig) {
  let source_options = match build_mqtt_options(
    &config.source_protocol,
    &config.source_host,
    &config.source_port,
    &config.source_client_id,
    &config.source_username,
    &config.source_password,
  ) {
    Ok(options) => options,
    Err(error) => {
      emit_bridge_log(
        &app,
        &config.id,
        "error",
        &format!("Invalid source configuration: {error}"),
      );
      emit_bridge_status(
        &app,
        &config.id,
        "disconnected",
        false,
        Some(error),
      );
      return;
    }
  };

  let destination_options = match build_mqtt_options(
    &config.destination_protocol,
    &config.destination_host,
    &config.destination_port,
    &config.destination_client_id,
    &config.destination_username,
    &config.destination_password,
  ) {
    Ok(options) => options,
    Err(error) => {
      emit_bridge_log(
        &app,
        &config.id,
        "error",
        &format!("Invalid destination configuration: {error}"),
      );
      emit_bridge_status(
        &app,
        &config.id,
        "disconnected",
        false,
        Some(error),
      );
      return;
    }
  };

  let mappings: Vec<TopicMapping> = config
    .topic_mappings
    .iter()
    .filter(|mapping| !mapping.source.trim().is_empty() && !mapping.destination.trim().is_empty())
    .cloned()
    .collect();

  let (source_client, mut source_eventloop) = AsyncClient::new(source_options, 16);
  let (destination_client, mut destination_eventloop) = AsyncClient::new(destination_options, 16);

  for mapping in &mappings {
    emit_bridge_log(
      &app,
      &config.id,
      "info",
      &format!(
        "Subscribing {} -> {}",
        mapping.source, mapping.destination
      ),
    );
    if let Err(error) = source_client.subscribe(mapping.source.clone(), QoS::AtLeastOnce).await {
      emit_bridge_log(
        &app,
        &config.id,
        "error",
        &format!("Subscribe failed for {}: {error}", mapping.source),
      );
      emit_bridge_status(
        &app,
        &config.id,
        "disconnected",
        false,
        Some(format!("Subscribe failed: {error}")),
      );
      return;
    }
  }

  let mut source_ready = false;
  let mut destination_ready = false;
  let mut collected_payloads: HashMap<usize, HashMap<String, CollectedPayloadEntry>> =
    HashMap::new();

  loop {
    tokio::select! {
      source_event = source_eventloop.poll() => {
        match source_event {
          Ok(Event::Incoming(Packet::ConnAck(_))) => {
            source_ready = true;
            emit_bridge_log(
              &app,
              &config.id,
              "info",
              "Source broker connected",
            );
            if destination_ready {
              emit_bridge_status(&app, &config.id, "connected", true, None);
              emit_bridge_log(
                &app,
                &config.id,
                "success",
                "Bridge connected and ready",
              );
            }
          }
          Ok(Event::Incoming(Packet::Publish(publish))) => {
            if let Some((mapping_index, mapping)) = resolve_topic_mapping(&mappings, &publish.topic) {
              let destination_topic = mapping.destination.clone();
              emit_bridge_log(
                &app,
                &config.id,
                "info",
                &format!(
                  "Received topic {} and forwarding to {}",
                  publish.topic, destination_topic
                ),
              );
              let payload_preview = preview_payload(&publish.payload);
              let outbound_payload = if mapping.collect_data {
                let entries = collected_payloads.entry(mapping_index).or_default();
                let now = Instant::now();
                entries.retain(|_, entry| now.duration_since(entry.updated_at) <= Duration::from_secs(120));
                let payload_value = payload_to_json_value(&publish.payload);
                let payload_key = build_collect_key(&payload_value)
                  .unwrap_or_else(|| publish.topic.clone());
                let payload_timestamp_ms = get_payload_timestamp_ms(&payload_value, now);

                let should_update = entries
                  .get(&payload_key)
                  .map(|existing| payload_timestamp_ms >= existing.source_timestamp_ms)
                  .unwrap_or(true);

                if should_update {
                  entries.insert(
                    payload_key.clone(),
                    CollectedPayloadEntry {
                      data: payload_value,
                      updated_at: now,
                      source_timestamp_ms: payload_timestamp_ms,
                    },
                  );
                }

                let collected_map: HashMap<String, Value> = entries
                  .iter()
                  .map(|(key, entry)| (key.clone(), entry.data.clone()))
                  .collect();
                let collected_count = collected_map.len();

                emit_bridge_log(
                  &app,
                  &config.id,
                  "info",
                  &format!(
                    "Collecting active data for {} | {} active entries",
                    publish.topic,
                    collected_count
                  ),
                );

                let snapshot = AggregateSnapshot {
                  generated_at: iso_now(),
                  reason: "update".to_string(),
                  entries: collected_map,
                };

                let snapshot_preview = serde_json::to_string(&snapshot)
                  .map(|json| {
                    if json.chars().count() > 240 {
                      format!("{}...", json.chars().take(240).collect::<String>())
                    } else {
                      json
                    }
                  })
                  .unwrap_or_else(|_| "{}".to_string());

                emit_bridge_log(
                  &app,
                  &config.id,
                  "success",
                  &format!(
                    "Collected count: {} | data: {}",
                    collected_count, snapshot_preview
                  ),
                );

                serde_json::to_vec(&snapshot).unwrap_or_else(|_| b"{}".to_vec())
              } else {
                publish.payload.to_vec()
              };
              if let Err(error) = destination_client
                .publish(
                  destination_topic.clone(),
                  QoS::AtLeastOnce,
                  false,
                  outbound_payload,
                )
                .await
              {
                emit_bridge_log(
                  &app,
                  &config.id,
                  "error",
                  &format!("Publish failed: {error}"),
                );
                emit_bridge_status(
                  &app,
                  &config.id,
                  "reconnect",
                  true,
                  Some(format!("Publish failed: {error}")),
                );
                sleep(Duration::from_millis(400)).await;
              } else {
                emit_bridge_log(
                  &app,
                  &config.id,
                  "success",
                  &format!(
                    "Forwarded from {} to {} | data: {}",
                    publish.topic, destination_topic, payload_preview
                  ),
                );
              }
            }
          }
          Ok(_) => {}
          Err(error) => {
            emit_bridge_log(
              &app,
              &config.id,
              "warn",
              &format!("Source broker reconnecting: {error}"),
            );
            emit_bridge_status(
              &app,
              &config.id,
              "reconnect",
              true,
              Some(format!("Source error: {error}")),
            );
            sleep(Duration::from_secs(1)).await;
          }
        }
      }
      destination_event = destination_eventloop.poll() => {
        match destination_event {
          Ok(Event::Incoming(Packet::ConnAck(_))) => {
            destination_ready = true;
            emit_bridge_log(
              &app,
              &config.id,
              "info",
              "Destination broker connected",
            );
            if source_ready {
              emit_bridge_status(&app, &config.id, "connected", true, None);
              emit_bridge_log(
                &app,
                &config.id,
                "success",
                "Bridge connected and ready",
              );
            }
          }
          Ok(Event::Outgoing(Outgoing::Disconnect)) => {
            emit_bridge_log(&app, &config.id, "warn", "Destination broker disconnected");
            emit_bridge_status(&app, &config.id, "disconnected", false, None);
            return;
          }
          Ok(_) => {}
          Err(error) => {
            emit_bridge_log(
              &app,
              &config.id,
              "warn",
              &format!("Destination broker reconnecting: {error}"),
            );
            emit_bridge_status(
              &app,
              &config.id,
              "reconnect",
              true,
              Some(format!("Destination error: {error}")),
            );
            sleep(Duration::from_secs(1)).await;
          }
        }
      }
    }
  }
}

fn build_mqtt_options(
  protocol: &str,
  host: &str,
  port: &str,
  client_id: &str,
  username: &str,
  password: &str,
) -> Result<MqttOptions, String> {
  let normalized_protocol = protocol.trim().to_lowercase();
  let normalized_host = host.trim();
  let normalized_port = port.trim().parse::<u16>().map_err(|_| "Invalid port".to_string())?;
  let normalized_client_id = if client_id.trim().is_empty() {
    format!("bridge-{}", uuid_like_suffix(normalized_host, port))
  } else {
    client_id.trim().to_string()
  };

  let mut options = match normalized_protocol.as_str() {
    "mqtt" => MqttOptions::new(normalized_client_id, normalized_host, normalized_port),
    "ws" => {
      let mut options = MqttOptions::new(
        normalized_client_id,
        format!("ws://{}:{}", normalized_host, normalized_port),
        normalized_port,
      );
      options.set_transport(Transport::ws());
      options
    }
    "wss" => {
      let mut options = MqttOptions::new(
        normalized_client_id,
        format!("wss://{}:{}", normalized_host, normalized_port),
        normalized_port,
      );
      options.set_transport(Transport::wss_with_default_config());
      options
    }
    _ => return Err(format!("Unsupported protocol: {protocol}")),
  };

  options.set_keep_alive(Duration::from_secs(15));

  if !username.trim().is_empty() {
    options.set_credentials(username.trim(), password);
  }

  Ok(options)
}

fn resolve_topic_mapping<'a>(
  mappings: &'a [TopicMapping],
  incoming_topic: &str,
) -> Option<(usize, &'a TopicMapping)> {
  mappings
    .iter()
    .enumerate()
    .find(|(_, mapping)| mqtt_filter_matches(&mapping.source, incoming_topic))
}

fn mqtt_filter_matches(filter: &str, topic: &str) -> bool {
  let mut filter_segments = filter.split('/').peekable();
  let mut topic_segments = topic.split('/').peekable();

  while let Some(filter_segment) = filter_segments.next() {
    match filter_segment {
      "#" => return filter_segments.peek().is_none(),
      "+" => {
        if topic_segments.next().is_none() {
          return false;
        }
      }
      literal => {
        if Some(literal) != topic_segments.next() {
          return false;
        }
      }
    }
  }

  topic_segments.next().is_none()
}

fn emit_bridge_status(
  app: &AppHandle,
  bridge_id: &str,
  connection_state: &str,
  enabled: bool,
  error: Option<String>,
) {
  let _ = app.emit(
    "bridge-status",
    BridgeStatusEvent {
      id: bridge_id.to_string(),
      connection_state: connection_state.to_string(),
      enabled,
      error,
    },
  );
}

fn emit_bridge_log(app: &AppHandle, bridge_id: &str, level: &str, message: &str) {
  let _ = app.emit(
    "bridge-log",
    BridgeLogEvent {
      id: bridge_id.to_string(),
      level: level.to_string(),
      message: message.to_string(),
    },
  );
}

fn uuid_like_suffix(host: &str, port: &str) -> String {
  format!("{}-{}", host.replace(['.', ':'], "-"), port)
}

fn preview_payload(payload: &[u8]) -> String {
  const MAX_CHARS: usize = 160;

  match std::str::from_utf8(payload) {
    Ok(text) => {
      let sanitized = text.replace('\n', "\\n").replace('\r', "\\r");
      if sanitized.chars().count() > MAX_CHARS {
        format!("{}...", sanitized.chars().take(MAX_CHARS).collect::<String>())
      } else {
        sanitized
      }
    }
    Err(_) => {
      let preview: Vec<String> = payload
        .iter()
        .take(24)
        .map(|byte| format!("{byte:02X}"))
        .collect();

      if payload.len() > 24 {
        format!("0x{} ... ({} bytes)", preview.join(" "), payload.len())
      } else {
        format!("0x{}", preview.join(" "))
      }
    }
  }
}

fn payload_to_json_value(payload: &[u8]) -> Value {
  std::str::from_utf8(payload)
    .ok()
    .and_then(|text| serde_json::from_str::<Value>(text).ok())
    .unwrap_or_else(|| Value::String(preview_payload(payload)))
}

fn build_collect_key(payload: &Value) -> Option<String> {
  let object = payload.as_object()?;

  object
    .get("identity")
    .and_then(|identity| identity.as_object())
    .and_then(|identity| identity.get("id"))
    .and_then(value_to_key_string)
    .or_else(|| object.get("serial_number").and_then(value_to_key_string))
    .or_else(|| object.get("device_id").and_then(value_to_key_string))
    .or_else(|| object.get("deviceId").and_then(value_to_key_string))
    .or_else(|| object.get("id").and_then(value_to_key_string))
}

fn value_to_key_string(value: &Value) -> Option<String> {
  match value {
    Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
    Value::Number(number) => Some(number.to_string()),
    _ => None,
  }
}

fn get_payload_timestamp_ms(payload: &Value, fallback: Instant) -> i64 {
  extract_timestamp_ms(payload).unwrap_or_else(|| instant_to_epoch_ms(fallback))
}

fn extract_timestamp_ms(payload: &Value) -> Option<i64> {
  let object = payload.as_object()?;

  object
    .get("gps")
    .and_then(|gps| gps.as_object())
    .and_then(|gps| gps.get("gps_timestamp"))
    .and_then(value_to_timestamp_ms)
    .or_else(|| object.get("timestamp").and_then(value_to_timestamp_ms))
    .or_else(|| object.get("received_at_unix").and_then(value_to_timestamp_ms))
}

fn value_to_timestamp_ms(value: &Value) -> Option<i64> {
  match value {
    Value::Number(number) => {
      let raw = number.as_i64()?;
      Some(if raw < 1_000_000_000_000 { raw * 1000 } else { raw })
    }
    Value::String(text) => {
      if let Ok(parsed) = text.parse::<i64>() {
        Some(if parsed < 1_000_000_000_000 { parsed * 1000 } else { parsed })
      } else {
        None
      }
    }
    _ => None,
  }
}

fn instant_to_epoch_ms(instant: Instant) -> i64 {
  let now_instant = Instant::now();
  let now_epoch = std::time::SystemTime::now();

  let system_time = if instant <= now_instant {
    now_epoch
      .checked_sub(now_instant.duration_since(instant))
      .unwrap_or(now_epoch)
  } else {
    now_epoch
      .checked_add(instant.duration_since(now_instant))
      .unwrap_or(now_epoch)
  };

  system_time
    .duration_since(std::time::UNIX_EPOCH)
    .map(|duration| duration.as_millis() as i64)
    .unwrap_or(0)
}

fn iso_now() -> String {
  let now = chrono_like_now();
  format!(
    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
    now.year, now.month, now.day, now.hour, now.minute, now.second, now.millisecond
  )
}

struct DateParts {
  year: i32,
  month: u32,
  day: u32,
  hour: u32,
  minute: u32,
  second: u32,
  millisecond: u32,
}

fn chrono_like_now() -> DateParts {
  let now = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap_or_default();
  let total_millis = now.as_millis() as i64;
  let total_seconds = total_millis / 1000;
  let millisecond = (total_millis % 1000) as u32;
  let days = total_seconds.div_euclid(86_400);
  let seconds_of_day = total_seconds.rem_euclid(86_400);

  let (year, month, day) = civil_from_days(days);
  let hour = (seconds_of_day / 3600) as u32;
  let minute = ((seconds_of_day % 3600) / 60) as u32;
  let second = (seconds_of_day % 60) as u32;

  DateParts {
    year,
    month,
    day,
    hour,
    minute,
    second,
    millisecond,
  }
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i32, u32, u32) {
  let z = days_since_unix_epoch + 719_468;
  let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
  let doe = z - era * 146_097;
  let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
  let y = yoe + era * 400;
  let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  let mp = (5 * doy + 2) / 153;
  let d = doy - (153 * mp + 2) / 5 + 1;
  let m = mp + if mp < 10 { 3 } else { -9 };
  let year = y + if m <= 2 { 1 } else { 0 };

  (year as i32, m as u32, d as u32)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
  tauri::Builder::default()
    .manage(BridgeManager::default())
    .invoke_handler(tauri::generate_handler![start_bridge, stop_bridge])
    .setup(|app| {
      if cfg!(debug_assertions) {
        app.handle().plugin(
          tauri_plugin_log::Builder::default()
            .level(log::LevelFilter::Info)
            .build(),
        )?;
      }
      Ok(())
    })
    .run(tauri::generate_context!())
    .expect("error while running tauri application");
}
